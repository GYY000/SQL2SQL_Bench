# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: pattern_tree_parser$
# @Author: 10379
# @Time: 2025/8/10 14:09
from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_element_tree
from sql_gen.generator.ele_type.type_def import IntLiteralType, StringLiteralType, WordLiteralType, FloatLiteralType, \
    OptionType
from sql_gen.generator.element.Pattern import Pattern, ValueSlot, ForSlot, Slot
from sql_gen.generator.point_type.TranPointType import TranPointType
from utils.tools import get_no_space_len


def mark_pos_node(tree_node: TreeNode, pos: int, begin_dis_to_node: dict[int, TreeNode],
                  end_dis_to_node: dict[int, TreeNode] = None):
    if tree_node.is_terminal:
        used_value = tree_node.value
        begin_dis_to_node[pos] = tree_node
        used_len = get_no_space_len(used_value)
        end_dis_to_node[pos + used_len - 1] = tree_node
        return pos + used_len
    else:
        for child in tree_node.children:
            pos = mark_pos_node(child, pos, begin_dis_to_node, end_dis_to_node)
        return pos


def parse_pattern_tree(point_type: TranPointType, pattern: Pattern, dialect) -> TreeNode:
    extended_pattern, slot_list = pattern.extend_pattern()
    tree_node, _, _, _ = parse_element_tree(extended_pattern, dialect, point_type)
    if tree_node is None:
        raise ValueError(f"Failed to parse the pattern {pattern}")
    tree_node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
    assert isinstance(tree_node, TreeNode)
    # if not only one node
    flag = False
    upper_most_node = tree_node
    while len(upper_most_node.children) == 1:
        upper_most_node = upper_most_node.children[0]
    if len(upper_most_node.children) == 0:
        flag = True
    if not flag:
        tree_node = upper_most_node
    tree_node.father = None
    rep_value_with_slot(tree_node, slot_list, None)
    return tree_node


def find_begin_node(begin_pos: int, begin_dis_to_node: dict[int, TreeNode]):
    if begin_pos in begin_dis_to_node:
        return begin_pos, begin_dis_to_node[begin_pos]
    else:
        max_pos_less_than_begin = -1
        node = None
        for key in begin_dis_to_node:
            if begin_pos > key > max_pos_less_than_begin:
                max_pos_less_than_begin = key
                node = begin_dis_to_node[key]
        assert node is not None
        return max_pos_less_than_begin, node


def find_end_node(end_pos: int, end_dis_to_node: dict[int, TreeNode]):
    if end_pos in end_dis_to_node:
        return end_pos, end_dis_to_node[end_pos]
    else:
        min_pos_greater_than_end = 1000000000
        node = None
        for key in end_dis_to_node:
            if end_pos < key < min_pos_greater_than_end:
                min_pos_greater_than_end = key
                node = end_dis_to_node[key]
        assert node is not None
        return min_pos_greater_than_end, node


def rep_value_with_slot(root_node: TreeNode, slot_list: list, ancestor_node):
    begin_dis_to_node = {}
    end_dis_to_node = {}
    mark_pos_node(root_node, 0, begin_dis_to_node, end_dis_to_node)
    rm_node_range = []
    slot_times = {}
    for i, slot_info in enumerate(slot_list):
        if isinstance(slot_info['slot'], ValueSlot):
            begin_pos = slot_info['info']['pos'][0]
            end_pos = slot_info['info']['pos'][1]
            node_begin_pos, begin_node = find_begin_node(begin_pos, begin_dis_to_node)
            begin_node = lift_node(begin_node, ancestor_node)
            node_end_pos, end_node = find_end_node(end_pos, end_dis_to_node)
            end_node = lift_node(end_node, ancestor_node)
            ancestor = get_closest_ancestor(begin_node, end_node, root_node)
            flag_first_node = False
            flag_last_node = False
            ancestor_used = ancestor
            while len(ancestor_used.children) != 0:
                node_first = ancestor_used.children[0]
                if node_first == begin_node:
                    flag_first_node = True
                ancestor_used = node_first
            ancestor_used = ancestor
            while len(ancestor_used.children) != 0:
                node_last = ancestor_used.children[-1]
                if node_last == end_node:
                    flag_last_node = True
                ancestor_used = node_last
            if flag_first_node and flag_last_node:
                begin_node = ancestor
                end_node = ancestor
            now_slot = slot_info['slot']
            assert isinstance(now_slot, ValueSlot)
            if ((isinstance(now_slot.get_type(), IntLiteralType) or
                 isinstance(now_slot.get_type(), StringLiteralType) or
                 isinstance(now_slot.get_type(), WordLiteralType)) or
                    isinstance(now_slot.get_type(), FloatLiteralType)
                    or isinstance(now_slot.get_type(), OptionType)):
                assert begin_node == end_node
                assert isinstance(begin_node, TreeNode)
                begin_node.ori_pattern_string = str(begin_node)
                if begin_node.pos_to_slot is None:
                    begin_node.pos_to_slot = []
                if now_slot in slot_times:
                    times = slot_times[now_slot] + 1
                    slot_times[now_slot] = times
                else:
                    times = 0
                    slot_times[now_slot] = times
                begin_node.slot_times[now_slot] = times
                begin_node.pos_to_slot.append({
                    "slot": now_slot,
                    "begin_pos": begin_pos - node_begin_pos,
                    "end_pos": end_pos - node_begin_pos,
                })
                continue
            if slot_info['slot'] in slot_times:
                times = slot_times[slot_info['slot']] + 1
                slot_times[slot_info['slot']] = times
            else:
                times = 0
                slot_times[slot_info['slot']] = times
            mark_nodes(begin_node, end_node, ancestor, slot_info['slot'], times)
        else:
            assert isinstance(slot_info['slot'], ForSlot)
            begin_pos1 = slot_info['info']['pos'][0]
            begin_pos2 = slot_info['info']['pos'][1]
            begin_pos3 = slot_info['info']['pos'][2]
            end_pos3 = slot_info['info']['pos'][3]

            begin_node1 = begin_dis_to_node[begin_pos1]
            begin_node1 = lift_node(begin_node1, ancestor_node)
            begin_node2 = begin_dis_to_node[begin_pos2]
            begin_node2 = lift_node(begin_node2, ancestor_node)
            end_node1 = end_dis_to_node[begin_pos2 - 1]
            end_node1 = lift_node(end_node1, ancestor_node)

            end_node2 = end_dis_to_node[begin_pos3 - 1]
            end_node2 = lift_node(end_node2, ancestor_node)
            end_node3 = end_dis_to_node[end_pos3]
            end_node3 = lift_node(end_node3, ancestor_node)
            ancestor12 = get_closest_ancestor(begin_node1, end_node2, root_node)
            ancestor13 = get_closest_ancestor(begin_node1, end_node3, root_node)

            assert ancestor12 == ancestor13
            rm_node_range.append([begin_node2, end_node3])
            first_tree = clone_tree(begin_node1, end_node1, ancestor13)
            begin_node1.slot = slot_info['slot']
            begin_node1.for_slot_ancestor_id = len(ancestor12.for_loop_sub_trees)
            begin_node1.for_slot_ancestor = ancestor12
            second_tree = clone_tree(begin_node2, end_node2, ancestor13)
            ancestor12.for_loop_sub_trees.append({
                "first_tree": first_tree,
                'second_tree': second_tree
            })
            ancestor12.for_loop_slot.append(slot_info['slot'])
            loop_slot_list1 = slot_info['slot_list'][0]
            loop_slot_list2 = slot_info['slot_list'][1]
            rep_value_with_slot(first_tree, loop_slot_list1, first_tree)
            rep_value_with_slot(second_tree, loop_slot_list2, second_tree)

    # lift_slot_nodes(root_node)
    for node_range in rm_node_range:
        begin_node = node_range[0]
        end_node = node_range[1]
        rm_nodes_in_range(root_node, begin_node, end_node)


def dfs_get_slot_node_set(root_node: TreeNode, node_set: list[TreeNode]):
    if root_node.slot is not None:
        node_set.append(root_node)
    else:
        for child in root_node.children:
            dfs_get_slot_node_set(child, node_set)


def lift_node(node: TreeNode, root_node):
    ori_node = node
    while node.father is not None:
        if root_node is not None and node.father == root_node:
            break
        father_node = node.father
        if len(father_node.children) == 1:
            node = father_node
        else:
            break
    return node


def get_closest_ancestor(node1: TreeNode, node2: TreeNode, root_node: TreeNode):
    if node1 == node2:
        return node1
    nodes_path1 = []
    nodes_path2 = []
    while node1 is not None and node1 != root_node:
        nodes_path1.insert(0, node1)
        node1 = node1.father
    nodes_path1.append(node1)
    while node2 is not None and node2 != root_node:
        nodes_path2.insert(0, node2)
        node2 = node2.father
    nodes_path1.append(node2)
    i = 0
    while i < len(nodes_path1) and i < len(nodes_path2) and nodes_path1[i] == nodes_path2[i]:
        i = i + 1
    if i == nodes_path1:
        return nodes_path1[-1]
    elif i == nodes_path2:
        return nodes_path2[-1]
    return nodes_path1[i - 1]


def mark_nodes(left_node: TreeNode, right_node: TreeNode, ancestor_node, slot: Slot, times: int):
    left_node.slot = slot
    left_node.slot_times[slot] = times
    if left_node == right_node:
        return
    left_i_node = left_node
    while left_i_node.father != ancestor_node:
        left_i_node = left_i_node.father
        left_pos = 0
        while left_i_node.children[left_pos] != left_node:
            left_pos = left_pos + 1
        left_pos = left_pos + 1
        while left_pos < len(left_i_node.children):
            left_i_node.children[left_pos].slot = slot
            left_i_node.children[left_pos].slot_times[slot] = times
            left_pos = left_pos + 1
        left_node = left_i_node
    right_i_node = right_node
    right_node.slot = slot
    right_node.slot_times[slot] = times
    while right_i_node.father != ancestor_node:
        right_i_node = right_i_node.father
        right_pos = 0
        while right_i_node.children[right_pos] != right_node:
            right_i_node.children[right_pos].slot = slot
            right_i_node.children[right_pos].slot_times[slot] = times
            right_pos = right_pos + 1
        right_node = right_i_node
    flag = False
    for child in ancestor_node.children:
        if child == left_i_node:
            flag = True
        if child == right_i_node:
            break
        if flag:
            child.slot = slot
            child.slot_times[slot] = times


def rm_nodes_in_range(root_node: TreeNode, left_node: TreeNode, right_node: TreeNode):
    ancestor = get_closest_ancestor(left_node, right_node, root_node)
    if left_node == right_node:
        assert left_node in root_node.children
        root_node.children.remove(left_node)
    ori_left_node = left_node
    if left_node.father == ancestor:
        left_i_node = left_node
    else:
        left_i_node = left_node.father
        while True:
            left_pos = 0
            while left_i_node.children[left_pos] != left_node:
                left_pos = left_pos + 1
            j = len(left_i_node.children) - 1
            while j > left_pos:
                left_i_node.children.remove(left_i_node.children[j])
            if left_node == ori_left_node or len(left_node.children) == 0:
                left_i_node.children.remove(left_node)
            left_node = left_i_node
            if left_i_node.father == ancestor:
                break
            left_i_node = left_node.father
    ori_right_node = right_node
    if right_node.father == ancestor:
        right_i_node = right_node
    else:
        right_i_node = right_node.father
        while True:
            while right_i_node.children[0] != right_node:
                right_i_node.children.remove(right_i_node.children[0])
            if right_node == ori_right_node or len(right_node.children) == 0:
                right_i_node.children.remove(right_node)
            right_node = right_i_node
            if right_i_node.father == ancestor:
                break
            right_i_node = right_i_node.father
    j = 0
    while j < len(ancestor.children):
        if ancestor.children[j] == left_i_node:
            break
        j = j + 1
    assert j < len(ancestor.children)
    rm_node_list = []
    if left_i_node == ori_left_node or len(left_i_node.children) == 0:
        rm_node_list.append(left_i_node)
    j = j + 1
    while j < len(ancestor.children) and ancestor.children[j] != right_i_node:
        rm_node_list.append(ancestor.children[j])
        j = j + 1
    if right_i_node == ori_right_node or len(right_i_node.children) == 0:
        rm_node_list.append(right_i_node)
    for rm_node in rm_node_list:
        ancestor.children.remove(rm_node)


def clone_single_node(tree_node: TreeNode):
    new_tree_node = TreeNode(tree_node.value, tree_node.dialect, tree_node.is_terminal)
    new_tree_node.slot = tree_node.slot
    new_tree_node.slot_times = tree_node.slot_times
    new_tree_node.for_slot_ancestor_id = tree_node.for_slot_ancestor_id
    new_tree_node.for_slot_ancestor = tree_node.for_slot_ancestor
    new_tree_node.for_loop_slot = tree_node.for_loop_slot
    new_tree_node.for_loop_sub_trees = tree_node.for_loop_sub_trees
    return new_tree_node


def clone_whole_tree(tree_node: TreeNode):
    new_root_node = clone_single_node(tree_node)
    for child in tree_node.children:
        new_root_node.add_child(clone_whole_tree(child))
    return new_root_node


def clone_tree(left_node: TreeNode, right_node: TreeNode, ancestor: TreeNode):
    clone_left_node = clone_whole_tree(left_node)
    if left_node == right_node:
        up_node = left_node
        clone_up_node = clone_left_node
        while True:
            if up_node == ancestor:
                break
            up_node = up_node.father
            clone_up_node = clone_single_node(up_node)
            clone_up_node.add_child(clone_left_node)
            clone_left_node = clone_up_node
        return clone_up_node
    temp_ancestor_node = get_closest_ancestor(left_node, right_node, ancestor)
    if left_node.father == temp_ancestor_node:
        left_i_node = left_node
        clone_left_i_node = clone_left_node
    else:
        left_i_node = left_node
        while left_i_node.father != temp_ancestor_node:
            left_i_node = left_i_node.father
            clone_left_i_node = clone_single_node(left_i_node)
            left_pos = 0
            while left_i_node.children[left_pos] != left_node:
                left_pos = left_pos + 1
            j = left_pos + 1
            clone_left_i_node.add_child(clone_left_node)
            while j < len(left_i_node.children):
                clone_left_i_node.add_child(clone_whole_tree(left_i_node.children[j]))
                j = j + 1
            left_node = left_i_node
            clone_left_node = clone_left_i_node
            left_i_node = left_i_node.father

    clone_right_node = clone_whole_tree(right_node)
    if right_node.father == temp_ancestor_node:
        right_i_node = right_node
        clone_right_i_node = clone_right_node
    else:
        right_i_node = right_node
        while right_i_node.father != temp_ancestor_node:
            right_i_node = right_i_node.father
            clone_right_i_node = clone_single_node(right_i_node)
            right_pos = 0
            while right_i_node.children[right_pos] != right_node:
                clone_right_i_node.add_child(clone_whole_tree(right_i_node.children[right_pos]))
                right_pos = right_pos + 1
            clone_right_i_node.add_child(clone_right_node)
            right_node = right_i_node
            clone_right_node = clone_right_i_node

    flag = False
    clone_ancestor_node = clone_single_node(temp_ancestor_node)
    for i in range(len(temp_ancestor_node.children)):
        if temp_ancestor_node.children[i] == left_i_node:
            clone_ancestor_node.add_child(clone_left_i_node)
            flag = True
            continue
        if temp_ancestor_node.children[i] == right_i_node:
            clone_ancestor_node.add_child(clone_right_i_node)
            break
        if flag:
            clone_ancestor_node.add_child(clone_whole_tree(temp_ancestor_node.children[i]))
    if temp_ancestor_node == ancestor:
        return clone_ancestor_node
    else:
        while temp_ancestor_node.father != ancestor:
            temp_node = clone_single_node(temp_ancestor_node.father)
            temp_node.add_child(clone_ancestor_node)
            clone_ancestor_node = temp_node
            temp_ancestor_node = temp_ancestor_node.father
        assert temp_ancestor_node.father == ancestor
        final_ancestor_node = clone_single_node(ancestor)
        final_ancestor_node.add_child(clone_ancestor_node)
    return final_ancestor_node


def get_child_pattern_value(for_slot: ForSlot, for_slot_tree, var_value_map):
    assert isinstance(for_slot, ForSlot)
    res = ''
    for j in range(len(var_value_map[for_slot.ele_slots[0]])):
        for i in range(len(for_slot.sub_ele_slots)):
            var_value_map[for_slot.sub_ele_slots[i]] = var_value_map[for_slot.ele_slots[i]][j]
        if j != 0:
            res = res + for_slot.strip_str
        first_tree_node = for_slot_tree['first_tree']
        value, _, _ = get_pattern_value(first_tree_node, var_value_map)
        assert value != ''
        res = res + value
    return res


def get_pattern_value(tree_node: TreeNode, var_value_map: dict):
    if tree_node.slot is not None:
        if isinstance(tree_node.slot, ValueSlot):
            if tree_node.slot not in var_value_map:
                print(tree_node.slot)
                assert False
            return var_value_map[tree_node.slot].str_value(), None, None
        else:
            assert isinstance(tree_node.slot, ForSlot)
            assert tree_node.for_slot_ancestor is not None
            return '', tree_node.for_slot_ancestor, tree_node.for_slot_ancestor_id
    if tree_node.value == '<EOF>':
        return '', None, None
    res = ''
    flag = False
    flag_paren = True
    if tree_node.is_terminal:
        res = tree_node.value
        return res, None, None
    if tree_node.dialect == 'mysql':
        if tree_node.value in ['comparisonOperator', 'logicalOperator', 'bitOperator', 'multOperator',
                               'jsonOperator']:
            i = 0
            while i < len(tree_node.children):
                value, for_slot_ancestor, for_slot_ancestor_id = get_pattern_value(tree_node.children[i], var_value_map)
                if for_slot_ancestor is not None:
                    if tree_node != for_slot_ancestor:
                        return '', for_slot_ancestor, for_slot_ancestor_id
                    else:
                        for_slot = tree_node.for_loop_slot[for_slot_ancestor_id]
                        for_slot_tree_node = tree_node.for_loop_sub_trees[for_slot_ancestor_id]
                        value = get_child_pattern_value(for_slot, for_slot_tree_node, var_value_map)
                        res = res + value.strip()
                        i = i + len(tree_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
                else:
                    sub_str = value
                    res = res + sub_str.strip()
                    i = i + 1
            return res, None, None
        if (tree_node.value == 'functionCall' and len(tree_node.children) != 0
                and (tree_node.children[0].value == 'scalarFunctionName' or tree_node.children[
                    0].value == 'fullId')):
            flag_paren = False
        elif (tree_node.value == 'specificFunction' or tree_node.value == 'passwordFunctionClause' or
              tree_node.value == 'aggregateWindowedFunction' or
              tree_node.value == 'nonAggregateWindowedFunction'
              or tree_node.value == 'dataType'):
            flag_paren = False
        if not flag_paren:
            i = 0
            while i < len(tree_node.children):
                child = tree_node.children[i]
                if child.is_terminal and child.value == '(':
                    res = res + child.value
                    flag = True
                    i = i + 1
                else:
                    value, for_slot_ancestor, for_slot_ancestor_id = get_pattern_value(tree_node.children[i],
                                                                                       var_value_map)
                    if for_slot_ancestor is not None:
                        if tree_node != for_slot_ancestor:
                            return '', for_slot_ancestor, for_slot_ancestor_id
                        else:
                            for_slot = tree_node.for_loop_slot[for_slot_ancestor_id]
                            for_slot_tree_node = tree_node.for_loop_sub_trees[for_slot_ancestor_id]
                            value = get_child_pattern_value(for_slot, for_slot_tree_node, var_value_map)
                            i = i + len(tree_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
                    else:
                        i = i + 1
                    sub_str = value
                    if sub_str.startswith('.') or sub_str.startswith('(') or res.endswith('.'):
                        flag = False
                    if sub_str != '':
                        if flag:
                            res = res + " " + sub_str.strip()
                        else:
                            res = res + sub_str
                            flag = True
            return res, None, None
    elif tree_node.dialect == 'pg':
        if ((tree_node.value == 'func_application' or tree_node.value == 'func_expr_common_subexpr')
                and len(tree_node.children) != 0):
            flag_paren = False
        if tree_node.father is not None and tree_node.father.value == 'simpletypename':
            flag_paren = False
        if not flag_paren:
            i = 0
            while i < len(tree_node.children):
                child = tree_node.children[i]
                if child.is_terminal and child.value == '(':
                    res = res + child.value
                    flag = True
                    i = i + 1
                else:
                    value, for_slot_ancestor, for_slot_ancestor_id = get_pattern_value(tree_node.children[i],
                                                                                       var_value_map)
                    if for_slot_ancestor is not None:
                        if tree_node != for_slot_ancestor:
                            return '', for_slot_ancestor, for_slot_ancestor_id
                        else:
                            for_slot = tree_node.for_loop_slot[for_slot_ancestor_id]
                            for_slot_tree_node = tree_node.for_loop_sub_trees[for_slot_ancestor_id]
                            value = get_child_pattern_value(for_slot, for_slot_tree_node, var_value_map)
                            i = i + len(tree_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
                    else:
                        i = i + 1
                    sub_str = value
                    if sub_str.startswith('.') or sub_str.startswith('(') or res.endswith('.'):
                        flag = False
                    if sub_str != '':
                        if flag:
                            res = res + " " + sub_str.strip()
                        else:
                            res = res + sub_str
                            flag = True
            return res, None, None
    elif tree_node.dialect == 'oracle':
        if tree_node.value in ['relational_operator']:
            for child in tree_node.children:
                sub_str = str(child)
                res = res + sub_str.strip()
            return res, None, None
        if (tree_node.value == 'string_function' or tree_node.value == 'json_function'
                or tree_node.value == 'other_function' or
                tree_node.value == 'numeric_function' or tree_node.value == 'datatype'):
            flag_paren = False
        if not flag_paren:
            i = 0
            while i < len(tree_node.children):
                child = tree_node.children[i]
                if child.is_terminal and child.value == '(':
                    res = res + child.value
                    flag = True
                    i = i + 1
                else:
                    value, for_slot_ancestor, for_slot_ancestor_id = get_pattern_value(tree_node.children[i],
                                                                                       var_value_map)
                    if for_slot_ancestor is not None:
                        if tree_node != for_slot_ancestor:
                            return '', for_slot_ancestor, for_slot_ancestor_id
                        else:
                            for_slot = tree_node.for_loop_slot[for_slot_ancestor_id]
                            for_slot_tree_node = tree_node.for_loop_sub_trees[for_slot_ancestor_id]
                            value = get_child_pattern_value(for_slot, for_slot_tree_node, var_value_map)
                            i = i + len(tree_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
                    else:
                        i = i + 1
                    sub_str = value
                    if sub_str.startswith('.') or sub_str.startswith('(') or res.endswith('.'):
                        flag = False
                    if sub_str != '':
                        if flag:
                            res = res + " " + sub_str.strip()
                        else:
                            res = res + sub_str
                            flag = True
            return res, None, None
        i = 0
        while i < len(tree_node.children):
            value, for_slot_ancestor, for_slot_ancestor_id = get_pattern_value(tree_node.children[i],
                                                                               var_value_map)
            if for_slot_ancestor is not None:
                if tree_node != for_slot_ancestor:
                    return '', for_slot_ancestor, for_slot_ancestor_id
                else:
                    for_slot = tree_node.for_loop_slot[for_slot_ancestor_id]
                    for_slot_tree_node = tree_node.for_loop_sub_trees[for_slot_ancestor_id]
                    value = get_child_pattern_value(for_slot, for_slot_tree_node, var_value_map)
                    i = i + len(tree_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
                if not (value.startswith('.') or res.endswith('.')):
                    res = res + " " + value.strip()
                else:
                    res = res + value.strip()
            else:
                if value.startswith('.'):
                    flag = False
                if value != '':
                    if (flag and not res.endswith('.')
                            and not tree_node.children[i].value == 'function_argument'
                            and not tree_node.children[i].value == 'function_argument_analytic'
                            and not tree_node.children[i].value == 'function_argument_modeling'):
                        res = res + " " + value.strip()
                    else:
                        res = res + value
                        flag = True
                i = i + 1
        return res, None, None
    i = 0
    while i < len(tree_node.children):
        value, for_slot_ancestor, for_slot_ancestor_id = get_pattern_value(tree_node.children[i],
                                                                           var_value_map)
        if for_slot_ancestor is not None:
            if tree_node != for_slot_ancestor:
                return '', for_slot_ancestor, for_slot_ancestor_id
            else:
                for_slot = tree_node.for_loop_slot[for_slot_ancestor_id]
                for_slot_tree_node = tree_node.for_loop_sub_trees[for_slot_ancestor_id]
                value = get_child_pattern_value(for_slot, for_slot_tree_node, var_value_map)
                i = i + len(tree_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
        else:
            i = i + 1
        sub_str = value
        if sub_str.startswith('.'):
            flag = False
        if sub_str != '':
            if flag and not res.endswith('.'):
                res = res + " " + sub_str.strip()
            else:
                res = res + sub_str
                flag = True
    return res, None, None
