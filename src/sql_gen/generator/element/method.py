# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: method$
# @Author: 10379
# @Time: 2025/5/10 12:24
from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_function_tree, parse_tree
from sql_gen.generator.element.Pattern import Pattern, Slot, ForSlot, ValueSlot
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


def parse_pattern_tree(point_type, pattern: Pattern, dialect) -> TreeNode:
    extended_pattern, slot_list = pattern.extend_pattern()
    if point_type == 'PATTERN':
        tree_node, _, _, _ = parse_tree(extended_pattern, dialect)
        if tree_node is None:
            raise ValueError(f"Failed to parse the pattern {pattern}")
        tree_node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        rep_value_with_slot(tree_node, slot_list, None)
    else:
        tree_node, _, _, _ = parse_function_tree(extended_pattern, dialect)
        if tree_node is None:
            raise ValueError(f"Failed to parse the pattern {extended_pattern}")
        tree_node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        rep_value_with_slot(tree_node, slot_list, None)
    return tree_node


def rep_value_with_slot(root_node: TreeNode, slot_list: list, ancestor_node):
    begin_dis_to_node = {}
    end_dis_to_node = {}
    mark_pos_node(root_node, 0, begin_dis_to_node, end_dis_to_node)
    rm_node_range = []
    slot_times = {}
    for slot_info in slot_list:
        if isinstance(slot_info['slot'], ValueSlot):
            begin_pos = slot_info['info']['pos'][0]
            end_pos = slot_info['info']['pos'][1]
            begin_node = begin_dis_to_node[begin_pos]
            begin_node = lift_node(begin_node, ancestor_node)
            end_node = end_dis_to_node[end_pos]
            end_node = lift_node(end_node, ancestor_node)
            ancestor = get_closest_ancestor(begin_node, end_node, root_node)
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


def lift_slot_nodes(root_node: TreeNode):
    all_node_to_lift = []
    dfs_get_slot_node_set(root_node, all_node_to_lift)
    for node in all_node_to_lift:
        lifted_node = lift_node(node)
        lifted_node.slot = node.slot
        lifted_node.slot_times = node.slot_times
        lifted_node.for_slot_ancestor_id = node.for_slot_ancestor_id
        lifted_node.for_slot_ancestor = node.for_slot_ancestor
        lifted_node.for_loop_slot = node.for_loop_slot
        lifted_node.for_loop_sub_trees = node.for_loop_sub_trees



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
    while node2 is not None and node2 != root_node:
        nodes_path2.insert(0, node2)
        node2 = node2.father
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
    left_node.slot_times = times
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
            left_i_node.children[left_pos].slot_times = times
            left_pos = left_pos + 1
        left_node = left_i_node
    right_i_node = right_node
    right_node.slot = slot
    right_node.slot_times = times
    while right_i_node.father != ancestor_node:
        right_i_node = right_i_node.father
        right_pos = 0
        while right_i_node.children[right_pos] != right_node:
            right_i_node.children[right_pos].slot = slot
            right_i_node.children[right_pos].slot_times = times
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
            child.slot_times = times


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
            left_node = up_node

        return clone_up_node
    if left_node.father == ancestor:
        left_i_node = left_node
        clone_left_i_node = clone_left_node
    else:
        left_i_node = left_node
        while left_i_node.father != ancestor:
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
            clone_left_i_node = clone_single_node(left_i_node)

    clone_right_node = clone_whole_tree(right_node)
    if right_node.father == ancestor:
        right_i_node = right_node
        clone_right_i_node = clone_right_node
    else:
        right_i_node = right_node
        while right_i_node.father != ancestor:
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
    clone_ancestor_node = clone_single_node(ancestor)
    for i in range(len(ancestor.children)):
        if ancestor.children[i] == left_i_node:
            clone_ancestor_node.add_child(clone_left_i_node)
            flag = True
            continue
        if ancestor.children[i] == right_i_node:
            clone_ancestor_node.add_child(clone_right_i_node)
            break
        if flag:
            clone_ancestor_node.add_child(clone_whole_tree(ancestor.children[i]))
    return clone_ancestor_node
