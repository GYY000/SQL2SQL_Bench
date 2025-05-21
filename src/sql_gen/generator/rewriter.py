# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: rewriter.py$
# @Author: 10379
# @Time: 2025/5/9 19:24
import json
from typing import List

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from sql_gen.generator.ele_type.type_def import BaseType, OptionType, QueryType, TableType, ListType, AnyValueType, \
    is_str_type, is_num_type, is_time_type
from sql_gen.generator.element.Operand import Operand
from sql_gen.generator.element.Pattern import Slot, ValueSlot, ForSlot
from sql_gen.generator.element.Point import Point
from sql_gen.generator.element.method import parse_pattern_tree
from sql_gen.generator.fetch_operand_type import fetch_operand_type
from sql_gen.generator.point_parser import parse_point
from utils.tools import get_proj_root_path


def type_equal(type1: BaseType, type2: BaseType) -> bool:
    if type1 == type2:
        return True


def type1_contains_type2(type1: BaseType, type2: BaseType) -> bool:
    if isinstance(type1, AnyValueType):
        return True
    if type(type1).__name__ == type(type2).__name__:
        return True
    elif is_num_type(type1) and is_num_type(type2):
        return True
    elif is_str_type(type1) and is_str_type(type2):
        return True
    elif is_time_type(type1) and is_time_type(type2):
        return True
    return False


def check_slot_value_map(slot_value_map: dict):
    for key, value in slot_value_map.items():
        final_slot_value = None
        for time, slot_value in value.items():
            if final_slot_value is None:
                final_slot_value = slot_value
            else:
                assert final_slot_value == slot_value


def get_final_value(slot_value_map, slot: Slot):
    values = slot_value_map[slot]
    final_value = None
    for key, value in values.items():
        if final_value is None:
            final_value = value
        else:
            assert final_value == value
    assert final_value is not None
    return final_value


def value_compare(value1, value2):
    if isinstance(value1, list):
        assert isinstance(value2, list)
        if len(value1) != len(value2):
            return False
        for i in range(len(value1)):
            if not value_compare(value1[i], value2[i]):
                return False
        return True
    else:
        assert isinstance(value1, str) and isinstance(value2, str)
        return value1 == value2


def match_tree_node(sql_tree_node: TreeNode, pattern_tree_node: TreeNode, try_fill_map: dict, sql_begin_pos=0,
                    pattern_begin_pos=0) -> tuple[bool, TreeNode | None, int]:
    if sql_tree_node.value == pattern_tree_node.value:
        add_map = {}
        if pattern_tree_node.slot is not None:
            if isinstance(pattern_tree_node.slot, ValueSlot):
                slot = pattern_tree_node.slot
                times = pattern_tree_node.slot_times
                if pattern_tree_node.slot not in try_fill_map:
                    try_fill_map[slot] = {}
                if times not in try_fill_map[pattern_tree_node.slot]:
                    try_fill_map[slot][times] = sql_tree_node
                else:
                    try_fill_map[slot][times] = get_merge_tree_node(try_fill_map[slot][times], sql_tree_node)
                return True, None, 0
            else:
                assert isinstance(pattern_tree_node.slot, ForSlot)
                assert pattern_tree_node.for_slot_ancestor is not None
                return True, pattern_tree_node.for_slot_ancestor, pattern_tree_node.for_slot_ancestor_id
        else:
            i = sql_begin_pos
            j = pattern_begin_pos
            match_begin = False
            while j < len(pattern_tree_node.children):
                while i < len(sql_tree_node.children):
                    flag, for_slot_ancestor_node, for_slot_id = match_tree_node(sql_tree_node.children[i],
                                                                                pattern_tree_node.children[j], add_map)
                    if for_slot_ancestor_node is not None:
                        if pattern_tree_node == for_slot_ancestor_node:
                            new_temp_map = {}
                            for_slot = pattern_tree_node.for_loop_slot[for_slot_id]
                            # we assert that the len of all the for loop is bigger than 1
                            first_tree = pattern_tree_node.for_loop_sub_trees[for_slot_id]['first_tree']
                            second_tree = pattern_tree_node.for_loop_sub_trees[for_slot_id]['second_tree']
                            flag_first, _, _ = match_tree_node(sql_tree_node, first_tree, new_temp_map, i, 0)

                            check_slot_value_map(new_temp_map)
                            if not flag_first:
                                flag = False
                            else:
                                assert isinstance(for_slot, ForSlot)
                                for k in range(len(for_slot.sub_ele_slots)):
                                    sub_ele = for_slot.sub_ele_slots[k]
                                    assert sub_ele in new_temp_map
                                    final_sub_ele_value = get_final_value(new_temp_map, sub_ele)
                                    if for_slot.ele_slots[k] not in add_map:
                                        add_map[for_slot.ele_slots[k]] = {}
                                    if 'full' not in add_map[for_slot.ele_slots[k]]:
                                        add_map[for_slot.ele_slots[k]]['full'] = []
                                    add_map[for_slot.ele_slots[k]]['full'].append(
                                        Operand(final_sub_ele_value, BaseType('')))
                                # consider slot not in sub_ele_list as global Variable
                                for key, value in new_temp_map.items():
                                    if key not in for_slot.sub_ele_slots:
                                        if 'full' in add_map[key]:
                                            assert value_compare(add_map[key]['full'], value)
                                        else:
                                            add_map[key]['full'] = value
                                i = i + len(first_tree.children) - 1
                                # prior to match the next pattern_tree_node tree node not the list
                                flag_next, _, _ = match_tree_node(sql_tree_node, pattern_tree_node, {}, i + 1, j + 1)
                                if not flag_next:
                                    i = i + 1
                                    j = j + 1
                                    continue
                                while True:
                                    i = i + 1
                                    new_temp_map = {}
                                    flag_second, _, _ = match_tree_node(sql_tree_node, second_tree, new_temp_map, i, 0)
                                    if not flag_second:
                                        # compensate for the i = i + 1 afterwards
                                        i = i - 1
                                        break
                                    else:
                                        for k in range(len(for_slot.sub_ele_slots)):
                                            sub_ele = for_slot.sub_ele_slots[k]
                                            assert sub_ele in new_temp_map
                                            final_sub_ele_value = get_final_value(new_temp_map, sub_ele)
                                            if for_slot.ele_slots[k] not in add_map:
                                                add_map[for_slot.ele_slots[k]] = {}
                                            if 'full' not in add_map[for_slot.ele_slots[k]]:
                                                add_map[for_slot.ele_slots[k]]['full'] = []
                                            add_map[for_slot.ele_slots[k]]['full'].append(
                                                Operand(final_sub_ele_value, BaseType('')))
                                        for key, value in new_temp_map.items():
                                            if key not in for_slot.sub_ele_slots:
                                                if 'full' in add_map[key]:
                                                    assert value_compare(add_map[key]['full'], value)
                                                else:
                                                    add_map[key]['full'] = value
                                        i = i + len(second_tree.children) - 1
                        else:
                            return True, for_slot_ancestor_node, for_slot_id
                    if flag:
                        j = j + 1
                        i = i + 1
                        match_begin = True
                        break
                    if match_begin:
                        # strict match, no empty
                        return False, None, 0
                    i = i + 1
                if i == len(sql_tree_node.children) and not j == len(pattern_tree_node.children):
                    return False, None, 0



            for slot, add_map_value in add_map.items():
                if slot not in try_fill_map:
                    try_fill_map[slot] = add_map_value
                    continue
                for time, temp_value in add_map_value.items():
                    if time == 'full':
                        if 'full' in try_fill_map[slot]:
                            assert value_compare(try_fill_map[slot]['full'], temp_value)
                    else:
                        if time not in try_fill_map[slot]:
                            try_fill_map[slot][time] = temp_value
                        else:
                            try_fill_map[slot][time] = get_merge_tree_node(try_fill_map[slot][time], temp_value)
        return True, None, 0
    else:
        return False, None, 0


def compare_slot_value(slot_type: BaseType, clone_node_map: dict,
                       value: Operand | List[Operand], dialect: str, db_name: str):
    if isinstance(slot_type, ListType):
        flag = True
        for i in range(len(value)):
            if isinstance(slot_type.element_type, ListType):
                assert isinstance(value[i], List)
            else:
                assert isinstance(value[i], Operand)
            assert value[i].value in clone_node_map
            flag = flag and compare_slot_value(slot_type.element_type, clone_node_map, value[i], dialect, db_name)
        return flag
    else:
        assert value.value in clone_node_map
        value_type = fetch_operand_type(db_name, clone_node_map[value.value], dialect)
        return type1_contains_type2(slot_type, value_type)


def no_space_and_case_insensitive_str(string_value: str):
    splits = string_value.split()
    result = ''
    for item in splits:
        result = result + item
    return result.lower()


def rewrite_tree_node(tree_node: TreeNode, src_pattern_trees: List[TreeNode], points: List[Point],
                      alias_id_map: dict[str, int], src_dialect, db_name, clone_node_map: dict):
    for child_node in tree_node.children:
        rewrite_tree_node(child_node, src_pattern_trees, points, alias_id_map,
                          src_dialect, db_name, clone_node_map)

    for i in range(len(src_pattern_trees)):
        try_fill_map = {}
        flag, _, _ = match_tree_node(tree_node, src_pattern_trees[i], try_fill_map)
        if flag:
            for slot, value in try_fill_map.items():
                used_value = get_final_value(try_fill_map, slot)
                assert isinstance(slot, ValueSlot)
                if isinstance(slot.get_type(), OptionType):
                    str_value = str(used_value)
                    temp_flag = False
                    for option_key, option_value in slot.get_type().map_dict.items():
                        if (no_space_and_case_insensitive_str(str_value) ==
                                no_space_and_case_insensitive_str(option_key)):
                            temp_flag = True
                            break
                    if not temp_flag:
                        flag = False
                        break
                elif (isinstance(slot.get_type(), OptionType) or isinstance(slot.get_type(), QueryType)
                      or isinstance(slot.get_type(), TableType)):
                    continue
                else:
                    if compare_slot_value(slot.get_type(), clone_node_map, used_value, src_dialect, db_name):
                        continue
                    else:
                        flag = False
                        break
            if not flag:
                continue
            slot_op_map = {}
            for slot, value in try_fill_map.items():
                used_value = get_final_value(try_fill_map, slot)
                if slot in slot_op_map:
                    assert slot_op_map[slot].value == str(used_value)
                else:
                    slot_op_map[slot] = Operand(used_value, slot.get_type())

            tree_node.value = points[i].tgt_pattern.fulfill_pattern(alias_id_map, slot_op_map)
            tree_node.is_terminal = True
            break


def get_merge_tree_node(node1: TreeNode, node2: TreeNode):
    # assert that node1 is on the left side of node2
    node1_path = [node1]
    while node1.father is not None:
        node1_path.insert(0, node1.father)
        node1 = node1.father
    node2_path = [node2]
    while node2.father is not None:
        node2_path.insert(0, node2.father)
        node2 = node2.father
    i = 0
    while i < len(node1_path) and i < len(node2_path):
        if node1_path[i] == node2_path[i]:
            i = i + 1
        else:
            break
    first_same_node = node1_path[i - 1]
    clone_first_same_node = TreeNode(first_same_node.value, first_same_node.dialect, first_same_node.is_terminal,
                                     first_same_node.father)
    node1_i = i
    last_node = clone_first_same_node
    while node1_i < len(node1_path):
        clone_node_path_node = TreeNode(node1_path[node1_i].value, node1_path[node1_i].dialect,
                                        node1_path[node1_i].is_terminal)
        last_node.add_child(clone_node_path_node)
        last_node = clone_node_path_node
        node1_i = node1_i + 1
    node2_i = i
    last_node = clone_first_same_node
    while node2_i < len(node2_path):
        clone_node_path_node = TreeNode(node2_path[node2_i].value, node2_path[node2_i].dialect,
                                        node2_path[node2_i].is_terminal)
        last_node.add_child(clone_node_path_node)
        last_node = clone_node_path_node
        node2_i = node2_i + 1
    return clone_first_same_node


def clone_node_mapping(node: TreeNode, node_map: dict):
    if node in node_map:
        return node_map[node]
    else:
        clone_node = TreeNode(node.value, node.dialect, node.is_terminal)
        node_map[node] = clone_node
        for child in node.children:
            clone_child = clone_node_mapping(child, node_map)
            clone_node.add_child(clone_child)
        return clone_node


def rewrite_sql(src_dialect, tgt_dialect, sql, points: List[Point], db_name: str):
    sql_tree_node, _, _, _ = parse_tree(sql, src_dialect)
    src_pattern_trees = []
    for point in points:
        pattern_tree = parse_pattern_tree(point.point_type, point.src_pattern, src_dialect)
        src_pattern_trees.append(pattern_tree)
    if sql_tree_node is None:
        return None
    sql_root_node = TreeNode.make_g4_tree_by_node(sql_tree_node, src_dialect)
    node_map = {}
    clone_sql_root_node = clone_node_mapping(sql_root_node, node_map)
    alias_id_map = {}
    rewrite_tree_node(sql_root_node, src_pattern_trees, points,
                      alias_id_map, src_dialect, db_name, node_map)
    print(sql_root_node)


with open(get_proj_root_path() + "/src/sql_gen/generator/sql.json", "r", encoding="utf-8") as file:
    json_content = json.load(file)
    parsed_points = []
    test_case = json_content[7]
    for point in test_case['points']:
        parsed_points.append(parse_point(point))
    rewrite_sql('oracle', 'pg', test_case['oracle'], parsed_points, db_name='bird')
