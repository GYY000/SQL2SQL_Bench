# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: rewriter.py$
# @Author: 10379
# @Time: 2025/5/9 19:24
import json
from typing import List

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from sql_gen.generator.ele_type.type_def import BaseType
from sql_gen.generator.element.Operand import Operand
from sql_gen.generator.element.Pattern import Slot, ValueSlot, ForSlot
from sql_gen.generator.element.Point import Point
from sql_gen.generator.element.method import parse_pattern_tree
from sql_gen.generator.point_parser import parse_point
from utils.tools import get_proj_root_path


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
                value = str(sql_tree_node)
                slot = pattern_tree_node.slot
                times = pattern_tree_node.slot_times
                if pattern_tree_node.slot not in try_fill_map:
                    try_fill_map[slot] = {}
                if times not in try_fill_map[pattern_tree_node.slot]:
                    try_fill_map[slot][times] = ''
                try_fill_map[slot][times] = try_fill_map[slot][times] + ' ' + value
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
                                for i in range(len(for_slot.sub_ele_slots)):
                                    sub_ele = for_slot.sub_ele_slots[i]
                                    assert sub_ele in new_temp_map
                                    final_sub_ele_value = get_final_value(new_temp_map, sub_ele)
                                    if for_slot.ele_slots[i] not in add_map:
                                        add_map[for_slot.ele_slots[i]] = {}
                                    if 'full' not in add_map[for_slot.ele_slots[i]]:
                                        add_map[for_slot.ele_slots[i]]['full'] = []
                                    add_map[for_slot.ele_slots[i]]['full'].append(
                                        Operand(final_sub_ele_value, BaseType('')))
                                # consider slot not in sub_ele_list as global Variable
                                for key, value in new_temp_map.items():
                                    if key not in for_slot.sub_ele_slots:
                                        if 'full' in add_map[key]:
                                            assert value_compare(add_map[key]['full'], value)
                                        else:
                                            add_map[key]['full'] = value
                                i = i + len(first_tree.children)
                                while True:
                                    new_temp_map = {}
                                    flag_second, _, _ = match_tree_node(sql_tree_node, second_tree, new_temp_map, i, 0)
                                    if not flag_second:
                                        break
                                    else:
                                        for i in range(len(for_slot.sub_ele_slots)):
                                            sub_ele = for_slot.sub_ele_slots[i]
                                            assert sub_ele in new_temp_map
                                            final_sub_ele_value = get_final_value(new_temp_map, sub_ele)
                                            if for_slot.ele_slots[i] not in add_map:
                                                add_map[for_slot.ele_slots[i]] = {}
                                            if 'full' not in add_map[for_slot.ele_slots[i]]:
                                                add_map[for_slot.ele_slots[i]]['full'] = []
                                            add_map[for_slot.ele_slots[i]]['full'].append(
                                                Operand(final_sub_ele_value, BaseType('')))
                                        for key, value in new_temp_map.items():
                                            if key not in for_slot.sub_ele_slots:
                                                if 'full' in add_map[key]:
                                                    assert value_compare(add_map[key]['full'], value)
                                                else:
                                                    add_map[key]['full'] = value
                                        i = i + len(second_tree.children)
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
                            try_fill_map[slot][time] = try_fill_map[slot][time] + temp_value
        return True, None, 0
    else:
        return False, None, 0


def rewrite_tree_node(tree_node: TreeNode, src_pattern_trees: List[TreeNode], points: List[Point],
                      alias_id_map: dict[str, int]):
    for child_node in tree_node.children:
        rewrite_tree_node(child_node, src_pattern_trees, points, alias_id_map)

    for i in range(len(src_pattern_trees)):
        try_fill_map = {}
        flag, _, _ = match_tree_node(tree_node, src_pattern_trees[i], try_fill_map)
        if flag:
            for slot, value in try_fill_map.items():
                used_value = get_final_value(try_fill_map, slot)
                slot.fill_value(Operand(used_value, BaseType('')))
            tree_node.value = points[i].tgt_pattern.fulfill_pattern(alias_id_map)
            tree_node.is_terminal = True
            break


def rewrite_sql(src_dialect, tgt_dialect, sql, points: List[Point]):
    sql_tree_node, _, _, _ = parse_tree(sql, src_dialect)
    src_pattern_trees = []
    for point in points:
        src_pattern_trees.append(parse_pattern_tree(point.point_type, point.src_pattern, src_dialect))
    if sql_tree_node is None:
        return None
    sql_root_node = TreeNode.make_g4_tree_by_node(sql_tree_node, src_dialect)
    alias_id_map = {}
    rewrite_tree_node(sql_root_node, src_pattern_trees, points, alias_id_map)
    print(sql_root_node)


with open(get_proj_root_path() + "/src/sql_gen/generator/sql.json", "r", encoding="utf-8") as file:
    json_content = json.load(file)
    parsed_points = []
    for point in json_content[1]['points']:
        parsed_points.append(parse_point(point))
    rewrite_sql('mysql', 'pg', json_content[1]['mysql'], parsed_points)
