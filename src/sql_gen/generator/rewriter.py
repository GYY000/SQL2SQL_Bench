# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: rewriter.py$
# @Author: 10379
# @Time: 2025/5/9 19:24
import json
import re
from typing import List

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree, parse_element_tree
from db_builder.normalize import rep_reserved_keyword_quote
from sql_gen.generator.ele_type.type_def import BaseType, OptionType, QueryType, TableType, ListType, AnyValueType, \
    is_str_type, is_num_type, is_time_type, IntLiteralType, StringLiteralType, WordLiteralType, FloatLiteralType, \
    XmlType, ArrayType, OrderByElementType, PointType, WindowDefinitionType, AliasType
from sql_gen.generator.element.Operand import Operand
from sql_gen.generator.element.Pattern import Slot, ValueSlot, ForSlot
from sql_gen.generator.element.Point import Point
from sql_gen.generator.fetch_operand_type import fetch_operand_type
from sql_gen.generator.method import merge_trans_points, add_point_to_point_dict
from sql_gen.generator.pattern_tree_parser import parse_pattern_tree, rm_nodes_in_range
from sql_gen.generator.point_loader import load_point_by_name
from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.point_type.TranPointType import ReservedKeywordType, LiteralType, ExpressionType
from sql_gen.generator.token_statistic import stat_begin_node_end_node, stat_tokens
from utils.ExecutionEnv import ExecutionEnv
from utils.tools import no_space_and_case_insensitive_str, get_all_db_name


def type_equal(type1: BaseType, type2: BaseType) -> bool:
    if type1 == type2:
        return True


def type1_contains_type2(type1: BaseType, type2: BaseType) -> bool:
    if (isinstance(type1, AnyValueType) or isinstance(type1, XmlType) or isinstance(type1, ArrayType) or
            isinstance(type1, PointType)):
        return True
    if type(type1).__name__ == type(type2).__name__:
        return True
    if type1.attr_container.has_strict() or type2.attr_container.has_strict():
        return False
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


def get_final_value(slot_value_map, slot: Slot) -> TreeNode | List:
    values = slot_value_map[slot]
    final_value = None
    for key, value in values.items():
        if final_value is None:
            final_value = value
        else:
            assert str(final_value).strip() == str(value).strip()
    assert final_value is not None
    assert isinstance(final_value, TreeNode) or isinstance(final_value, List)
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
        assert isinstance(value1, TreeNode) and isinstance(value2, TreeNode)
        return no_space_and_case_insensitive_str(str(value1)) == no_space_and_case_insensitive_str(str(value2))


def gen_pattern_string(ori_pattern, slot_list: list):
    # Position offset
    revised_pos = 0
    pattern = ori_pattern

    for slot_and_pos in slot_list:
        if isinstance(slot_and_pos['slot'].get_type(), IntLiteralType):
            pattern_str = r"([+-]?\d+)"
        elif isinstance(slot_and_pos['slot'].get_type(), StringLiteralType):
            pattern_str = r"('.*?')"
        elif isinstance(slot_and_pos['slot'].get_type(), WordLiteralType):
            pattern_str = r"(.*?)"
        elif isinstance(slot_and_pos['slot'].get_type(), FloatLiteralType):
            pattern_str = r'([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)'
        elif isinstance(slot_and_pos['slot'].get_type(), OptionType):
            # pattern_str = "(.*?)"
            pattern_str = '('
            for key, value in slot_and_pos['slot'].get_type().map_dict.items():
                if pattern_str != '(':
                    pattern_str += '|'
                pattern_str += re.escape(key)
            pattern_str += ')'
        else:
            assert False
        pattern = (pattern[:slot_and_pos['begin_pos'] + revised_pos] + pattern_str +
                   pattern[slot_and_pos['end_pos'] + 1 + revised_pos:])
        revised_pos += len(pattern_str) - (slot_and_pos['end_pos'] - slot_and_pos['begin_pos']) - 1
    pattern = f"^{pattern}$"
    return pattern


def match_tree_node(sql_tree_node: TreeNode, pattern_tree_node: TreeNode, try_fill_map: dict, sql_begin_pos=0,
                    pattern_begin_pos=0) -> tuple[bool, TreeNode | None, int, TreeNode | None, TreeNode | None]:
    if (sql_tree_node.value == pattern_tree_node.value or
            (sql_tree_node.is_terminal and pattern_tree_node.is_terminal and
             sql_tree_node.terminal_node_name == pattern_tree_node.terminal_node_name)):
        add_map = {}
        if len(pattern_tree_node.pos_to_slot) != 0:
            pattern_string = gen_pattern_string(pattern_tree_node.ori_pattern_string, pattern_tree_node.pos_to_slot)
            to_match_value = str(sql_tree_node)
            match = re.match(pattern_string, to_match_value)
            if not match:
                return False, None, 0, None, None
            for i, value in enumerate(pattern_tree_node.pos_to_slot):
                slot = value['slot']
                times = pattern_tree_node.slot_times[slot]
                if slot not in try_fill_map:
                    try_fill_map[slot] = {}
                if times not in try_fill_map[slot]:
                    try_fill_map[slot][times] = TreeNode(match.group(i + 1), sql_tree_node.dialect, True)
            cur_first_node = sql_tree_node
            cur_last_node = sql_tree_node
        elif pattern_tree_node.slot is not None:
            if isinstance(pattern_tree_node.slot, ValueSlot):
                slot = pattern_tree_node.slot
                times = pattern_tree_node.slot_times[slot]
                if pattern_tree_node.slot not in try_fill_map:
                    try_fill_map[slot] = {}
                if times not in try_fill_map[pattern_tree_node.slot]:
                    try_fill_map[slot][times] = sql_tree_node
                else:
                    try_fill_map[slot][times] = get_merge_tree_node(try_fill_map[slot][times], sql_tree_node)
                return True, None, 0, sql_tree_node, sql_tree_node
            else:
                assert isinstance(pattern_tree_node.slot, ForSlot)
                assert pattern_tree_node.for_slot_ancestor is not None
                return (True, pattern_tree_node.for_slot_ancestor, pattern_tree_node.for_slot_ancestor_id,
                        sql_tree_node, sql_tree_node)
        else:
            i = sql_begin_pos
            j = pattern_begin_pos
            match_begin = False
            cur_first_node = None
            cur_last_node = None
            if len(pattern_tree_node.children) == 0:
                return True, None, 0, sql_tree_node, sql_tree_node
            while j < len(pattern_tree_node.children):
                while i < len(sql_tree_node.children):
                    flag, for_slot_ancestor_node, for_slot_id, first_node, last_node = match_tree_node(
                        sql_tree_node.children[i],
                        pattern_tree_node.children[j], add_map)
                    if for_slot_ancestor_node is not None:
                        if pattern_tree_node == for_slot_ancestor_node:
                            new_temp_map = {}
                            for_slot = pattern_tree_node.for_loop_slot[for_slot_id]
                            # we assert that the len of all the for loop is bigger than 1
                            first_tree = pattern_tree_node.for_loop_sub_trees[for_slot_id]['first_tree']
                            second_tree = pattern_tree_node.for_loop_sub_trees[for_slot_id]['second_tree']
                            flag_first, _, _, first_node, last_node = match_tree_node(sql_tree_node, first_tree,
                                                                                      new_temp_map, i, 0)
                            check_slot_value_map(new_temp_map)
                            if not flag_first:
                                flag = False
                            else:
                                if cur_first_node is None:
                                    cur_first_node = first_node
                                cur_last_node = last_node
                                assert isinstance(for_slot, ForSlot)
                                for k in range(len(for_slot.sub_ele_slots)):
                                    sub_ele = for_slot.sub_ele_slots[k]
                                    assert sub_ele in new_temp_map
                                    final_sub_ele_value = get_final_value(new_temp_map, sub_ele)
                                    if for_slot.ele_slots[k] not in add_map:
                                        add_map[for_slot.ele_slots[k]] = {}
                                    if 'full' not in add_map[for_slot.ele_slots[k]]:
                                        add_map[for_slot.ele_slots[k]]['full'] = []
                                    add_map[for_slot.ele_slots[k]]['full'].append(final_sub_ele_value)
                                # consider slot not in sub_ele_list as global Variable
                                for key, value in new_temp_map.items():
                                    if key not in for_slot.sub_ele_slots:
                                        if 'full' in add_map[key]:
                                            assert value_compare(add_map[key]['full'], value)
                                        else:
                                            add_map[key]['full'] = value
                                i = i + len(first_tree.children) - 1
                                # prior to match the next pattern_tree_node tree node not the list
                                flag_next = True
                                if i + 1 < len(sql_tree_node.children) and j + 1 < len(pattern_tree_node.children):
                                    # Need Further Revision
                                    tempi = i + 1
                                    tempj = j + 1
                                    while (tempi < len(sql_tree_node.children) and
                                           tempj < len(pattern_tree_node.children)):
                                        flag_next, _, _, first_node, last_node = match_tree_node(
                                            sql_tree_node.children[tempi],
                                            pattern_tree_node.children[tempj], {},
                                            0, 0)
                                        tempi += 1
                                        tempj += 1
                                        if not flag_next:
                                            break
                                else:
                                    flag_next = False
                                if flag_next:
                                    i = i + 1
                                    j = j + 1
                                    continue
                                while True:
                                    i = i + 1
                                    if i < len(sql_tree_node.children) and j + 1 < len(pattern_tree_node.children):
                                        # Need Further Revision
                                        flag_next = True
                                        tempi = i
                                        tempj = j + 1
                                        while (tempi < len(sql_tree_node.children) and
                                               tempj < len(pattern_tree_node.children)):
                                            flag_next, _, _, first_node, last_node = match_tree_node(
                                                sql_tree_node.children[tempi],
                                                pattern_tree_node.children[tempj], {},
                                                0, 0)
                                            tempi += 1
                                            tempj += 1
                                            if not flag_next:
                                                break
                                    else:
                                        flag_next = False
                                    if flag_next:
                                        i = i - 1
                                        break
                                    new_temp_map = {}
                                    flag_second, _, _, first_node, last_node = (
                                        match_tree_node(sql_tree_node, second_tree, new_temp_map, i, 0))
                                    if not flag_second:
                                        # compensate for the i = i + 1 afterwards
                                        i = i - 1
                                        break
                                    else:
                                        if cur_first_node is None:
                                            cur_first_node = first_node
                                        cur_last_node = last_node
                                        for k in range(len(for_slot.sub_ele_slots)):
                                            sub_ele = for_slot.sub_ele_slots[k]
                                            assert sub_ele in new_temp_map
                                            final_sub_ele_value = get_final_value(new_temp_map, sub_ele)
                                            if for_slot.ele_slots[k] not in add_map:
                                                add_map[for_slot.ele_slots[k]] = {}
                                            if 'full' not in add_map[for_slot.ele_slots[k]]:
                                                add_map[for_slot.ele_slots[k]]['full'] = []
                                            add_map[for_slot.ele_slots[k]]['full'].append(
                                                final_sub_ele_value)
                                        for key, value in new_temp_map.items():
                                            if key not in for_slot.sub_ele_slots:
                                                if 'full' in add_map[key]:
                                                    assert value_compare(add_map[key]['full'], value)
                                                else:
                                                    add_map[key]['full'] = value
                                        i = i + len(second_tree.children) - 1
                        else:
                            return True, for_slot_ancestor_node, for_slot_id, first_node, last_node
                    else:
                        if flag:
                            if cur_first_node is None:
                                cur_first_node = first_node
                            cur_last_node = last_node
                    if flag:
                        j = j + 1
                        i = i + 1
                        match_begin = True
                        break
                    if match_begin:
                        # strict match, no empty
                        return False, None, 0, None, None
                    i = i + 1
                if i == len(sql_tree_node.children) and not j == len(pattern_tree_node.children):
                    return False, None, 0, None, None
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
        return True, None, 0, cur_first_node, cur_last_node
    else:
        return False, None, 0, None, None


def is_literal_type(value_type):
    return (isinstance(value_type, OptionType) or isinstance(value_type, StringLiteralType)
            or isinstance(value_type, IntLiteralType) or isinstance(value_type, WordLiteralType))


def compare_slot_value(slot_type: BaseType, clone_node_map: dict,
                       value: TreeNode | List[TreeNode], execution_env: ExecutionEnv):
    if isinstance(slot_type, ListType):
        flag = True
        for i in range(len(value)):
            if isinstance(slot_type.element_type, ListType):
                assert isinstance(value[i], List)
            else:
                assert isinstance(value[i], TreeNode)
                assert (value[i] in clone_node_map
                        or is_literal_type(slot_type.element_type))
            flag = flag and compare_slot_value(slot_type.element_type, clone_node_map, value[i], execution_env)
        return flag
    else:
        if (isinstance(slot_type, AnyValueType) or isinstance(slot_type, OrderByElementType)
                or isinstance(slot_type, WindowDefinitionType) or isinstance(slot_type, AliasType) or
                isinstance(slot_type, TableType) or isinstance(slot_type, QueryType)):
            return True
        if isinstance(slot_type, OptionType):
            str_value = str(value)
            temp_flag = False
            for option_key, option_value in slot_type.map_dict.items():
                if (no_space_and_case_insensitive_str(str_value) ==
                        no_space_and_case_insensitive_str(option_key)):
                    temp_flag = True
                    break
            return temp_flag
        if value in clone_node_map:
            # if value is a literal, just use antlr parser to judge the type
            value_type = fetch_operand_type(clone_node_map[value], execution_env)
        else:
            # It's the literal Type values
            if isinstance(slot_type, StringLiteralType):
                if value.value.startswith('\'') and value.value.endswith('\''):
                    return True
                return False
            elif isinstance(slot_type, WordLiteralType):
                if re.match(r'^[A-Za-z0-9]+$', value.value.strip()):
                    return True
                else:
                    return False
            elif isinstance(slot_type, FloatLiteralType):
                try:
                    number = float(value.value)
                    return True
                except ValueError:
                    return False
            else:
                assert isinstance(slot_type, IntLiteralType)
                try:
                    number = int(value.value)
                    return True
                except ValueError:
                    return False
        return type1_contains_type2(slot_type, value_type)


def rewrite_tree_node(tree_node: TreeNode, src_pattern_trees: List[TreeNode], points: List[Point],
                      alias_id_map: dict[str, int], execution_env: ExecutionEnv,
                      clone_node_map: dict, pattern_tree_to_map: dict, tgt_dialect: str):
    all_rewrite_token = 0
    all_rewrite_points = []
    for child_node in tree_node.children:
        new_rewrite_token, new_rewrite_points = rewrite_tree_node(child_node, src_pattern_trees, points, alias_id_map,
                                                                  execution_env, clone_node_map,
                                                                  pattern_tree_to_map, tgt_dialect)
        all_rewrite_token += new_rewrite_token
        all_rewrite_points = merge_trans_points(all_rewrite_points, new_rewrite_points)
    for i in range(len(src_pattern_trees)):
        try_fill_map = {}
        flag, _, _, first_node, last_node = match_tree_node(tree_node, src_pattern_trees[i], try_fill_map)

        if flag:
            for slot, value in try_fill_map.items():
                used_value = get_final_value(try_fill_map, slot)
                assert isinstance(slot, ValueSlot)
                if slot.udf_func is not None:
                    if not slot.udf_func.fulfill_cond(used_value):
                        flag = False
                        break
                if (isinstance(slot.get_type(), AnyValueType) or
                        compare_slot_value(slot.get_type(), clone_node_map, used_value, execution_env)):
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
                    slot_op_map[slot] = make_value_op(used_value, slot.get_type())
            final_value = points[i].tgt_pattern.fulfill_pattern(alias_id_map, slot_op_map, None,
                                                                execution_env, execution_env.dialect, False)
            point_type = pattern_tree_to_map[id(src_pattern_trees[i])]
            if isinstance(point_type, LiteralType):
                parse_type = ExpressionType()
            else:
                parse_type = point_type
            final_value_tree_node, _, _, _ = parse_element_tree(final_value, tgt_dialect, parse_type)
            if final_value_tree_node is None:
                print(f"\033[91m Parse Error {final_value} \033[0m")
            else:
                final_value_tree_node = TreeNode.make_g4_tree_by_node(final_value_tree_node, tgt_dialect)
            src_token_cnt, _ = stat_begin_node_end_node(tree_node, first_node, last_node, False)
            all_rewrite_token += src_token_cnt
            tgt_token_cnt = stat_tokens(final_value_tree_node)
            all_rewrite_token += tgt_token_cnt
            add_point_to_point_dict(all_rewrite_points, points[i].point_name)
            if tree_node is None:
                raise ValueError(f"Failed to parse the pattern {final_value}")
            if first_node == tree_node:
                assert last_node == tree_node
                father_node = tree_node.father
                assert isinstance(father_node, TreeNode)
                tree_node.children = []
                tree_node.add_child(final_value_tree_node)
            else:
                begin_pos_node = first_node
                end_pos_node = last_node
                while begin_pos_node.father != tree_node and begin_pos_node.father is not None:
                    begin_pos_node = begin_pos_node.father
                while end_pos_node.father != tree_node and begin_pos_node.father is not None:
                    end_pos_node = end_pos_node.father
                begin_pos = -1
                end_pos = -1
                for j in range(len(tree_node.children)):
                    if tree_node.children[j] == begin_pos_node:
                        begin_pos = j
                        break
                for j in range(len(tree_node.children)):
                    if tree_node.children[j] == end_pos_node:
                        end_pos = j
                        break
                assert begin_pos != -1 and end_pos != -1 and begin_pos != end_pos
                rm_nodes_in_range(tree_node, begin_pos_node, end_pos_node)
                if begin_pos_node not in tree_node.children:
                    tree_node.children.insert(begin_pos, final_value_tree_node)
                else:
                    tree_node.children.insert(begin_pos + 1, final_value_tree_node)
            break
    return all_rewrite_token, all_rewrite_points


def make_value_op(value, slot_type):
    if isinstance(value, List):
        res = []
        assert isinstance(slot_type, ListType)
        for sub_value in value:
            res.append(make_value_op(sub_value, slot_type.element_type))
    else:
        assert isinstance(value, TreeNode)
        res = Operand(value, slot_type)
    return res


def get_merge_tree_node(node1: TreeNode, node2: TreeNode):
    # assert that node1 is on the left side of node2
    ori_node1 = node1
    ori_node2 = node2
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
        if node1_path[node1_i] == ori_node1:
            clone_node_path_node = ori_node1
        else:
            clone_node_path_node = TreeNode(node1_path[node1_i].value, node1_path[node1_i].dialect,
                                            node1_path[node1_i].is_terminal)
        last_node.add_child(clone_node_path_node)
        last_node = clone_node_path_node
        node1_i = node1_i + 1
    node2_i = i
    last_node = clone_first_same_node
    while node2_i < len(node2_path):
        if node2_path[node2_i] == ori_node2:
            clone_node_path_node = ori_node2
        else:
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


def rewrite_sql(src_dialect, tgt_dialect, sql, points: List[Point], extra_db_param: dict = None):
    execution_env = ExecutionEnv(src_dialect, get_all_db_name(src_dialect))
    sql_tree_node, _, _, _ = parse_tree(sql, src_dialect)
    src_pattern_trees = []
    keyword_points = []
    pattern_tree_to_map = {}
    for point in points:
        assert isinstance(point, Point)
        if point.tag is not None and 'DB PARAMETER' in point.tag:
            for key, value in point.tag['DB PARAMETER'].items():
                flag = execution_env.add_param(key, value)
                if not flag:
                    raise ValueError('DB Parameter conflict')
        pattern_tree = parse_pattern_tree(point.point_type, point.src_pattern, src_dialect)
        src_pattern_trees.append(pattern_tree)
        pattern_tree_to_map[id(pattern_tree)] = point.point_type
    if extra_db_param is not None:
        for dialect, dialect_params in extra_db_param.items():
            flag = execution_env.add_param(dialect, dialect_params)
            if not flag:
                raise ValueError('DB Parameter conflict')
    if sql_tree_node is None:
        return None, 0, []
    sql_root_node = TreeNode.make_g4_tree_by_node(sql_tree_node, src_dialect)
    node_map = {}
    _ = clone_node_mapping(sql_root_node, node_map)
    rep_reserved_keyword_quote(None, sql_root_node, src_dialect, tgt_dialect)
    alias_id_map = {}
    all_rewrite_token, all_rewrite_points = rewrite_tree_node(sql_root_node, src_pattern_trees, points,
                                                              alias_id_map, execution_env, node_map,
                                                              pattern_tree_to_map, tgt_dialect)
    return str(sql_root_node), all_rewrite_token, all_rewrite_points

# with open('/home/gyy/SQL2SQL_Bench/src/sql_gen/generator/sql.json', 'r') as f:
#     sqls = json.load(f)
#
# test_sql = sqls[-1]
# src_dialect = 'mysql'
# tgt_dialect = 'oracle'
# sql = test_sql[src_dialect]
# points = []
#
# for point in test_sql['points']:
#     point_info = load_point_by_name(src_dialect, tgt_dialect, point)
#     points.append(parse_point(point_info))
# print(rewrite_sql(src_dialect, tgt_dialect, sql, points))
