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
from sql_gen.generator.element.Pattern import Slot, ValueSlot
from sql_gen.generator.element.Point import Point
from sql_gen.generator.element.method import parse_pattern_tree
from sql_gen.generator.point_parser import parse_point
from utils.tools import get_proj_root_path


def match_tree_node(tree_node: TreeNode, pattern_tree_node: TreeNode, try_fill_map: dict):
    if tree_node.value == pattern_tree_node.value:
        add_map = {}
        if pattern_tree_node.slot is not None:
            assert isinstance(pattern_tree_node.slot, ValueSlot)
            value = str(tree_node)
            try_fill_map[pattern_tree_node.slot] = Operand(value, BaseType(''))
        else:
            i = 0
            j = 0
            while j < len(pattern_tree_node.children):
                while i < len(tree_node.children):
                    if match_tree_node(tree_node.children[i], pattern_tree_node.children[j], add_map):
                        j = j + 1
                        break
                    i = i + 1
                if i == len(tree_node.children):
                    return False
            for slot, value in add_map.items():
                if slot in try_fill_map:
                    assert try_fill_map[slot].value != value.value
                else:
                    try_fill_map[slot] = value
        return True
    else:
        return False



def rewrite_tree_node(tree_node: TreeNode, src_pattern_trees: List[TreeNode], points: List[Point],
                      alias_id_map: dict[str, int]):
    for child_node in tree_node.children:
        rewrite_tree_node(child_node, src_pattern_trees, points, alias_id_map)

    for i in range(len(src_pattern_trees)):
        try_fill_map = {}
        if match_tree_node(tree_node, src_pattern_trees[i], try_fill_map):
            for slot, value in try_fill_map.items():
                assert isinstance(slot, ValueSlot)
                slot.fill_value(value)
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
    point = parse_point(json_content[0]['point'])
    rewrite_sql('mysql', 'pg', json_content[0]['mysql'], [point])
