# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: generate$
# @Author: 10379
# @Time: 2024/12/28 19:31
import json
import os.path
import random
from typing import Dict

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from generator.element.Pattern import Pattern
from generator.point_parser import *
from schema.get_type import get_type, get_usable_cols
from utils.tools import get_proj_root_path


def check_select_cond(src_sql: str, dialect: str) -> bool:
    node, _, _, _ = parse_tree(src_sql, dialect)
    node = TreeNode.make_g4_tree_by_node(node, dialect)
    return get_select_list_node(node, dialect) is not None


def get_select_list_node(node: TreeNode, dialect: str):
    if dialect == 'mysql':
        while node.value != 'selectStatement':
            node = node.children[0]
        if node.get_child_by_value('UNION') is not None:
            # TODO: deal with union except and intersect...
            return None
        else:
            if node.get_child_by_value('querySpecificationNointo') is not None:
                node = node.get_child_by_value('querySpecificationNointo')
            elif node.get_child_by_value('querySpecification') is not None:
                node = node.get_child_by_value('querySpecification')
            elif node.get_child_by_value('queryExpressionNointo') is not None:
                node = node.get_child_by_value('queryExpressionNointo')
                while node.get_child_by_value('querySpecificationNointo') is None:
                    node = node.get_child_by_value('queryExpressionNointo')
                    assert node is not None
                node = node.get_child_by_value('querySpecificationNointo')
            elif node.get_child_by_value('queryExpression') is not None:
                node = node.get_child_by_value('queryExpression')
                while node.get_child_by_value('querySpecification') is None:
                    node = node.get_child_by_value('queryExpression')
                    assert node is not None
                node = node.get_child_by_value('querySpecification')
            node = node.get_child_by_value('selectElements')
            assert node is not None
            return node
    elif dialect == 'pg':
        while node.value != 'select_no_parens' and node.value != 'select_with_parens':
            node = node.children[0]
        while node.value == 'select_with_parens':
            used_node = node.get_child_by_value('select_no_parens')
            while used_node is None:
                node = node.get_child_by_value('select_with_parens')
                assert node is not None
                used_node = node.get_child_by_value('select_no_parens')
            node = used_node
            assert isinstance(node, TreeNode)
            select_clause_node = node.get_child_by_value('select_clause')
            assert isinstance(select_clause_node, TreeNode)
            if len(select_clause_node.get_children_by_value('simple_select_intersect')) != 1:
                # TODO: deal with UNION
                return None
            else:
                simple_select_intersect_node = select_clause_node.get_child_by_value('simple_select_intersect')
                if len(simple_select_intersect_node.get_children_by_value('simple_select_pramary')) != 1:
                    # TODO: deal with intersect
                    return None
                else:
                    simple_select_primary_node = simple_select_intersect_node.get_child_by_value(
                        'simple_select_pramary')
                    assert isinstance(simple_select_primary_node, TreeNode)
                    if simple_select_primary_node.get_child_by_value('select_with_parens') is None:
                        node = simple_select_primary_node.get_child_by_value('select_with_parens')
                        break
                assert node is not None
                target_list = node.get_child_by_value('target_list')
                if target_list is None:
                    opt_target_list = node.get_child_by_value('opt_target_list')
                    target_list = opt_target_list.get_child_by_value('target_list')
                    assert target_list is not None
                return target_list
    elif dialect == 'oracle':
        pass


def insert_select_list_pos(src_sql: str, dialect: str, insert_content: str) -> str:
    if dialect == 'mysql':
        node = TreeNode.make_g4_tree_by_node(parse_tree(src_sql, dialect), dialect)
        select_lists_node = get_select_list_node(node, dialect)
        assert isinstance(select_lists_node, TreeNode)
        if select_lists_node.get_child_by_value('*') is not None:
            select_lists_node.rm_child_by_value('*')
            select_lists_node.add_child(TreeNode(insert_content, dialect, True))
        else:
            select_lists_node.add_child(TreeNode(',' + insert_content, dialect, True))
    elif dialect == 'pg':
        pass
    elif dialect == 'oracle':
        pass
    else:
        assert False


def gen_by_insert(point: Point, src_dialect: str, tgt_dialect: str, dbname: str = 'bird') -> Dict:
    queries_path = os.path.join(get_proj_root_path(), 'data', dbname, 'query', f"{src_dialect}_{tgt_dialect}.json")
    with open(queries_path, 'r') as file:
        queries = json.load(file)

    query = random.choice(queries)
    while not check_select_cond(query[src_dialect], src_dialect) or not check_select_cond(query[tgt_dialect],
                                                                                          tgt_dialect):
        query = random.choice(queries)
    src_sql = query[src_dialect]
    tgt_sql = query[tgt_dialect]

    src_normal_cols, src_aggregate_cols, src_group_by_node = get_usable_cols(dbname, src_sql, src_dialect)
    tgt_normal_cols, tgt_aggregate_cols, tgt_group_by_node = get_usable_cols(dbname, tgt_sql, tgt_dialect)

    # print(src_normal_cols)
    # print(tgt_normal_cols)
    col_pairs = []
    for src_col in src_normal_cols:
        tgt_col = None
        for col in tgt_normal_cols:
            if src_col.value.lower() == col.value.lower():
                tgt_col = col
        if tgt_col is not None:
            col_pairs.append({
                src_dialect: src_col,
                tgt_dialect: tgt_col
            })
    for value_slot in point.slots:
        assert isinstance(value_slot, ValueSlot)
        value_slot.prefill(col_pairs)

    return {
        src_dialect: "",
        tgt_dialect: ""
    }


def gen_by_transform(src_pattern: Point, src_dialect: str, tgt_dialect: str) -> Dict:
    return {
        src_dialect: "",
        tgt_dialect: ""
    }


def generate_by_point(point: Dict, src_dialect: str, tgt_dialect: str) -> object:
    used_point = parse_point(point, src_dialect, tgt_dialect)
    if point["type"] == 'insert':
        return gen_by_insert(used_point, src_dialect, tgt_dialect)
    else:
        return gen_by_transform(used_point, src_dialect, tgt_dialect)


def test():
    with open('D:\\Coding\\SQL2SQL_Bench\\conv_point\\test_point.json', 'r') as file:
        points = json.load(file)

    for point in points:
        generate_by_point(point, 'mysql', 'pg')


test()
