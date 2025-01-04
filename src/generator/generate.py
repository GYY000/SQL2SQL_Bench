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
    if dialect == 'mysql':
        node = TreeNode.make_g4_tree_by_node(parse_tree(src_sql, dialect), dialect)
        while (node.value != 'selectStatement'):
            node = node.children[0]
        if node.get_child_by_value('UNION') is not None:
            # TODO: strategy to use sql have UNION is to be solved
            return False
        else:
            return True
    elif dialect == 'pg':
        return True
    elif dialect == 'oracle':
        return False
    else:
        assert False


def insert_select_list_pos(src_sql: str, dialect: str, insert_content: str) -> str:
    if dialect == 'mysql':
        node = TreeNode.make_g4_tree_by_node(parse_tree(src_sql, dialect), dialect)
        while (node.value != 'selectStatement'):
            node = node.children[0]
        if node.get_child_by_value('UNION') is not None:
            assert False
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
    elif dialect == 'pg':
        pass
    elif dialect == 'oracle':
        pass
    else:
        assert False


def gen_by_insert(src_pattern: Pattern, tgt_pattern: Pattern, src_dialect: str, tgt_dialect: str, functions: List,
                  dbname: str = 'BIRD') -> Dict:
    queries_path = os.path.join(get_proj_root_path(), 'data', dbname, 'query', f"{src_dialect}_{tgt_dialect}.json")
    with open(queries_path, 'r') as file:
        queries = json.load(file)

    query = random.choice(queries)
    while not check_select_cond(query[src_dialect], src_dialect) or not check_select_cond(query[tgt_dialect],
                                                                                          tgt_dialect):
        query = random.choice(queries)
    src_sql = query[src_dialect]
    tgt_sql = query[tgt_pattern]

    normal_cols, aggregate_cols, group_by_node = get_usable_cols(dbname, src_sql, src_dialect)
    for value_slot in src_pattern.value_slots:
        pass
    return {
        src_dialect: "",
        tgt_dialect: ""
    }


def gen_by_transform(src_pattern: Pattern, tgt_pattern: Pattern, src_dialect: str, tgt_dialect: str,
                     functions: List) -> Dict:
    return {
        src_dialect: "",
        tgt_dialect: ""
    }


def generate_by_point(point: Dict, src_dialect, tgt_dialect):
    src_pattern = point[src_dialect]
    tgt_pattern = point[tgt_dialect]

    src_split = split(src_pattern)
    src_pattern, _ = parse_pattern(split(src_pattern), 0, len(src_split), src_dialect)

    tgt_split = split(tgt_pattern)
    tgt_pattern, _ = parse_pattern(split(tgt_split), 0, len(tgt_split), tgt_dialect)

    functions = None
    if 'func_def' in point:
        """
        {
            "func_name": "",
            "func_definition": ""
        }
        """
        functions = point['func_def']

    if point["type"] == 'insert':
        return gen_by_insert(src_pattern, tgt_pattern, src_dialect, tgt_dialect, functions)
    else:
        return gen_by_transform(src_pattern, tgt_pattern, src_dialect, tgt_dialect, functions)


def test():
    with open('D:\\Coding\\SQL2SQL_Bench\\conv_point\\test_point.json', 'r') as file:
        points = json.load(file)

    for point in points:
        generate_by_point(point, 'mysql', 'postgres')
