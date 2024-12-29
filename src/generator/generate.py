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
from generator.point_parser import *
from utils.tools import get_proj_root_path


def insert_select_list_pos(src_sql: str, dialect: str, insert_content: str):
    if dialect == 'mysql':
        node = TreeNode.make_g4_tree_by_node(parse_tree(src_sql, dialect), dialect)

    elif dialect == 'pg':
        pass
    elif dialect == 'oracle':
        pass
    else:
        raise ValueError("wrong input of dialect")


def gen_by_insert(src_pattern: Pattern, tgt_pattern: Pattern, src_dialect: str, tgt_dialect: str, functions: List,
                  dbname: str = 'BIRD') -> Dict:
    queries_path = os.path.join(get_proj_root_path(), 'data', dbname, 'query', f"{src_dialect}_{tgt_dialect}.json")
    print(queries_path)
    with open(queries_path, 'r') as file:
        queries = json.load(file)
    query = random.choice(queries)
    src_sql = query[src_dialect]
    tgt_sql = query[tgt_pattern]

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
    src_pattern, _ = parse_pattern(split(src_pattern), 0, len(src_split))

    tgt_split = split(tgt_pattern)
    tgt_pattern, _ = parse_pattern(split(tgt_split), 0, len(tgt_split))

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
