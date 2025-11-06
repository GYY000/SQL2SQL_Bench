# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: rep_non_deterministic$
# @Author: 10379
# @Time: 2025/8/8 15:13

import json
import os.path

from cracksql.utils.tools import print_err

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from sql_gen.generator.element.Point import Point
from sql_gen.generator.point_parser import parse_pattern
from sql_gen.generator.point_type.TranPointType import gen_point_type
from sql_gen.generator.rewriter import rewrite_sql
from utils.tools import get_proj_root_path


def rep_non_deterministic_function_list(sql: str, dialect: str):
    """directly using string replacing"""
    non_deterministic_function_key_list = {
        "mysql": [
            ['CURDATE()', 'CURDATE( )', 'CURRENT_DATE()', 'CURRENT_DATE( )', 'CURRENT_DATE'],
            ['CURRENT_TIMESTAMP()', 'CURRENT_TIMESTAMP( )', 'CURRENT_TIMESTAMP', 'NOW()', 'NOW( )'],
            ['CURTIME()', 'CURRENT_TIME()', 'CURRENT_TIME', 'CURTIME( )', 'CURRENT_TIME( )'],
            ['SYSDATE()', 'SYSDATE( )'],
            ['UTC_DATE()', 'UTC_DATE( )'],
            ['UTC_TIME()', 'UTC_TIME( )'],
            ['UTC_TIMESTAMP()', 'UTC_TIMESTAMP( )'],
        ],
        "pg": [
            ['CURRENT_DATE'],
            ['CURRENT_TIMESTAMP', 'NOW()'],
            ['LOCALTIMESTAMP'],
        ],
        "oracle": [
            ['CURRENT_DATE'],
            ['CURRENT_TIMESTAMP'],
            ['LOCALTIMESTAMP'],
            ['SYSDATE'],
            ['SYSTIMESTAMP']
        ]
    }
    non_deterministic_function_value_list = {
        "mysql": [
            "DATE '2000-01-02'",
            'TIMESTAMP \'2000-01-02 00:30:26\'',
            'CAST(\'00:30:21\' AS TIME)',
            'CAST(\'2000-01-02 00:30:21\' AS DATETIME)',
            'DATE \'2000-01-01\'',
            'CAST(\'16:30:21\' AS TIME)',
            'TIMESTAMP \'2000-01-01 16:30:21\''
        ],
        "pg": [
            "DATE '2000-01-01'",
            "TIMESTAMPTZ '2000-01-01 16:30:21+0'",
            "TIMESTAMP '2000-01-01 16:30:21'"
        ],
        "oracle": [
            'DATE \'2000-01-01\'',
            "TIMESTAMP '2000-01-01 16:30:21 +08:00'",
            "TIMESTAMP '2000-01-01 16:30:21'",
            'DATE \'2000-01-01\'',
            "TIMESTAMP '2000-01-01 16:30:21.126789 +08:00'",
        ]
    }
    keys = non_deterministic_function_key_list.get(dialect, [])
    values = non_deterministic_function_value_list.get(dialect, [])
    for key_group, value in zip(keys, values):
        for key in key_group:
            sql = sql.replace(key, value)
            sql = sql.replace(key.lower(), value)
    return sql


def parse_rule(rule: dict, dialect) -> Point:
    for key, value in rule.items():
        assert key in ['SrcPattern', 'TgtPattern', 'Type', 'Condition', 'Tag']
    # print(point)
    src_pattern = rule['SrcPattern']
    tgt_pattern = rule['TgtPattern']
    point_type = rule['Type']
    return_type = None
    predicate = None
    tag = None
    if 'Condition' in rule:
        predicate = rule['Condition']
    if 'Tag' in rule:
        tag = rule['Tag']
    slot_defs = [[]]
    point_type = gen_point_type(point_type)
    src_pattern, _ = parse_pattern(src_pattern, 0, dialect, slot_defs)
    tgt_pattern, _ = parse_pattern(tgt_pattern, 0, dialect, slot_defs)
    return Point('', dialect, dialect, src_pattern, tgt_pattern, slot_defs[0], point_type,
                 return_type, predicate, tag)


def rewrite_dialect_specific_func(sql: str | TreeNode, dialect):
    with open(os.path.join(get_proj_root_path(), 'src', 'verification', 'rewritten_rules', f'{dialect}.json'),
              'r') as f:
        rewritten_rules = json.load(f)
    # if isinstance(sql, str):
    #     root_node, _, _, _ = parse_tree(sql, dialect)
    #     if root_node is None:
    #         print_err(f"Rewrite for SQL {sql} failed for antlr parser.")
    #         return sql
    #     root_node = TreeNode.make_g4_tree_by_node(root_node, dialect)
    # else:
    #     root_node = sql
    points = []
    for rule in rewritten_rules:
        points.append(parse_rule(rule, dialect))
    rewrite_res, _, _ = rewrite_sql(dialect, dialect, sql, points)
    return rewrite_res
