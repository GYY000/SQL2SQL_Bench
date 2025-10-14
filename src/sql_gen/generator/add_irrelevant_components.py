# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: add_irrelevant_components$
# @Author: 10379
# @Time: 2025/7/29 2:41
import json
import os
import random

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from sql_gen.generator.ele_type.type_conversion import type_mapping
from sql_gen.generator.method import merge_query
from sql_gen.generator.point_loader import load_point_by_name
from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.rewriter import rewrite_sql
from sql_gen.generator.token_statistic import stat_tokens
from utils.ExecutionEnv import ExecutionEnv
from utils.tools import get_db_ids, get_proj_root_path, get_all_db_name


def fetch_no_points_sqls(src_dialect, tgt_dialect):
    db_ids = get_db_ids()
    all_sqls = []
    for db in db_ids:
        sql_root_path = os.path.join(get_proj_root_path(), 'SQL', db)
        if os.path.exists(os.path.join(sql_root_path, 'no_points')):
            path1 = os.path.join(sql_root_path, 'no_points', f'{src_dialect}_{tgt_dialect}.json')
            path2 = os.path.join(sql_root_path, 'no_points', f'{tgt_dialect}_{src_dialect}.json')
            if os.path.exists(path1):
                with open(path1, 'r') as file:
                    sqls = json.load(file)
            elif os.path.exists(path2):
                with open(path2, 'r') as file:
                    sqls = json.load(file)
            else:
                assert False
            assert sqls is not None
            for sql in sqls:
                if 'points' in sql:
                    if len(sql['points']) > 0:
                        print(path1)
                        print(path2)
                        print(sql)
                point_list = sql.get('points', [])
                all_sqls.append(sql)
    return all_sqls


def union_query(sql1: str, sql2: str, src_dialect: str):
    return f"{sql1} UNION ({sql2})"


def stat_token_query(sql: str, src_dialect: str):
    tree_node, _, _, _ = parse_tree(sql, src_dialect)
    if tree_node is None:
        return None
    tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
    return stat_tokens(tree_node)


def add_irrelevant_sql(sql1: dict, src_dialect: str, tgt_dialect: str, to_add_tokens: int):
    src_execute_env = ExecutionEnv(src_dialect, get_all_db_name(src_dialect))
    tgt_execute_env = ExecutionEnv(tgt_dialect, get_all_db_name(tgt_dialect))
    for point in sql1['points']:
        if isinstance(point, dict):
            point = point['point']
        point1 = load_point_by_name(src_dialect, tgt_dialect, point)
        if "Tag" in point1:
            tag = point1['Tag']
            if 'DB PARAMETER' in tag:
                for key, value in point1['Tag']['DB PARAMETER'].items():
                    flag1 = src_execute_env.add_param(key, value)
                    flag2 = tgt_execute_env.add_param(key, value)
                    if not flag1 or not flag2:
                        return None
    # src_root_node, _, _, _ = parse_tree(sql1[src_dialect].strip(';'), src_dialect)
    # src_root_node = TreeNode.make_g4_tree_by_node(src_root_node, src_dialect)
    # tgt_root_node, _, _, _ = parse_tree(sql1[tgt_dialect].strip(';'), tgt_dialect)
    # tgt_root_node = TreeNode.make_g4_tree_by_node(tgt_root_node, tgt_dialect)
    # flag, src_ctes = analysis_ctes(src_root_node, src_dialect)
    # flag, tgt_ctes = analysis_ctes(tgt_root_node, tgt_dialect)
    # src_with_clause = build_ctes(src_ctes, src_dialect)
    # src_query_body = str(src_root_node)
    # tgt_with_clause = build_ctes(tgt_ctes, tgt_dialect)
    # tgt_query_body = str(tgt_root_node)
    fetch_no_point_sql = fetch_no_points_sqls(src_dialect, tgt_dialect)
    flag, res = src_execute_env.fetch_type(sql1[src_dialect].strip(';'))
    sql1_types = [type_mapping(src_dialect, t['type']) for t in res]
    retry_time = 8
    cur_try = 0
    ori_token = stat_token_query(sql1[src_dialect].strip(';'), src_dialect)
    while cur_try < retry_time:
        new_sql = sql1[src_dialect].strip(';')
        cur_token = ori_token
        random.shuffle(fetch_no_point_sql)
        cur_tables = sql1['tables']
        print(f"ROUND {cur_try} for {src_dialect}-{tgt_dialect} {sql1['points']}:")
        while True:
            flag = True
            for sql2 in fetch_no_point_sql:
                if cur_token - ori_token + sql2['tokens'] > to_add_tokens + 5:
                    continue
                if isinstance(sql1['points'][0], dict):
                    added_point_name = sql1['points'][0]['point']
                else:
                    added_point_name = sql1['points'][0]
                res = merge_query(
                    {src_dialect: new_sql, tgt_dialect: '', 'points':
                        [{"point": added_point_name, 'num': 1}]},
                    {src_dialect: sql2[src_dialect], tgt_dialect: '', 'points': []}, src_execute_env, True, True)
                if res is None:
                    flag = False
                    break
                new_tables = list(set(cur_tables + sql2['tables']))
                # src_tables = fetch_all_table_in_sql(new_sql, src_dialect)
                # if src_tables is None:
                #     continue
                now_token = stat_token_query(res[src_dialect], src_dialect)
                if now_token is None:
                    continue
                cur_token = now_token
                if cur_token > ori_token + to_add_tokens + 10:
                    flag = False
                    break
                new_sql = res[src_dialect]
                cur_tables = new_tables
                if to_add_tokens + 10 >= cur_token - ori_token >= to_add_tokens - 10:
                    flag_exe, res = src_execute_env.explain_execute_sql(new_sql)
                    parsed_points = [
                        parse_point(load_point_by_name(src_dialect, tgt_dialect, added_point_name))]
                    tgt_sql, all_rewrite_token, _ = rewrite_sql(src_dialect, tgt_dialect, new_sql,
                                                                parsed_points)
                    flag_tgt_exe, res = tgt_execute_env.explain_execute_sql(tgt_sql)
                    if flag_exe and flag_tgt_exe:
                        return {
                            src_dialect: new_sql,
                            tgt_dialect: tgt_sql,
                            "points": [{"point": added_point_name, 'num': 1}],
                            "rewrite_tokens": sql1['rewrite_tokens'],
                            "tables": list(cur_tables)
                        }
                    else:
                        flag = False
                        break
            if not flag:
                break
        cur_try += 1
