# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: geenrate_pipeline$
# @Author: 10379
# @Time: 2025/5/12 14:49
import json
import os.path
import random
import traceback

from antlr_parser.general_tree_analysis import fetch_all_table_in_sql
from sql_gen.generator.add_point import generate_sql_with_point
from sql_gen.generator.element.Point import Point
from sql_gen.generator.point_loader import load_points_by_req, load_point_by_name
from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.point_type.TranPointType import OrderByClauseType, ReservedKeywordType
from sql_gen.generator.rewriter import rewrite_sql
from utils.CISpacelessSet import CISpacelessSet
from utils.ExecutionEnv import ExecutionEnv
from utils.db_connector import sql_execute
from utils.tools import get_all_db_name


def fetch_db_param_by_point(points: list[dict], src_dialect, tgt_dialect):
    db_param = {
        src_dialect: {},
        tgt_dialect: {}
    }
    for point in points:
        point = load_point_by_name(src_dialect, tgt_dialect, point['point'])
        if "Tag" in point:
            param = point['Tag']
            if 'DB PARAMETER' in param:
                for key, value in param['DB PARAMETER'].items():
                    db_param[key].update(value)
    return db_param


def generate_sql_by_points(points: list[dict], aggressive_flag: bool, execution_env: ExecutionEnv,
                           cur_sql: dict | None = None, only_cur_sql_mode=False):
    already_build_items_map = CISpacelessSet()
    while True:
        to_add_point = fetch_point_to_add(points)
        if to_add_point is None:
            break
        cnt = 0
        ori_cur_sql = None
        if cur_sql is not None:
            ori_cur_sql = {}
            for key, value in cur_sql.items():
                ori_cur_sql[key] = value
        sql_dict = None
        while cnt < 5 and sql_dict is None:
            try:
                temp_set = already_build_items_map.clone()
                sql_dict = generate_sql_with_point(to_add_point, cur_sql, points,
                                                   aggressive_flag, execution_env, temp_set, only_cur_sql_mode)
                if sql_dict is not None:
                    flag, _ = execution_env.explain_execute_sql(sql_dict[execution_env.dialect])
                    if not flag:
                        sql_dict = None
                    else:
                        already_build_items_map = temp_set
            except Exception as e:
                print(e)
                traceback.print_exc()
                cur_sql = ori_cur_sql
                cnt += 1
        if cnt >= 5:
            return None
        cur_sql = sql_dict
        assert sql_dict is not None
    return cur_sql


def generate_equivalent_sql_pair(src_dialect: str, tgt_dialect: str, point_requirements: list[dict],
                                 point: str | None = None, max_retry_time=5,
                                 cur_sql: dict | None = None, only_cur_sql_mode=False) -> dict | None:
    """
    Generates an equivalent SQL pair based on the given point and configuration parameters.
    Parameters:
    :param point: str give the point_id that used to generate.
    :param src_dialect: str - The source dialect of the SQL pair.
    :param tgt_dialect: str - The target dialect of the SQL pair.
    :param max_retry_time: int - The maximum number retrying time
    Returns:
    :return: dict - A dict containing two equivalent SQL expressions that are logically equivalent and the environment.
            Example: {'mysql': '...', 'oracle': '...', 'points': {...}}
    """
    if isinstance(point, str):
        point_requirements = [{
            "point": point,
            "num": 1
        }]
    point_requirements = load_points_by_req(point_requirements, src_dialect, tgt_dialect)
    i = 0
    while True:
        i += 1
        if i > max_retry_time:
            print(f'No SQL pair can be generated with point requirement {point_requirements}')
            return None
        print(f'round {i}')
        point_req_list = []
        for point_req in point_requirements:
            point_req_list.append({
                "point": parse_point(point_req['point']),
                "num": point_req['num']
            })
        if i < 0.6 * max_retry_time:
            aggressive_flag = True
        else:
            aggressive_flag = False
        try:
            execution_env = ExecutionEnv(src_dialect, get_all_db_name(src_dialect))
            for point_req in point_req_list:
                point = point_req['point']
                assert isinstance(point, Point)
                if point.tag is not None and 'DB PARAMETER' in point.tag:
                    for key, value in point.tag['DB PARAMETER'].items():
                        flag = execution_env.add_param(key, value)
                        if not flag:
                            raise ValueError('DB Parameter conflict')
            for point in cur_sql['points'] if cur_sql is not None else []:
                point = load_point_by_name(src_dialect, tgt_dialect, point['point'])
                if point is None:
                    continue
                if "Tag" in point:
                    param = point['Tag']
                    if 'DB PARAMETER' in param:
                        for key, value in param['DB PARAMETER'].items():
                            flag = execution_env.add_param(key, value)
                            if not flag:
                                raise ValueError('DB Parameter conflict')
            sql_pair = generate_sql_by_points(point_req_list, aggressive_flag, execution_env, cur_sql,
                                              only_cur_sql_mode)
            if sql_pair is None:
                continue
            parsed_points = []
            for point in sql_pair['points']:
                parsed_point = parse_point(load_point_by_name(src_dialect, tgt_dialect, point['point']))
                if isinstance(parsed_point.point_type, ReservedKeywordType):
                    continue
                parsed_points.append(parsed_point)
            print(f'generated {src_dialect} SQL: ' + sql_pair[src_dialect])
            tgt_sql, all_rewrite_token, rewrite_points = rewrite_sql(src_dialect, tgt_dialect, sql_pair[src_dialect],
                                                                     parsed_points)

            if tgt_sql is None:
                print(f"\033[91m{tgt_sql}\033[0m")
                continue
            sql_pair[tgt_dialect] = tgt_sql
        except Exception as e:
            print(e)
            traceback.print_exc()
            continue
            # raise e
        db_param = fetch_db_param_by_point(sql_pair['points'], src_dialect, tgt_dialect)
        flag1, res1 = sql_execute(src_dialect, get_all_db_name(src_dialect), sql_pair[src_dialect],
                                  db_param[src_dialect], False,
                                  True)
        print(flag1)
        if len(res1) == 0:
            continue
        flag2, res2 = sql_execute(tgt_dialect, get_all_db_name(tgt_dialect), sql_pair[tgt_dialect],
                                  db_param[tgt_dialect], False,
                                  True)
        print(sql_pair[tgt_dialect])
        print(flag2)
        sql_pair['rewrite_tokens'] = all_rewrite_token
        src_tables = fetch_all_table_in_sql(sql_pair[src_dialect], src_dialect)
        tgt_tables = fetch_all_table_in_sql(sql_pair[tgt_dialect], tgt_dialect)
        if src_tables is None and tgt_tables is None:
            continue
        elif src_tables is None:
            sql_pair['tables'] = list(tgt_tables)
        elif tgt_tables is None:
            sql_pair['tables'] = list(src_tables)
        else:
            assert src_tables == tgt_tables
            sql_pair['tables'] = list(src_tables)
        if flag1 and flag2:
            break
        if sql_pair is None:
            raise ValueError('No SQL pair can be generated')
    return sql_pair


def fetch_point_to_add(points: list[dict]):
    if len(points) == 0:
        return None
    to_add_point = random.choice(points)
    while len(points) > 1 and isinstance(to_add_point['point'].point_type, OrderByClauseType):
        to_add_point = random.choice(points)
    if to_add_point['num'] == 1:
        points.remove(to_add_point)
    else:
        to_add_point['num'] -= 1
    return to_add_point['point']
