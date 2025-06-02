# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: geenrate_pipeline$
# @Author: 10379
# @Time: 2025/5/12 14:49
import json
import os.path
import random

from sql_gen.generator.add_point import generate_sql_with_point
from sql_gen.generator.element.Point import Point
from sql_gen.generator.point_loader import load_points_by_req, load_point_by_name, load_db_param_point
from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.rewriter import rewrite_sql
from utils.db_connector import sql_execute
from utils.tools import get_db_ids, get_proj_root_path


def fetch_db_param_by_point(points: list[str], src_dialect, tgt_dialect):
    db_param = {}
    for point_name in points:
        point = load_point_by_name(src_dialect, tgt_dialect, point_name)
        if "Tag" in point:
            param = point['Tag']
            if 'DB PARAMETER' in param:
                for key, value in param['DB PARAMETER'].items():
                    if key in db_param:
                        assert db_param[key] == value
                    db_param[key] = value
    return db_param


def fetch_fulfilled_sqls(points: list[dict], src_dialect, tgt_dialect, db_id: str = None):
    if db_id is not None:
        db_ids = [db_id]
    else:
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
                flag = True
                for point in sql['points']:
                    if point not in points:
                        flag = False
                        break
                if flag:
                    all_sqls.append(sql)
        path = os.path.join(sql_root_path, 'points', f'{src_dialect}_{tgt_dialect}.json')
        if os.path.exists(path):
            with open(path, 'r') as file:
                sqls = json.load(file)
            for sql in sqls:
                flag = True
                for point in sql['points']:
                    if point not in points:
                        flag = False
                        break
                if flag:
                    all_sqls.append(sql)
    return all_sqls


def generate_sql_by_points(points: list[dict], structural_complexity_req, num_of_points_req, src_dialect, tgt_dialect,
                           db_name: str | None):
    fulfilled_sqls = fetch_fulfilled_sqls(points, src_dialect, tgt_dialect, db_name)
    parsed_points = [parse_point(point) for point in points]
    for point_number in range(num_of_points_req['min'], num_of_points_req['max'] + 1):
        for structural_complexity in range(structural_complexity_req['min_complexity'],
                                           structural_complexity_req['max_complexity'] + 1):
            point = random.choice(parsed_points)
            sql_pair = generate_sql_with_point(point, structural_complexity, point_number, src_dialect, tgt_dialect,
                                               fulfilled_sqls, parsed_points)
            if sql_pair is None:
                continue
            else:
                return sql_pair
    return None


def generate_equivalent_sql_pair(point_requirement: dict | None, point: str | None, structural_complexity_req: dict,
                                 src_dialect: str, tgt_dialect: str, num_of_points_req: dict,
                                 db_name: str | None = None) -> dict:
    # TODO: add switcher that can control whether use existing SQL is allowed
    """
    Generates an equivalent SQL pair based on the given point and configuration parameters.
    Parameters:
    :param point_requirement: dict[str, str] - give the type of translation point to be added, four fields can be added.
            Example: {'type_resolution': true, 'type_mapping': false, 'db parameter': true, 'min_ops': 1, 'max_ops': 3}
    :param point: str give the point_id that used to transfer the sql.
    :param structural_complexity_req: give the minimum and maximum structural complexity of the generated SQL pair.
            Example: {'min_complexity': 1, 'max_complexity': 3}
    :param src_dialect: str - The number of points to be considered when generating the SQL pair.
    :param tgt_dialect: str - The number of points to be considered when generating the SQL pair.
    :param num_of_points_req: dict - The minimum and maximum number of points to included in the generated SQL pair.
            Example: {'min': 1, 'max': 3}
    :param db_name: str - The name of the database to be used for generating the SQL pair.

    Returns:
    :return: dict - A dict containing two equivalent SQL expressions that are logically equivalent and the environment.
            Example: {'mysql': '...', 'oracle': '...', 'db para': {...}, 'db_id'}
    """
    if point is not None:
        points = [load_point_by_name(src_dialect, tgt_dialect, point)]
    else:
        points = load_points_by_req(point_requirement, src_dialect, tgt_dialect)
    i = 0
    while True:
        i += 1
        print(f'round {i}')
        sql_pair = generate_sql_by_points(points, structural_complexity_req, num_of_points_req, src_dialect,
                                          tgt_dialect,
                                          db_name)
        parsed_points = []
        for point in sql_pair['points']:
            parsed_point = parse_point(load_point_by_name(src_dialect, tgt_dialect, point))
            parsed_points.append(parsed_point)
        tgt_sql = rewrite_sql(src_dialect, tgt_dialect, sql_pair[src_dialect], parsed_points, sql_pair['db_id'])
        sql_pair[tgt_dialect] = tgt_sql

        db_param = fetch_db_param_by_point(sql_pair['points'], src_dialect, tgt_dialect)
        flag1, _ = sql_execute(src_dialect, sql_pair['db_id'], sql_pair[src_dialect], db_param)
        print(sql_pair[src_dialect])
        print(flag1)
        flag2, _ = sql_execute(tgt_dialect, sql_pair['db_id'], sql_pair[tgt_dialect])
        print(sql_pair[tgt_dialect])
        print(flag2)
        if flag1 and flag2:
            break

        if sql_pair is None:
            raise ValueError('No SQL pair can be generated')
        if i > 15:
            print(points)
            raise ValueError('No SQL pair can be generated')
    return sql_pair


points = load_db_param_point()
sql_pairs = []
complexity = 2
for point in points:
    sql_pair = generate_equivalent_sql_pair(None, point['Desc'],
                                            {'min_complexity': complexity, 'max_complexity': complexity},
                                            point['Dialect']['Src'],
                                            point['Dialect']['Tgt'], {'min': 1, 'max': 3})
    sql_pairs.append(sql_pair)
with open(f'/home/gyy/SQL2SQL_Bench/SQL/param{complexity}.json', 'w') as file:
    json.dump(sql_pairs, file, indent=4, ensure_ascii=False)

complexity = 3
for point in points:
    sql_pair = generate_equivalent_sql_pair(None, point['Desc'],
                                            {'min_complexity': complexity, 'max_complexity': complexity},
                                            point['Dialect']['Src'],
                                            point['Dialect']['Tgt'], {'min': 1, 'max': 3})
    sql_pairs.append(sql_pair)
with open(f'/home/gyy/SQL2SQL_Bench/SQL/param{complexity}.json', 'w') as file:
    json.dump(sql_pairs, file, indent=4, ensure_ascii=False)
