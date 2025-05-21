# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: point_loader.py$
# @Author: 10379
# @Time: 2025/4/30 15:25
import json
import os

from sql_gen.generator.point_parser import parse_point
from utils.tools import get_proj_root_path

categories = [
    "function_operator",
    "functional_keyword",
    "literal",
    "object_name"
]

dialects = ['mysql', 'pg', 'oracle']


def dialect_check_folder(folder_path, src_dialect, tgt_dialect):
    dialect_pair = f"{src_dialect}_{tgt_dialect}"
    flag = True
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isdir(file_path):
            dialect_check_folder(file_path, src_dialect, tgt_dialect)
        else:
            flag = False
            break
    if not flag:
        file_path = f"{folder_path}/{dialect_pair}_point.json"
        with open(file_path, "r", encoding="utf-8") as file:
            json_content = json.load(file)
        assert isinstance(json_content, list)
        for item in json_content:
            assert isinstance(item, dict)
            if not src_dialect in item or not tgt_dialect in item:
                print(folder_path)
                print(src_dialect)
                print(tgt_dialect)
                print(item)
                print('------')


def dialect_check(src_dialect, tgt_dialect):
    assert src_dialect in dialects
    assert tgt_dialect in dialects
    root_path = get_proj_root_path()
    for category_path in categories:
        folder_path = os.path.join(root_path, 'conv_point', category_path)
        dialect_check_folder(folder_path, src_dialect, tgt_dialect)


def load_translation_point_folder(folder_path, src_dialect, tgt_dialect):
    dialect_pair = f"{src_dialect}_{tgt_dialect}"
    flag = True
    translation_points = []
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isdir(file_path):
            translation_points = translation_points + load_translation_point_folder(file_path, src_dialect, tgt_dialect)
        else:
            flag = False
            break
    if not flag:
        file_path = f"{folder_path}/{dialect_pair}_point.json"
        with open(file_path, "r", encoding="utf-8") as file:
            points = json.load(file)
            for point in points:
                assert isinstance(point, dict)
                translation_points.append(point)
    return translation_points


def load_translation_point(src_dialect, tgt_dialect):
    point_path = os.path.join(get_proj_root_path(), 'conv_point')
    points = {}
    for category in categories:
        category_path = os.path.join(point_path, category)
        category_points = load_translation_point_folder(category_path, src_dialect, tgt_dialect)
        points[category] = category_points
    return points


def load_point_by_name(src_dialect, tgt_dialect, point_name):
    point_path = os.path.join(get_proj_root_path(), 'conv_point')
    for category in categories:
        category_path = os.path.join(point_path, category)
        category_points = load_translation_point_folder(category_path, src_dialect, tgt_dialect)
        for point in category_points:
            if point['Desc'] == point_name:
                return point
    raise ValueError(f"Point {point_name} not found")

# point_types = set()
# fields = set()
# return_fields = set()
# type_fields = set()
#
# for src_dialect in dialects:
#     for tgt_dialect in dialects:
#         if src_dialect == tgt_dialect:
#             continue
#         points = load_translation_point(src_dialect, tgt_dialect)
#         for category, values in points.items():
#             for point in values:
#                 return_fields.add(point['Return'])
#                 type_fields.add(point['Type'])
#
# print(return_fields)
# print(type_fields)
