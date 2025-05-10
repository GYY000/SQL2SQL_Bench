# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: point_statistic$
# @Author: 10379
# @Time: 2025/4/13 9:41
import json
import os

from utils.tools import get_proj_root_path

category_paths = [
    "function_operator",
    "functional_keyword",
    "literal",
    "object_name"
]

dialect_pairs = {
    "mysql_oracle",
    "mysql_pg",
    "pg_mysql",
    "pg_oracle",
    "oracle_mysql",
    "oracle_pg"
}

statistics = {}


def statistic_folder(folder_path, dialect_pair, folder_name, hierarchy_level=1):
    all_num = 0
    all_stats = []
    flag = True
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isdir(file_path):
            stats = statistic_folder(file_path, dialect_pair, file, hierarchy_level + 1)
            all_num += stats["number"]
            all_stats.append(stats)
        else:
            flag = False
            break
    if not flag:
        file_path = f"{folder_path}/{dialect_pair}_point.json"
        with open(file_path, "r", encoding="utf-8") as file:
            json_content = json.load(file)
            all_num = len(json_content)
        return {
            "name": folder_name,
            "number": all_num,
            "level": hierarchy_level
        }
    return {
        "name": folder_name,
        "number": all_num,
        "stats": all_stats,
        "level": hierarchy_level,
    }


def print_summary(stats):
    indent = "\t" * stats["level"]
    if 'stats' in stats:
        print(f"{indent}{stats['name']} ({stats['number']}):")
        for sub_stats in stats["stats"]:
            print_summary(sub_stats)
    else:
        print(f"{indent}{stats['name']} ({stats['number']})")


def statistic():
    root_path = get_proj_root_path()
    all_count = 0
    all_statistics = []
    for dialect_pair in dialect_pairs:
        dialect_pair_count = 0
        dialect_stats = {
            "name": dialect_pair,
            "stats": [],
            "level": 0
        }
        for category_path in category_paths:
            folder_path = os.path.join(root_path, 'conv_point', category_path)
            stats = statistic_folder(folder_path, dialect_pair, category_path)
            dialect_pair_count += stats["number"]
            dialect_stats["stats"].append(stats)
        dialect_stats['number'] = dialect_pair_count
        all_count += dialect_pair_count
        print_summary(dialect_stats)
    print('ALL count: ', all_count)


statistic()
