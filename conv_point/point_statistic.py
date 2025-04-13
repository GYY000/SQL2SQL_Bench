# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: point_statistic$
# @Author: 10379
# @Time: 2025/4/13 9:41
import json
import os

from utils.tools import get_proj_root_path

file_map_path = [
    {
        "category": "data_type",
        "sub_categories": [
            "explicit_conv",
            "implicit_conv"
        ]
    },
    {
        "category": "function_operator",
        "sub_categories": [
            "aggregate_function",
            "operator",
            "value_function"
        ]
    },
    "functional_keyword",
    "literal"
]

dialect_pairs = {
    "mysql_oracle",
    "mysql_pg",
    "pg_mysql",
    "pg_oracle",
    "oracle_mysql",
    "oracle_pg"
}


def statistic():
    root_path = get_proj_root_path()
    all_count = 0
    for dialect_pair in dialect_pairs:
        dialect_pair_count = 0
        for ele in file_map_path:
            category_count = 0
            if isinstance(ele, dict):
                category = ele["category"]
                sub_categories = ele["sub_categories"]
                for sub_category in sub_categories:
                    sub_category_file_path = f"{root_path}/conv_point/{category}/{sub_category}"
                    file_path = f"{sub_category_file_path}/{dialect_pair}.json"
                    if os.path.exists(file_path):
                        with open(file_path, "r", encoding="utf-8") as file:
                            sub_category_count = len(json.load(file))
                    else:
                        sub_category_count = 0
                    category_count += sub_category_count
                    print(f"\t\t{sub_category} count: ", sub_category_count)
            else:
                category = ele
                file_path = f"{root_path}/conv_point/{category}/{dialect_pair}.json"
                if os.path.exists(file_path):
                    with open(file_path, "r", encoding="utf-8") as file:
                        json_content = json.load(file)
                        category_count = len(json_content)
            dialect_pair_count += category_count
            print(f"\t{category} count: ", category_count)
        all_count += dialect_pair_count
        print(f"{dialect_pair} count: ", dialect_pair_count)
    print('ALL count: ', all_count)

statistic()
