# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: try_rewrite_sql$
# @Author: 10379
# @Time: 2025/6/12 15:38
from sql_gen.generator.point_loader import load_translation_point, load_point_by_name
from sql_gen.generator.point_parser import parse_point


def try_rewrite_sql(sql, db_name, src_dialect, tgt_dialect):
    points = load_translation_point(src_dialect, tgt_dialect)
    cnt = 0
    for point_type, points in points.items():
        for point in points:
            # print(point)
            try:
                parse_point(point)
            except Exception as e:
                cnt = cnt + 1
                print(e)
                print(point)
                print('------')
    return cnt

# try_rewrite_sql('SELECT 1', 'bird', 'mysql', 'oracle')
dialects = ['mysql', 'oracle', 'pg']

all_cnt = 0
for src_dialect in dialects:
    for tgt_dialect in dialects:
        if src_dialect == tgt_dialect:
            continue
        print(src_dialect, tgt_dialect)
        all_cnt += try_rewrite_sql('SELECT 1', 'bird', src_dialect, tgt_dialect)
print(all_cnt)
