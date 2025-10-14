import json
import os

from sql_gen.generator.add_pattern_point import fetch_fulfilling_sql
from sql_gen.generator.generate_pipeline import generate_equivalent_sql_pair
from sql_gen.generator.point_loader import load_translation_point
from utils.tools import get_all_db_name, get_db_ids, get_proj_root_path


def mark_sql_generable_points():
    dialects = ['mysql', 'pg', 'oracle']
    for src_dialect in dialects:
        for tgt_dialect in dialects:
            if src_dialect == tgt_dialect:
                continue
            all_points = load_translation_point(src_dialect, tgt_dialect)
            all_points_list = []
            for key, value in all_points.items():
                all_points_list = all_points_list + (value)
            for db_id in get_db_ids():
                sql_root_path = os.path.join(get_proj_root_path(), 'SQL', db_id)
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
                for sql in sqls:
                    sql_generable_points = []
                    for p in all_points_list:
                        value = generate_equivalent_sql_pair(src_dialect, tgt_dialect, [], p['Desc'], 5, sql, True)
                        if value is None:
                            continue
                        else:
                            sql_generable_points.append(p['Desc'])
                    print(sql_generable_points)
                    break


mark_sql_generable_points()
