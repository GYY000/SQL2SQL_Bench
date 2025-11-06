# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: list_all_point$
# @Author: 10379
# @Time: 2025/7/15 20:08
import json

from tqdm import tqdm

from sql_gen.generator.generate_pipeline import generate_equivalent_sql_pair
from sql_gen.generator.point_loader import load_translation_point


def gen_for_folder(folder_path):
    dialects = ['mysql', 'pg', 'oracle']
    for src_dialect in dialects:
        for tgt_dialect in dialects:
            if src_dialect == tgt_dialect:
                continue


def list_all_points():
    dialects = ['pg', 'oracle', 'mysql']
    for src_dialect in dialects:
        for tgt_dialect in dialects:
            if src_dialect == tgt_dialect:
                continue
            with open(f'/home/gyy/SQL2SQL_Bench/dataset/individual/{src_dialect}_{tgt_dialect}.json', 'r') as file:
                data = json.load(file)
            if len(data) == 0:
                points = load_translation_point(src_dialect, tgt_dialect)
                final_points = []
                for point_type, points_of_type in points.items():
                    for point in points_of_type:
                        final_points.append(point)
                with open(f'/home/gyy/SQL2SQL_Bench/dataset/individual/{src_dialect}_{tgt_dialect}.json', 'w') as file:
                    json.dump(final_points, file, indent=4)
            else:
                points = load_translation_point(src_dialect, tgt_dialect)
                all_points = []
                for category, points_of_type in points.items():
                    all_points = all_points + points_of_type
                for point in all_points:
                    not_in_flag = True
                    remake_flag = False
                    for point2 in data:
                        if point['Desc'] == point2['Desc']:
                            not_in_flag = False
                            if 'Return' not in point:
                                print(point)
                                exit()
                            if 'Return' not in point2:
                                point2['Return'] = None
                            if 'Condition' not in point2:
                                point2['Condition'] = None
                            if (point['Type'] != point2['Type']
                                    or point['Dialect']['Src'] != point2['Dialect']['Src'] or
                                    point['Dialect']['Tgt'] != point2['Dialect']['Tgt'] or
                                    point['SrcPattern'] != point2['SrcPattern'] or
                                    point['TgtPattern'] != point2['TgtPattern'] or
                                    point['Condition'] != point2['Condition'] or
                                    point['Return'] != point2['Return']):
                                remake_flag = True
                            else:
                                continue
                            if remake_flag:
                                print('remake point: ' + point['Desc'])
                                assert isinstance(point, dict)
                                if 'SQL' in point2:
                                    point2.pop('SQL')
                                point2['Type'] = point['Type']
                                point2['Dialect']['Src'] = point['Dialect']['Src']
                                point2['Dialect']['Tgt'] = point['Dialect']['Tgt']
                                point2['SrcPattern'] = point['SrcPattern']
                                point2['TgtPattern'] = point['TgtPattern']
                                point2['Condition'] = point['Condition']
                                point2['Return'] = point['Return']
                                if 'Tag' in point:
                                    point2['Tag'] = point['Tag']
                            break
                    if not_in_flag:
                        assert isinstance(data, list)
                        data.append(point)
                rm_points = []
                for point in data:
                    flag = False
                    for ori_point in all_points:
                        if point['Desc'] == ori_point['Desc']:
                            flag = True
                            break
                    if not flag:
                        rm_points.append(point)
                for point in rm_points:
                    data.remove(point)
                with open(f'/home/gyy/SQL2SQL_Bench/dataset/individual/{src_dialect}_{tgt_dialect}.json', 'w') as file:
                    json.dump(data, file, indent=4)
                continue


def gen_for_individual_points():
    dialects = ['oracle', 'mysql']
    cnt = 0
    for src_dialect in dialects:
        for tgt_dialect in dialects:
            if src_dialect == tgt_dialect:
                continue
            src_dialect = 'oracle'
            tgt_dialect = 'mysql'
            with open(f'/home/gyy/SQL2SQL_Bench/dataset/individual/{src_dialect}_{tgt_dialect}.json', 'r') as file:
                data = json.load(file)
            for point in tqdm(data):
                if 'SQL' in point:
                    cnt = cnt + 1
                    print(cnt)
                    continue
                print('****' + point['Desc'])
                try:
                    sql_pair = generate_equivalent_sql_pair(None, point['Desc'],
                                                            point['Dialect']['Src'],
                                                            point['Dialect']['Tgt'], {'min': 1, 'max': 1})
                    point['SQL'] = sql_pair
                    cnt = cnt + 1
                    with open(f'/home/gyy/SQL2SQL_Bench/dataset/individual/{src_dialect}_{tgt_dialect}.json',
                              'w') as file:
                        json.dump(data, file, indent=4)
                    print(cnt)
                except Exception as e:
                    print(e)
                    continue
            break
        break
    print(cnt)


list_all_points()
gen_for_individual_points()
