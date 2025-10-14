import json
import os.path

from utils.tools import get_db_ids, get_schema_path


def fetch_foreign_key_tables(table):
    for db_id in get_db_ids():
        with open(os.path.join(get_schema_path(db_id), 'schema.json'), 'r') as file:
            schema = json.load(file)
        if table not in schema:
            continue
        fks = schema[table]['foreign_key']
        fk_tables = [fk['ref_table'] for fk in fks]
        for key, value in schema.items():
            if key == table:
                continue
            for fk in value['foreign_key']:
                if fk['ref_table'] == table:
                    fk_tables.append(key)
        return list(set(fk_tables))
    return []


def minimal_set_selection(src_dialect, tgt_dialect, sqls: list[dict], gen_points):
    for sql in sqls:
        points = sql['fulfilling_points']
        new_points = []
        for p in points:
            if p in gen_points:
                new_points.append(p)
        sql['fulfilling_points'] = new_points
    added_points = set()
    relevant_tables = set()
    # greedy algorithm to select minimal set of sqls to cover all gen_points
    selected_sqls = []
    while len(added_points) < len(sqls):
        max_cover = 0
        best_sql = None
        relevant_flag = False
        for sql in sqls:
            cover = 0
            for p in sql['fulfilling_points']:
                if p not in added_points:
                    cover += 1
            if cover > max_cover and (not relevant_flag):
                max_cover = cover
                best_sql = sql
        if best_sql is None:
            print('Error to find best sql')
            break
        sqls.remove(best_sql)
        selected_sqls.append(best_sql)
        best_sql['selected'] = True
        for p in best_sql['fulfilling_points']:
            added_points.add(p)
        for tbl in best_sql['tables']:
            relevant_tables.add(tbl)
            for tbl in fetch_foreign_key_tables(tbl):
                relevant_tables.add(tbl)
    return selected_sqls
