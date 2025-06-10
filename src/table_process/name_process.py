# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: name_process$
# @Author: 10379
# @Time: 2025/5/29 23:11
import json
import os.path

from utils.tools import get_data_path


# This module is used to rename table and columns
# db name and column name have different constraint in different DBMS(e.g., 30 for oracle)

def rename_strategy(ori_name: str, length_limit: int):
    pieces = []
    for char in ori_name:
        pass


def rename_table(db_name, ori_table_name, new_table_name):
    with open(os.path.join(get_data_path(), db_name, 'schema.json'), 'r') as file:
        schema = json.load(file)
        assert ori_table_name in schema
        table_schema = schema[ori_table_name]
        table_schema['table'] = new_table_name
        assert isinstance(schema, dict)
        schema.pop(ori_table_name)
        schema[new_table_name] = table_schema
        # revise_foreign_key
        for table_name, table_schema in schema.items():
            for fk in table_schema['foreign_key']:
                if fk['ref_table'] == ori_table_name:
                    fk['ref_table'] = new_table_name

    with open(os.path.join(get_data_path(), db_name, 'schema_new.json'), 'w') as file:
        json.dump(schema, file, indent=4)
    if os.path.exists(os.path.join(get_data_path(), db_name, 'data', f'{ori_table_name}.json')):
        os.rename(os.path.join(get_data_path(), db_name, 'data', f'{ori_table_name}.json'),
                  os.path.join(get_data_path(), db_name, 'data', f'{new_table_name}.json'))


def rename_column(db_name, table_name, column_name, new_column_name):
    with open(os.path.join(get_data_path(), db_name, 'schema.json'), 'r') as file:
        schema = json.load(file)
        assert table_name in schema
        table_schema = schema[table_name]
        for col in table_schema['cols']:
            if col['col_name'] == column_name:
                col['col_name'] = new_column_name
                break
        else:
            raise ValueError(f'Column {column_name} not found in table {table_name}')
        i = 0
        while i < len(table_schema['primary_key']):
            if table_schema['primary_key'][i] == column_name:
                table_schema['primary_key'][i] = new_column_name
                break
            i += 1
        for fk in table_schema['foreign_key']:
            if isinstance(fk['col'], list):
                for i in range(len(fk['col'])):
                    if fk['col'][i] == column_name:
                        fk['col'][i] = new_column_name
                        break
            else:
                assert isinstance(fk['col'], str)
                if fk['col'] == column_name:
                    fk['col'] = new_column_name
                    break
        for fk_table_name, fk_table_schema in schema.items():
            for fk in fk_table_schema['foreign_key']:
                if fk['ref_table'] == table_name:
                    if isinstance(fk['ref_col'], list):
                        i = 0
                        while i < len(fk['ref_col']):
                            if fk['ref_col'][i] == column_name:
                                fk['ref_col'][i] = new_column_name
                                break
                            i += 1
                    else:
                        assert isinstance(fk['ref_col'], str)
                        if fk['ref_col'] == column_name:
                            fk['ref_col'] = new_column_name
                            break
        for index_cols in table_schema['index']:
            i = 0
            while i < len(index_cols):
                if index_cols[i] == column_name:
                    index_cols[i] = new_column_name
                    break
                i += 1
    with open(os.path.join(get_data_path(), db_name, 'schema_new.json'), 'w') as file:
        json.dump(schema, file, indent=4)
    if os.path.exists(os.path.join(get_data_path(), db_name, 'data', f'{table_name}.json')):
        with open(os.path.join(get_data_path(), db_name, 'data', f'{table_name}.json'), 'r') as file:
            data = json.load(file)
            first_row = data[0]
            i = 0
            print(first_row)
            while i < len(first_row):
                if first_row[i] == column_name:
                    first_row[i] = new_column_name
                    break
                i += 1
            print(first_row)
        with open(os.path.join(get_data_path(), db_name, 'data', f'{table_name}_new.json'), 'w') as file:
            json.dump(data, file, indent=4)


def rename_db(db_id):
    pass
