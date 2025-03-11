# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: schema_builder$
# @Author: 10379
# @Time: 2025/2/18 21:52
import json
import os.path

from db_builder.general_type import build_type
from utils.db_connector import oracle_sql_execute, mysql_sql_execute, pg_sql_execute


def mysql_create_table(schema):
    table_name = schema['table']
    cols = schema['cols']
    primary_keys = ''
    if "primary_key" in schema:
        primary_key = schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + '`' + key + '`'
    col_defs = ''
    constraints = []
    for col in cols:
        col_name = col['col_name']
        type, constraint = build_type(col['type'], col_name, 'mysql')
        if constraint is not None:
            constraints.append(constraint)
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = f"\t`{col_name}` {type} NOT NULL"
        else:
            type_def = f"\t`{col_name}` {type}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    create_stmt = f"CREATE TABLE `{table_name}` (\n{col_defs}"
    if primary_keys is not None:
        constraints.append(f"CONSTRAINT `PK_{table_name}` PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt


def mysql_add_foreign_key(schema):
    table = schema['FK_table']
    column = schema['FK_col']
    ref_table = schema['REF_table']
    ref_column = schema['REF_col']
    return (f"ALTER TABLE `{table}` ADD CONSTRAINT `FK_{table}{column}` "
            f"FOREIGN KEY (`{column}`) REFERENCES `{ref_table}` (`{ref_column}`) "
            f"ON DELETE NO ACTION ON UPDATE NO ACTION;")


def mysql_add_index(schema):
    table = schema['index_tbl']
    column = schema['index_col']
    return (f"CREATE INDEX idx_{table}_{column} ON `{table}` (`{column}`);")


def pg_create_table(schema: dict):
    table_name = schema['table']
    cols = schema['cols']
    primary_keys = ''
    if "primary_key" in schema:
        primary_key = schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + '"' + key + '"'
    constraints = []
    col_defs = ''
    for col in cols:
        col_name = col['col_name']
        type, constraint = build_type(col['type'], col_name, 'pg')
        if constraint is not None:
            constraints.append(constraint)
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = f"\t\"{col_name}\" {type} NOT NULL"
        else:
            type_def = f"\t\"{col_name}\" {type}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    create_stmt = f"CREATE TABLE \"{table_name}\" (\n{col_defs}"
    if primary_keys is not None:
        constraints.append(f"CONSTRAINT \"PK_{table_name}\" PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt


def pg_add_foreign_key(schema):
    table = schema['FK_table']
    column = schema['FK_col']
    ref_table = schema['REF_table']
    ref_column = schema['REF_col']
    return (f"ALTER TABLE \"{table}\"\nADD CONSTRAINT {table}_{column}_FKEY FOREIGN KEY (\"{column}\")\n\t"
            f"REFERENCES \"{ref_table}\" (\"{ref_column}\") ON DELETE NO ACTION ON UPDATE NO ACTION;")


def pg_add_index(schema):
    table = schema['index_tbl']
    column = schema['index_col']
    return (f"CREATE INDEX idx_{table}_{column} ON \"{table}\" (\"{column}\");")


def oracle_create_table(schema: dict):
    table_name = schema['table']
    cols = schema['cols']
    primary_keys = ''
    if "primary_key" in schema:
        primary_key = schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + '"' + key + '"'
    col_defs = ''
    constraints = []
    for col in cols:
        col_name = col['col_name']
        type, constraint = build_type(col['type'], col_name, 'oracle')
        constraint.append(constraint)
        if type == 'JSON' or type == 'Unsupported':
            continue
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = f"\t\"{col_name}\" {type} NOT NULL"
        else:
            type_def = f"\t\"{col_name}\" {type}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    create_stmt = f"CREATE TABLE \"{table_name}\" (\n{col_defs}"
    if primary_keys is not None:
        constraints.append(f"CONSTRAINT \"PK_{table_name}\" PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt


def oracle_add_foreign_key(schema):
    table = schema['FK_table']
    column = schema['FK_col']
    ref_table = schema['REF_table']
    ref_column = schema['REF_col']
    return (f"ALTER TABLE \"{table}\"\nADD CONSTRAINT {table}_{column}_FKEY FOREIGN KEY (\"{column}\")\n\t"
            f"REFERENCES \"{ref_table}\" (\"{ref_column}\");")


def oracle_add_index(schema):
    table = schema['index_tbl']
    column = schema['index_col']
    return (f"CREATE INDEX idx_{table}_{column} ON \"{table}\" (\"{column}\");")


def sql_writer(file, ddl_sqls: list, foreign_key_sqls: list = None, index_sqls: list = None):
    with open(file, 'w') as file:
        for sql in ddl_sqls:
            file.write(sql + '\n\n')
        if foreign_key_sqls is not None:
            for sql in foreign_key_sqls:
                file.write(sql + '\n\n')
        if index_sqls is not None:
            for sql in index_sqls:
                file.write(sql + '\n\n')


def build_schema(out_dir: str, schema_path: str):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    with open(schema_path, 'r') as file:
        schema = json.load(file)
    ddls = {}
    ddls['mysql'] = []
    ddls['pg'] = []
    ddls['oracle'] = []
    foreign_key = {}
    foreign_key['mysql'] = []
    foreign_key['pg'] = []
    foreign_key['oracle'] = []
    index = {}
    index['mysql'] = []
    index['pg'] = []
    index['oracle'] = []
    for ele in schema:
        if "table" in ele:
            ddls['mysql'].append(mysql_create_table(ele))
            ddls['pg'].append(pg_create_table(ele))
            ddls['oracle'].append(oracle_create_table(ele))
        elif 'FK_table' in ele:
            foreign_key['mysql'].append(mysql_add_foreign_key(ele))
            foreign_key['pg'].append(pg_add_foreign_key(ele))
            foreign_key['oracle'].append(oracle_add_foreign_key(ele))
        elif 'index_tbl' in ele:
            index['mysql'].append(mysql_add_index(ele))
            index['pg'].append(pg_add_index(ele))
            index['oracle'].append(oracle_add_index(ele))
        else:
            assert False

    for dialect in ['mysql', 'pg', 'oracle']:
        if not os.path.exists(os.path.join(out_dir, dialect)):
            os.makedirs(os.path.join(out_dir, dialect))
        sql_writer(os.path.join(out_dir, dialect, f'{dialect}_ddl.sql'), ddls[dialect])
        sql_writer(os.path.join(out_dir, dialect, f'{dialect}_fk.sql'), foreign_key[dialect])
        sql_writer(os.path.join(out_dir, dialect, f'{dialect}_idx.sql'), index[dialect])


def drop_schema(schema: dict, dialect, db_name):
    for ele in schema:
        if "table" in ele:
            table_name = ele['table']
            if dialect == 'oracle':
                flag, res = oracle_sql_execute(db_name, f"DROP TABLE {table_name} CASCADE CONSTRAINTS;")
            elif dialect == 'mysql':
                flag, res = mysql_sql_execute(db_name, f"DROP TABLE {table_name};")
            elif dialect == 'pg':
                flag, res = pg_sql_execute(db_name, f"DROP TABLE {table_name} CASCADE;")
            else:
                assert False
            if not flag:
                print(f'{table_name} may fail to drop')
