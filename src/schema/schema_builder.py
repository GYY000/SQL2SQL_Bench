# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: schema_builder$
# @Author: 10379
# @Time: 2025/2/18 21:52
import json
import os.path


def mysql_create_table(schema):
    table_name = schema['table']
    cols = schema['cols']
    primary_keys = ''
    if "primary_key" in schema:
        primary_key = schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + key
    col_defs = ''
    for col in cols:
        col_name = col['col_name']
        type = col['type']['mysql']
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = f"\t`{col_name}` {type} NOT NULL"
        else:
            type_def = f"\t`{col_name}` {type}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    if primary_keys is not None:
        return (f"CREATE TABLE `{table_name}` (\n{col_defs},\n\tCONSTRAINT `PK_{table_name}` "
                f"PRIMARY KEY (`{primary_keys}`)\n);")
    else:
        return f"CREATE TABLE `{table_name}` (\n{col_defs}\n);"


def mysql_add_foreign_key(schema):
    table = schema['FK_table']
    column = schema['FK_col']
    ref_table = schema['REF_table']
    ref_column = schema['REF_col']
    return (f"ALTER TABLE `{table}` ADD CONSTRAINT `FK_{table}{column}` "
            f"FOREIGN KEY (`{column}`) REFERENCES `{ref_table}` (`{ref_column}`) "
            f"ON DELETE NO ACTION ON UPDATE NO ACTION;")


def pg_create_table(schema: dict):
    table_name = schema['table']
    cols = schema['cols']
    primary_keys = ''
    if "primary_key" in schema:
        primary_key = schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + key
    col_defs = ''
    for col in cols:
        col_name = col['col_name']
        type = col['type']['pg']
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = f"\t\"{col_name}\" {type} NOT NULL"
        else:
            type_def = f"\t\"{col_name}\" {type}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    if primary_keys != '':
        return (f"CREATE TABLE \"{table_name}\" (\n{col_defs},\n\tCONSTRAINT PK_{table_name} "
                f"PRIMARY KEY (\"{primary_keys}\")\n);")
    else:
        return f"CREATE TABLE \"{table_name}\" (\n{col_defs}\n);"


def pg_add_foreign_key(schema):
    table = schema['FK_table']
    column = schema['FK_col']
    ref_table = schema['REF_table']
    ref_column = schema['REF_col']
    return (f"ALTER TABLE \"{table}\"\nADD CONSTRAINT {table}_{column}_FKEY FOREIGN KEY (\"{column}\")\n\t"
            f"REFERENCES \"{ref_table}\" (\"{ref_column}\") ON DELETE NO ACTION ON UPDATE NO ACTION;")


def oracle_create_table(schema: dict):
    table_name = schema['table']
    cols = schema['cols']
    primary_keys = ''
    if "primary_key" in schema:
        primary_key = schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + key
    col_defs = ''
    for col in cols:
        col_name = col['col_name']
        type = col['type']['oracle']
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = f"\t\"{col_name}\" {type} NOT NULL"
        else:
            type_def = f"\t\"{col_name}\" {type}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    if primary_keys != '':
        return (f"CREATE TABLE \"{table_name}\" (\n{col_defs},\n\tCONSTRAINT \"PK_{table_name}\" "
                f"PRIMARY KEY (\"{primary_keys}\")\n);")
    else:
        return f"CREATE TABLE \"{table_name}\" (\n{col_defs}\n);"


def oracle_add_foreign_key(schema):
    table = schema['FK_table']
    column = schema['FK_col']
    ref_table = schema['REF_table']
    ref_column = schema['REF_col']
    return (f"ALTER TABLE \"{table}\"\nADD CONSTRAINT {table}_{column}_FKEY FOREIGN KEY (\"{column}\")\n\t"
            f"REFERENCES \"{ref_table}\" (\"{ref_column}\");")


def sql_writer(file, sqls: list):
    with open(file, 'w') as file:
        for sql in sqls:
            file.write(sql + '\n\n')


def build_schema(out_dir: str, schema_path: str):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    with open(schema_path, 'r') as file:
        schema = json.load(file)
    mysql_ddls = []
    pg_ddls = []
    oracle_ddls = []
    for ele in schema:
        if "table" in ele:
            mysql_ddls.append(mysql_create_table(ele))
            pg_ddls.append(pg_create_table(ele))
            oracle_ddls.append(oracle_create_table(ele))
        elif 'FK_table' in ele:
            mysql_ddls.append(mysql_add_foreign_key(ele))
            pg_ddls.append(pg_add_foreign_key(ele))
            oracle_ddls.append(oracle_add_foreign_key(ele))
        else:
            assert False
    sql_writer(os.path.join(out_dir, 'mysql_ddl.sql'), mysql_ddls)
    sql_writer(os.path.join(out_dir, 'pg_ddl.sql'), pg_ddls)
    sql_writer(os.path.join(out_dir, 'oracle_ddl.sql'), oracle_ddls)



