# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: schema_builder$
# @Author: 10379
# @Time: 2025/2/18 21:52
import json
import os.path

from tqdm import tqdm

from db_builder.general_type import build_type, build_value
from utils.db_connector import oracle_sql_execute, mysql_sql_execute, pg_sql_execute
from utils.tools import get_proj_root_path, str_split

id_number = {}


def create_table(schema, dialect):
    if dialect == 'pg':
        return pg_create_table(schema)
    elif dialect == 'mysql':
        return mysql_create_table(schema)
    elif dialect == 'oracle':
        return oracle_create_table(schema)
    else:
        assert False


def add_foreign_key(schema, dialect):
    if dialect == 'pg':
        return pg_add_foreign_key(schema)
    elif dialect == 'mysql':
        return mysql_add_foreign_key(schema)
    elif dialect == 'oracle':
        return oracle_add_foreign_key(schema)
    else:
        assert False


def add_index(schema, dialect):
    if dialect == 'pg':
        return pg_add_index(schema)
    elif dialect == 'mysql':
        return mysql_add_index(schema)
    elif dialect == 'oracle':
        return oracle_add_index(schema)
    else:
        assert False


def rename_constraints(name: str):
    splits = name.split('_')
    res = ''
    if len(splits) == 1:
        res = name[:3]
    else:
        for split in splits:
            if res != '':
                res = res + "_"
            res = res + split[:3].upper()
    res.strip('_')
    if res in id_number:
        id_number[res] = id_number[res] + 1
        return res + '_' + str(id_number[res])
    else:
        id_number[res] = 0
        return res + '_' + str(id_number[res])


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
    type_def_stmts = []
    col_defs = ''
    constraints = []
    for col in cols:
        col_name = col['col_name']
        type, constraint, type_defs = build_type(col['type'], col_name, 'mysql')
        if type is None:
            continue
        type_def_stmts = type_def_stmts + type_defs
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
    if primary_keys != '':
        constraints.append(f"CONSTRAINT `{rename_constraints(f"PK_{table_name}")}` PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt, type_def_stmts


def mysql_add_foreign_key(schema):
    table = schema['table']
    res = []
    if 'foreign_key' not in schema:
        return res
    for fk in schema['foreign_key']:
        column = fk['col']
        ref_table = fk['ref_table']
        ref_column = fk['ref_col']
        res.append((f"ALTER TABLE `{table}` ADD CONSTRAINT `{rename_constraints(f"FK_{table}")}` "
                    f"FOREIGN KEY (`{column}`) REFERENCES `{ref_table}` (`{ref_column}`) "
                    f"ON DELETE NO ACTION ON UPDATE NO ACTION;"))
    return res


def mysql_add_index(schema):
    table = schema['table']
    res = []
    if 'index' not in schema:
        return res
    for index_cols in schema['index']:
        str_columns = ''
        for col in index_cols:
            if str_columns != '':
                str_columns = str_columns + ', '
            str_columns = str_columns + '`' + col + '`'
        res.append(f"CREATE INDEX {rename_constraints(f"IDX_{table}")} ON `{table}` ({str_columns});")
    return res


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
    type_def_stmts = []
    for col in cols:
        col_name = col['col_name']
        type, constraint, type_defs = build_type(col['type'], col_name, 'pg')
        if type is None:
            continue
        type_def_stmts = type_def_stmts + type_defs
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
    if primary_keys != '':
        constraints.append(f"CONSTRAINT \"{rename_constraints(f"PK_{table_name}")}\" PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt, type_def_stmts


def pg_add_foreign_key(schema):
    table = schema['table']
    res = []
    if 'foreign_key' not in schema:
        return res
    for fk in schema['foreign_key']:
        column = fk['col']
        ref_table = fk['ref_table']
        ref_column = fk['ref_col']
        res.append((
            f"ALTER TABLE \"{table}\"\nADD CONSTRAINT {rename_constraints(f"FK_{table}")} FOREIGN KEY (\"{column}\")\n\t"
            f"REFERENCES \"{ref_table}\" (\"{ref_column}\") ON DELETE NO ACTION ON UPDATE NO ACTION;"))
    return res


def pg_add_index(schema):
    table = schema['table']
    res = []
    if 'index' not in schema:
        return res
    for index_cols in schema['index']:
        str_columns = ''
        for col in index_cols:
            if str_columns != '':
                str_columns = str_columns + ', '
            str_columns = str_columns + '"' + col + '"'
        res.append(f"CREATE INDEX {rename_constraints(f"IDX_{table}")} ON \"{table}\" ({str_columns});")
    return res


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
    create_type_stmts = []
    for col in cols:
        col_name = col['col_name']
        type, constraint, type_defs = build_type(col['type'], col_name, 'oracle')
        if type is None:
            continue
        create_type_stmts = create_type_stmts + type_defs
        if constraint is not None:
            constraints.append(constraint)
        if type is None:
            continue
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = f"\t\"{col_name}\" {type} NOT NULL"
        else:
            type_def = f"\t\"{col_name}\" {type}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    create_stmt = f"CREATE TABLE \"{table_name}\" (\n{col_defs}"
    if primary_keys != '':
        constraints.append(f"CONSTRAINT \"{rename_constraints(f"PK_{table_name}")}\" PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt, create_type_stmts


def oracle_add_foreign_key(schema):
    table = schema['table']
    res = []
    if 'foreign_key' not in schema:
        return res
    for fk in schema['foreign_key']:
        column = fk['col']
        ref_table = fk['ref_table']
        ref_column = fk['ref_col']
        res.append((
            f"ALTER TABLE \"{table}\"\nADD CONSTRAINT {rename_constraints(f"FK_{table}")} FOREIGN KEY (\"{column}\")\n\t"
            f"REFERENCES \"{ref_table}\" (\"{ref_column}\");"))
    return res


def oracle_add_index(schema):
    table = schema['table']
    res = []
    if 'index' not in schema:
        return res
    for index_cols in schema['index']:
        str_columns = ''
        for col in index_cols:
            if str_columns != '':
                str_columns = str_columns + ', '
            str_columns = str_columns + '"' + col + '"'
        res.append(f"CREATE INDEX {rename_constraints(f"IDX_{table}")} ON \"{table}\" ({str_columns});")
    return res


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


def build_schema(db_name: str):
    db_path = os.path.join(get_proj_root_path(), 'data', db_name)
    ddl_dir = os.path.join(db_path, 'ddl')
    if not os.path.exists(ddl_dir):
        os.makedirs(ddl_dir)
    schema_path = os.path.join(db_path, 'schema.json')
    with open(schema_path, 'r') as file:
        schema = json.load(file)
    ddls = {'mysql': [], 'pg': [], 'oracle': []}
    foreign_key = {'mysql': [], 'pg': [], 'oracle': []}
    index = {'mysql': [], 'pg': [], 'oracle': []}
    type_defs = {'mysql': [], 'pg': [], 'oracle': []}
    for key, value in schema.items():
        for dialect in ['pg', 'mysql', 'oracle']:
            create_stmt, type_def_stmts = create_table(value, dialect)
            ddls[dialect].append(create_stmt)
            type_defs[dialect] = type_defs[dialect] + type_def_stmts
            foreign_key[dialect] = foreign_key[dialect] + add_foreign_key(value, dialect)
            index[dialect] = index[dialect] + add_index(value, dialect)

    for dialect in ['mysql', 'pg', 'oracle']:
        if not os.path.exists(os.path.join(ddl_dir, dialect)):
            os.makedirs(os.path.join(ddl_dir, dialect))
        sql_writer(os.path.join(ddl_dir, dialect, f'{dialect}_ddl.sql'), type_defs[dialect] + ddls[dialect])
        sql_writer(os.path.join(ddl_dir, dialect, f'{dialect}_fk.sql'), foreign_key[dialect])
        sql_writer(os.path.join(ddl_dir, dialect, f'{dialect}_idx.sql'), index[dialect])


def drop_schema(db_name, dialect):
    with open(os.path.join(get_proj_root_path(), 'data', db_name, 'schema.json'), 'r') as file:
        schema = json.loads(file.read())
    for table_name, value in schema.items():
        if dialect == 'oracle':
            oracle_sql_execute(db_name, f"DROP TABLE \"{table_name}\" CASCADE CONSTRAINTS;")
        elif dialect == 'mysql':
            mysql_sql_execute(db_name, f"DROP TABLE `{table_name}`;")
        elif dialect == 'pg':
            pg_sql_execute(db_name, f"DROP TABLE \"{table_name}\" CASCADE;")
        else:
            assert False
    flag1 = True
    for ele in schema:
        if "table" in ele:
            table_name = ele['table']
            if dialect == 'oracle':
                flag, res = oracle_sql_execute(db_name, f"SELECT * FROM \"{table_name}\";")
            elif dialect == 'mysql':
                flag, res = mysql_sql_execute(db_name, f"SELECT * FROM `{table_name}`;")
            elif dialect == 'pg':
                flag, res = pg_sql_execute(db_name, f"SELECT * FROM \"{table_name}\";")
            else:
                assert False
            flag1 = flag1 and not flag
            if flag:
                print(f'{table_name} may fail to drop')
    if flag1:
        print(f'{db_name} drop successfully')


def build_db(db_name, dialect):
    with open(os.path.join(get_proj_root_path(), 'data', db_name, 'schema.json'), 'r') as file:
        schema = json.loads(file.read())
    build_schema(db_name)
    with open(os.path.join(get_proj_root_path(), 'data', db_name, 'ddl', dialect, f'{dialect}_ddl.sql'), 'r') as file:
        sqls = file.read().split(';')
    for sql in sqls:
        if sql.strip() == '':
            continue
        else:
            if dialect == 'oracle':
                flag, res = oracle_sql_execute(db_name, sql)
            elif dialect == 'mysql':
                flag, res = mysql_sql_execute(db_name, sql)
            elif dialect == 'pg':
                flag, res = pg_sql_execute(db_name, sql)
            else:
                assert False
            if not flag:
                print(f'{sql} may fail to execute')
                print(res)
    flag1 = True
    for ele in schema:
        if "table" in ele:
            table_name = ele['table']
            if dialect == 'oracle':
                flag, res = oracle_sql_execute(db_name, f"SELECT * FROM \"{table_name}\";")
            elif dialect == 'mysql':
                flag, res = mysql_sql_execute(db_name, f"SELECT * FROM `{table_name}`;")
            elif dialect == 'pg':
                flag, res = pg_sql_execute(db_name, f"SELECT * FROM \"{table_name}\";")
            else:
                assert False
            flag1 = flag1 and flag
            if not flag:
                print(f'{table_name} may fail to create')
                print(res)
    if flag1:
        print(f'{db_name} create successfully')
    else:
        print(f'{db_name} create failed')

    if os.path.exists(os.path.join(get_proj_root_path(), 'data', db_name, 'data', f'{dialect}_data.sql')):
        with open(os.path.join(get_proj_root_path(), 'data', db_name, 'data', f'{dialect}_data.sql'), 'r') as file:
            insert_sqls = str_split(file.read(), ';')
        for sql in tqdm(insert_sqls):
            if sql.strip() == '':
                continue
            else:
                if dialect == 'oracle':
                    flag, res = oracle_sql_execute(db_name, sql + ';')
                elif dialect == 'pg':
                    flag, res = pg_sql_execute(db_name, sql)
                elif dialect =='mysql':
                    flag, res = mysql_sql_execute(db_name, sql)
                else:
                    assert False
                if not flag:
                    print(f'{sql} may fail to execute')
                    exit()
    else:
        for table_name, table_content in schema.items():
            table_name = table_content['table']
            with open(os.path.join(get_proj_root_path(), 'data', db_name, 'data', f'{table_name}_data.json'),
                      'r') as file:
                data = json.loads(file.read())
            print(f'insert into table {table_name}')
            for row in tqdm(data):
                value_str = ''
                assert isinstance(row, dict)
                columns_str = ''
                for key, value in row.items():
                    if value_str != '':
                        value_str = value_str + ', '
                    for col in table_content['cols']:
                        if col['col_name'] == key:
                            value_str = value_str + build_value(col, value, dialect)
                            # if columns_str!= '':
                            #     columns_str = columns_str + ', '
                            # if dialect == 'oracle' or dialect == 'pg':
                            #     columns_str = columns_str + f'"{key}"'
                            # elif dialect =='mysql':
                            #     columns_str = columns_str + f'`{key}`'
                            break
                if dialect == 'oracle':
                    sql = f"INSERT INTO \"{table_name}\" VALUES ({value_str});"
                    flag, res = oracle_sql_execute(db_name, sql)
                elif dialect == 'mysql':
                    sql = f"INSERT INTO `{table_name}` VALUES ({value_str});"
                    flag, res = mysql_sql_execute(db_name, sql)
                elif dialect == 'pg':
                    sql = f"INSERT INTO \"{table_name}\" VALUES ({value_str});"
                    flag, res = pg_sql_execute(db_name, sql)
                if not flag:
                    print(f'{row} may fail to insert in {dialect}, sql is {sql}')
                    print(res)
                    exit()
                # break
    with open(os.path.join(get_proj_root_path(), 'data', db_name, 'ddl', dialect, f'{dialect}_fk.sql'), 'r') as file:
        sqls = file.read().split(';')
    for sql in sqls:
        if sql.strip() == '':
            continue
        else:
            if dialect == 'oracle':
                flag, res = oracle_sql_execute(db_name, sql)
            elif dialect == 'pg':
                flag, res = pg_sql_execute(db_name, sql)
            elif dialect == 'mysql':
                flag, res = mysql_sql_execute(db_name, sql)
            if not flag:
                print(f'{sql} may fail to execute')
                exit()
    with open(os.path.join(get_proj_root_path(), 'data', db_name, 'ddl', dialect, f'{dialect}_idx.sql'), 'r') as file:
        sqls = file.read().split(';')
    for sql in sqls:
        if sql.strip() == '':
            continue
        else:
            if dialect == 'oracle':
                flag, res = oracle_sql_execute(db_name, sql)
            elif dialect == 'pg':
                flag, res = pg_sql_execute(db_name, sql)
            elif dialect == 'mysql':
                flag, res = mysql_sql_execute(db_name, sql)
            if not flag:
                print(f'{sql} may fail to execute')
                exit()
    print(f"{db_name} create successful")
