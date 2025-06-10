# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: schema_builder$
# @Author: 10379
# @Time: 2025/2/18 21:52
import json
import os.path

from tqdm import tqdm

from sql_gen.generator.ele_type.type_def import BaseType
from sql_gen.generator.ele_type.type_operation import load_col_type, build_value
from utils.db_connector import oracle_sql_execute, mysql_sql_execute, pg_sql_execute, oracle_drop_db, pg_drop_db, \
    mysql_drop_db, sql_execute
from utils.tools import str_split, get_table_col_name, get_data_path, get_empty_db_name, load_db_config

id_number = {}


def create_table(table_schema, constraints, dialect):
    if dialect == 'pg':
        return pg_create_table(table_schema, constraints)
    elif dialect == 'mysql':
        return mysql_create_table(table_schema, constraints)
    elif dialect == 'oracle':
        return oracle_create_table(table_schema, constraints)
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


def build_attributes(col_type, attributes, dialect):
    res = []
    if attributes is None:
        return res
    for attribute in attributes:
        if attribute is None:
            continue
        elif isinstance(attribute, str):
            res.append(str(attribute))
        else:
            assert isinstance(attribute, dict)
            attr_type = attribute['type']
            if attr_type == 'default':
                res.append(f"DEFAULT {build_value(col_type, attribute['value'], dialect)}")
    return res


def mysql_create_table(table_schema, constraints):
    table_name = table_schema['table']
    cols = table_schema['cols']
    primary_keys = ''
    if "primary_key" in table_schema:
        primary_key = table_schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + get_table_col_name(key, 'mysql')
    col_defs = ''
    for col in cols:
        col_name = col['col_name']
        if col['type'].get_type_name('mysql') is None:
            continue
        assert isinstance(col, dict)
        attributes = build_attributes(col['type'], col.get('attribute', None), 'mysql')
        str_attribute = ''
        for attribute in attributes:
            str_attribute = str_attribute + ' '
            str_attribute = str_attribute + attribute
        type_def = (f"\t{get_table_col_name(col_name, 'mysql')} "
                    f"{col['type'].get_type_name('mysql')}{str_attribute}")
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    create_stmt = f"CREATE TABLE {get_table_col_name(table_name, 'mysql')} (\n{col_defs}"
    if primary_keys != '':
        constraints.append(
            f"CONSTRAINT {get_table_col_name(rename_constraints(f'PK_{table_name}'), 'mysql')} "
            f"PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt


def mysql_add_foreign_key(schema):
    table = schema['table']
    res = []
    if 'foreign_key' not in schema:
        return res
    for fk in schema['foreign_key']:
        ori_column = fk['col']
        ref_table = fk['ref_table']
        ori_ref_column = fk['ref_col']
        if isinstance(ori_column, list):
            column = ''
            for column in ori_column:
                if column != '':
                    column = column + ', '
                column = column + get_table_col_name(column, 'mysql')
            assert isinstance(ori_ref_column, list)
            ref_column = ''
            for column in ori_ref_column:
                if ref_column != '':
                    ref_column = ref_column + ', '
                ref_column = ref_column + get_table_col_name(column, 'mysql')
        else:
            column = get_table_col_name(ori_column, 'mysql')
            ref_column = get_table_col_name(ori_ref_column, 'mysql')
        res.append((
            f"ALTER TABLE {get_table_col_name(table, 'mysql')} ADD CONSTRAINT "
            f"{get_table_col_name(rename_constraints(f'FK_{table}'), 'mysql')} "
            f"FOREIGN KEY ({column}) REFERENCES {get_table_col_name(ref_table, 'mysql')} ({ref_column}) "
            f"ON DELETE CASCADE ON UPDATE NO ACTION;"))
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
            str_columns = str_columns + get_table_col_name(col, 'mysql')
        res.append(
            f"CREATE INDEX {rename_constraints(f'IDX_{table}')} ON {get_table_col_name(table, 'mysql')} "
            f"({str_columns});")
    return res


def pg_create_table(table_schema: dict, constraints):
    table_name = table_schema['table']
    cols = table_schema['cols']
    primary_keys = ''
    if "primary_key" in table_schema:
        primary_key = table_schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + get_table_col_name(key, 'pg')
    col_defs = ''
    for col in cols:
        col_name = col['col_name']
        if col['type'].get_type_name('pg') is None:
            continue
        attributes = build_attributes(col['type'], col.get('attribute', None), 'pg')
        str_attribute = ''
        for attribute in attributes:
            str_attribute = str_attribute + ' '
            str_attribute = str_attribute + attribute
        type_def = f"\t{get_table_col_name(col_name, 'pg')} {col['type'].get_type_name('pg')}{str_attribute}"
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    create_stmt = f"CREATE TABLE {get_table_col_name(table_name, 'pg')} (\n{col_defs}"
    if primary_keys != '':
        constraints.append(
            f"CONSTRAINT {get_table_col_name(rename_constraints(f'PK_{table_name}'), 'pg')} "
            f"PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt


def pg_add_foreign_key(schema):
    table = schema['table']
    res = []
    if 'foreign_key' not in schema:
        return res
    for fk in schema['foreign_key']:
        ori_column = fk['col']
        ref_table = fk['ref_table']
        ori_ref_column = fk['ref_col']
        if isinstance(ori_column, list):
            column = ''
            for column in ori_column:
                if column != '':
                    column = column + ', '
                column = column + get_table_col_name(column, 'pg')
            assert isinstance(ori_ref_column, list)
            ref_column = ''
            for column in ori_ref_column:
                if ref_column != '':
                    ref_column = ref_column + ', '
                ref_column = ref_column + get_table_col_name(column, 'pg')
        else:
            column = get_table_col_name(ori_column, 'pg')
            ref_column = get_table_col_name(ori_ref_column, 'pg')
        res.append((
            f"ALTER TABLE {get_table_col_name(table, 'pg')}\n"
            f"ADD CONSTRAINT {rename_constraints(f'FK_{table}')} "
            f"FOREIGN KEY ({column})\n\t"
            f"REFERENCES {get_table_col_name(ref_table, 'pg')} ({ref_column}) "
            f"ON DELETE CASCADE ON UPDATE NO ACTION;"))
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
            str_columns = str_columns + get_table_col_name(col, 'pg')
        res.append(
            f"CREATE INDEX {rename_constraints(f'IDX_{table}')} "
            f"ON {get_table_col_name(table, 'pg')} ({str_columns});")
    return res


def oracle_create_table(table_schema: dict, constraints):
    table_name = table_schema['table']
    cols = table_schema['cols']
    primary_keys = ''
    if "primary_key" in table_schema:
        primary_key = table_schema['primary_key']
        for key in primary_key:
            if primary_keys != '':
                primary_keys = primary_keys + ', '
            primary_keys = primary_keys + get_table_col_name(key, 'oracle')
    col_defs = ''
    for col in cols:
        col_name = col['col_name']
        assert isinstance(col['type'], BaseType)
        if col['type'].get_type_name('oracle') is None:
            continue
        if 'attribute' in col and 'NOT NULL' in col['attribute']:
            type_def = (f"\t{get_table_col_name(col_name, 'oracle')} "
                        f"{col['type'].get_type_name('oracle')} NOT NULL")
        else:
            type_def = (f"\t{get_table_col_name(col_name, 'oracle')} "
                        f"{col['type'].get_type_name('oracle')}")
        if col_defs != '':
            col_defs = col_defs + ',\n'
        col_defs = col_defs + type_def
    create_stmt = f"CREATE TABLE {get_table_col_name(table_name, 'oracle')} (\n{col_defs}"
    if primary_keys != '':
        constraints.append(
            f"CONSTRAINT {get_table_col_name(rename_constraints(f'PK_{table_name}'), 'oracle')} "
            f"PRIMARY KEY ({primary_keys})")
    for constraint in constraints:
        create_stmt += ',\n\t' + constraint
    create_stmt += '\n);'
    return create_stmt


def oracle_add_foreign_key(schema):
    table = schema['table']
    res = []
    if 'foreign_key' not in schema:
        return res
    for fk in schema['foreign_key']:
        ori_column = fk['col']
        ref_table = fk['ref_table']
        ori_ref_column = fk['ref_col']
        if isinstance(ori_column, list):
            column = ''
            for column in ori_column:
                if column != '':
                    column = column + ', '
                column = column + get_table_col_name(column, 'oracle')
            assert isinstance(ori_ref_column, list)
            ref_column = ''
            for ref_column in ori_ref_column:
                if ref_column != '':
                    ref_column = ref_column + ', '
                ref_column = ref_column + get_table_col_name(ref_column, 'oracle')
        else:
            column = get_table_col_name(ori_column, 'oracle')
            ref_column = get_table_col_name(ori_ref_column, 'oracle')
        res.append((
            f"ALTER TABLE {get_table_col_name(table, 'oracle')}\n"
            f"ADD CONSTRAINT {rename_constraints(f'FK_{table}')} "
            f"FOREIGN KEY ({column})\n\t"
            f"REFERENCES {get_table_col_name(ref_table, 'oracle')} ({ref_column});"))
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
            str_columns = str_columns + get_table_col_name(col, 'oracle')
        res.append(
            f"CREATE INDEX {rename_constraints(f'IDX_{table}')} ON {get_table_col_name(table, 'oracle')} "
            f"({str_columns});")
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


def dump_schema(schema: dict, constraints: dict, schema_type_defs: dict, dialect: str, db_name: str):
    db_path = os.path.join(get_data_path(), db_name)
    ddl_dir = os.path.join(db_path, 'ddl')
    if not os.path.exists(ddl_dir):
        os.makedirs(ddl_dir)
    ddls = []
    foreign_key = []
    indexes = []
    type_defs = []
    for table_name, table_content in schema.items():
        create_stmt = create_table(table_content, constraints[table_name], dialect)
        ddls.append(create_stmt)
        type_defs = type_defs + schema_type_defs[table_name]
        foreign_key = foreign_key + add_foreign_key(table_content, dialect)
        indexes = indexes + add_index(table_content, dialect)

    if not os.path.exists(os.path.join(ddl_dir, dialect)):
        os.makedirs(os.path.join(ddl_dir, dialect))
    sql_writer(os.path.join(ddl_dir, dialect, f'{dialect}_ddl.sql'), type_defs + ddls)
    sql_writer(os.path.join(ddl_dir, dialect, f'{dialect}_fk.sql'), foreign_key)
    sql_writer(os.path.join(ddl_dir, dialect, f'{dialect}_idx.sql'), indexes)


def drop_schema(db_name, dialect, empty_db_flag: bool = False):
    if empty_db_flag:
        db_name = get_empty_db_name(db_name)
    if dialect == 'oracle':
        oracle_drop_db(db_name)
        return
    elif dialect == 'pg':
        pg_drop_db(db_name)
        return
    elif dialect == 'mysql':
        mysql_drop_db(db_name)
        return
    else:
        assert False


def create_schema(db_name, dialect, schema=None, empty_db_flag=False):
    if schema is None:
        with open(os.path.join(get_data_path(), db_name, 'schema.json'), 'r') as file:
            schema = json.loads(file.read())
    with open(os.path.join(get_data_path(), db_name, 'ddl', dialect, f'{dialect}_ddl.sql'), 'r') as file:
        sqls = file.read().split(';')
    if empty_db_flag:
        db_name = get_empty_db_name(db_name)
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
                flag, res = oracle_sql_execute(db_name, f"SELECT * "
                                                        f"FROM {get_table_col_name(table_name, 'oracle')};")
            elif dialect == 'mysql':
                flag, res = mysql_sql_execute(db_name, f"SELECT * "
                                                       f"FROM {get_table_col_name(table_name, 'mysql')};")
            elif dialect == 'pg':
                flag, res = pg_sql_execute(db_name, f"SELECT * "
                                                    f"FROM {get_table_col_name(table_name, 'pg')};")
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


def build_foreign_key(db_name, dialect) -> bool:
    with open(os.path.join(get_data_path(), db_name, 'ddl', dialect, f'{dialect}_fk.sql'), 'r') as file:
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
            else:
                assert False
            if not flag:
                print(f'{sql} may fail to execute')
                exit()
    return True


def build_index(db_name, dialect) -> bool:
    with open(os.path.join(get_data_path(), db_name, 'ddl', dialect, f'{dialect}_idx.sql'), 'r') as file:
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
            else:
                assert False
            if not flag:
                print(f'{sql} may fail to execute')
                return False
    return True


limit = load_db_config()
max_oracle_sql = limit['max_len_oracle_sql']
max_mysql_sql = limit['max_len_mysql_sql']
max_pg_sql = limit['max_len_pg_sql']


def build_insert(db_name: str, dialect: str, schema: dict):
    if os.path.exists(os.path.join(get_data_path(), db_name, 'data', f'{dialect}_data_revised.sql')):
        with open(os.path.join(get_data_path(), db_name, 'data', f'{dialect}_data_revised.sql'), 'r') as file:
            insert_sqls = str_split(file.read(), ';')
        for sql in tqdm(insert_sqls):
            if sql.strip() == '':
                continue
            else:
                if dialect == 'oracle':
                    flag, res = oracle_sql_execute(db_name, sql)
                elif dialect == 'pg':
                    flag, res = pg_sql_execute(db_name, sql)
                elif dialect == 'mysql':
                    flag, res = mysql_sql_execute(db_name, sql)
                else:
                    assert False
                if not flag:
                    print(f'{sql} may fail to execute')
                    return False
    else:
        if dialect == 'mysql':
            limit = max_mysql_sql
        elif dialect == 'pg':
            limit = max_pg_sql
        elif dialect == 'oracle':
            limit = max_oracle_sql
        else:
            assert False
        for table_name, table_content in schema.items():
            table_name = table_content['table']
            with open(os.path.join(get_data_path(), db_name, 'data', f'{table_name}.json'),
                      'r', encoding='utf-8') as file:
                data = json.load(file)
            print(f'insert into table {table_name}')
            first_row_flag = False
            pos_key_map = {}

            not_insert_flag = False
            if dialect == 'oracle':
                insert_sql = f"INSERT ALL\n"
            else:
                insert_sql = f"INSERT INTO {get_table_col_name(table_name, dialect)} VALUES "
            for row in tqdm(data):
                value_str = ''
                # columns_str = ''
                t = 0
                if not first_row_flag:
                    for key in row:
                        pos_key_map[key] = t
                        t += 1
                    first_row_flag = True
                    continue
                for col in table_content['cols']:
                    assert col['col_name'] in pos_key_map
                    value = row[pos_key_map[col['col_name']]]
                    value_rep = build_value(col['type'], value, dialect)
                    if value_rep is None:
                        continue
                    if value_str != '':
                        value_str = value_str + ', '
                    value_str = value_str + value_rep
                if dialect == 'oracle':
                    try_all_sql = f"INSERT ALL INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str}) SELECT 1 FROM dual;"
                    if len(try_all_sql) > limit:
                        ins_sql = f"INSERT INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str});"
                        flag, res = oracle_sql_execute(db_name, ins_sql)
                        if not flag:
                            print(f'{row} may fail to insert in {dialect}')
                            print(ins_sql)
                            print(res)
                            return False
                    else:
                        if len(f"{insert_sql} INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str}) SELECT 1 FROM dual;") < max_oracle_sql:
                            insert_sql = f"{insert_sql} INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
                        else:
                            flag, res = oracle_sql_execute(db_name, insert_sql + ' SELECT 1 FROM dual')
                            if not flag:
                                print(f'{row} may fail to insert in {dialect}')
                                print(insert_sql)
                                print(res)
                                return False
                            insert_sql = f"INSERT ALL INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
                        not_insert_flag = True
                elif dialect == 'mysql' or dialect == 'pg':
                    if not_insert_flag is False:
                        new_insert_sql = f"{insert_sql} ({value_str})"
                    else:
                        new_insert_sql = f"{insert_sql}, ({value_str})"
                    if len(new_insert_sql) < max_mysql_sql:
                        insert_sql = new_insert_sql
                    else:
                        flag, res = sql_execute(dialect, db_name, insert_sql)
                        if not flag:
                            print(f'{row} may fail to insert in {dialect}')
                            print(insert_sql)
                            print(res)
                            return False
                        insert_sql = f"INSERT INTO {get_table_col_name(table_name, dialect)} VALUES ({value_str})"
                    not_insert_flag = True
                else:
                    assert False
            if not_insert_flag:
                if dialect == 'oracle':
                    insert_sql = insert_sql + ' SELECT 1 FROM dual'
                flag, res = sql_execute(dialect, db_name, insert_sql)
                if not flag:
                    print(f'{insert_sql} may fail to insert in {dialect}')
                    print(res)
                    return False
    return True


def schema_build(db_name, dialect):
    with open(os.path.join(get_data_path(), db_name, 'schema.json'), 'r') as file:
        schema = json.loads(file.read())

    add_constraints = {}
    type_defs = {}
    for table_name, value in schema.items():
        add_constraints[table_name] = []
        type_defs[table_name] = []
        for col in value['cols']:
            built_in_type, add_constraint, col_type_defs = load_col_type(col['type'], col['col_name'], dialect, db_name)
            col['type'] = built_in_type
            if add_constraint is not None:
                add_constraints[table_name].append(add_constraint)
            type_defs[table_name] = type_defs[table_name] + col_type_defs
    return schema, add_constraints, type_defs


def build_db(db_name: str, dialect: str, only_create: bool = False, build_fk: bool = True, build_idx: bool = True):
    schema, add_constraints, type_defs = schema_build(db_name, dialect)
    dump_schema(schema, add_constraints, type_defs, dialect, db_name)
    create_schema(db_name, dialect, schema)
    if only_create:
        return
    if build_insert(db_name, dialect, schema):
        print(f"{db_name} insert successfully")
    else:
        print(f"{db_name} insert failed")
        exit()
    if build_fk:
        if build_foreign_key(db_name, dialect):
            print('Foreign key build successfully')
        else:
            print('Foreign key build failed')
            exit()
    if build_idx:
        if build_index(db_name, dialect):
            print('Index build successfully')
        else:
            print('Index build failed')
            exit()
    print(f"{db_name} create successful")


def build_test_db(db_name, dialect):
    schema, add_constraints, type_defs = schema_build(db_name, dialect)
    dump_schema(schema, add_constraints, type_defs, dialect, db_name)
    create_schema(db_name, dialect, schema, True)
