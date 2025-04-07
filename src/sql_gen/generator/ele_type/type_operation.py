# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: type_operation$
# @Author: 10379
# @Time: 2025/3/24 19:59

import json
import os
from datetime import datetime

from sql_gen.generator.ele_type.type_def import *
from udfs.date_udf import date_format_udf
from utils.tools import get_proj_root_path


def load_col_type(type_def: dict, col_name: str, dialect: str):
    # col_name are used for build VARRAY Type for Oracle
    type_name = type_def['type_name']
    add_constraint = None
    type_defs = []
    if type_name == 'INT':
        final_type = IntType()
    elif type_name == 'BOOL':
        final_type = BoolType()
    elif type_name == 'DECIMAL':
        final_type = DecimalType(type_def['precision'], type_def['scale'])
    elif type_name == 'DOUBLE':
        final_type = DoubleType()
    elif type_name == 'DATE':
        final_type = DateType()
    elif type_name == 'TIME':
        final_type = TimeType(type_def['fraction'])
    elif type_name == 'YEAR':
        final_type = YearType()
    elif type_name == 'TIMESTAMP':
        if 'fraction' in type_def:
            final_type = TimestampType(type_def['fraction'])
        else:
            final_type = TimestampType()
    elif type_name == 'DATETIME':
        if 'fraction' in type_def:
            final_type = DatetimeType(type_def['fraction'])
        else:
            final_type = DatetimeType()
    elif type_name == 'INTERVAL YEAR TO MONTH':
        final_type = IntervalYearMonthType()
    elif type_name == 'TIMESTAMPTZ':
        if 'fraction' in type_def:
            final_type = TimestampTZType(type_def['fraction'])
        else:
            final_type = TimestampTZType()
    elif type_name == 'VARCHAR':
        final_type = VarcharType(type_def['length'])
    elif type_name == 'ENUM':
        values = ''
        for value in type_def['values']:
            if values != '':
                values = values + ', '
            values = values + f"'{value}'"
        final_type = EnumType(type_def['values'])
        if dialect == 'pg':
            add_constraint = f"CONSTRAINT \"{col_name}_check\" CHECK(\"{col_name}\" IN ({values}))"
        elif dialect == 'oracle':
            add_constraint = f"CONSTRAINT \"{col_name}_check\" CHECK(\"{col_name}\" IN ({values}))"
    elif type_name == 'NVARCHAR':
        final_type = NvarcharType(type_def['length'])
    elif type_name == 'CHAR':
        final_type = CharType(type_def['length'])
    elif type_name == 'TEXT':
        final_type = TextType()
    elif type_name == 'UUID':
        final_type = UuidType()
    elif type_name == 'JSON':
        final_type = JsonType(type_def['structure'])
    elif type_name == 'JSONB':
        final_type = JsonbType(type_def['structure'])
    elif type_name == 'POINT':
        final_type = PointType()
    elif type_name == 'XML':
        final_type = XmlType()
    elif type_name == 'BLOB':
        final_type = BlobType()
    elif type_name == 'ARRAY':
        ele_type, ele_constraint, ele_type_defs = load_col_type(type_def['ele_type'], 'sub' + col_name, dialect)
        final_type = ArrayType(ele_type, col_name, type_def['length'])
        type_defs = type_defs + ele_type_defs
        if dialect == 'oracle':
            type_def = f"CREATE TYPE {f'{col_name}_varray_type'} AS VARRAY({type_def['length']}) OF {ele_type};"
            type_defs.append(type_def)
    else:
        assert False
    return final_type, add_constraint, type_defs


def type_statistic():
    types = {
        "INT": 0,
        "BOOL": 0,
        "DECIMAL": 0,
        "DOUBLE": 0,
        "DATE": 0,
        "TIME": 0,
        "YEAR": 0,
        "TIMESTAMP": 0,
        "DATETIME": 0,
        "INTERVAL YEAR TO MONTH": 0,
        "TIMESTAMPTZ": 0,
        "VARCHAR": 0,
        "ENUM": 0,
        "NVARCHAR": 0,
        "CHAR": 0,
        "TEXT": 0,
        "UUID": 0,
        "JSON": 0,
        "JSONB": 0,
        "POINT": 0,
        "XML": 0,
        "BLOB": 0,
        "ARRAY": 0
    }
    table_cnt = 0
    for file in os.listdir(os.path.join(get_proj_root_path(), 'data')):
        db_root_path = os.path.join(get_proj_root_path(), 'data', file)
        if os.path.exists(os.path.join(db_root_path, 'schema.json')):
            with open(os.path.join(db_root_path, 'schema.json'), 'r') as f:
                schema = json.load(f)
            table_cnt += len(schema)
            for table, value in schema.items():
                for col in value['cols']:
                    col_type = col['type']['type_name']
                    types[col_type] = types[col_type] + 1
    for type_name in types:
        print(f"{type_name}: {types[type_name]}")
    cnt = 0
    for key, value in types.items():
        cnt += value
    print(cnt)
    print(table_cnt)


def build_value(built_in_type: BaseType, value, dialect: str) -> str | None:
    if built_in_type is None:
        return None
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        value = value.replace("'", "''")
    return built_in_type.gen_value(dialect, value)