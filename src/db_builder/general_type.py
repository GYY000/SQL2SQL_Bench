# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: general_type$
# @Author: 10379
# @Time: 2025/3/11 13:56
import json
import os
from datetime import datetime

from udfs.date_udf import date_format_udf
from utils.tools import get_proj_root_path


def build_type(general_type: dict, col_name: str, dialect: str):
    type_name = general_type['type_name']
    final_type = None
    add_constraint = None
    type_defs = []
    if type_name == 'INT':
        if dialect == 'oracle':
            final_type = 'NUMBER'
        else:
            final_type = 'INT'
    elif type_name == 'BOOL':
        if dialect == 'oracle':
            final_type = 'NUMBER(1)'
        else:
            final_type = 'BOOL'
    elif type_name == 'DECIMAL':
        if dialect == 'oracle':
            final_type = f'NUMBER({general_type["precision"]}, {general_type["scale"]})'
        else:
            final_type = f'DECIMAL({general_type['precision']}, {general_type['scale']})'
    elif type_name == 'DOUBLE':
        if dialect == 'oracle':
            final_type = f'FLOAT(126)'
        elif dialect == 'pg':
            final_type = 'DOUBLE PRECISION'
        else:
            final_type = 'DOUBLE'
    elif type_name == 'DATE':
        final_type = 'DATE'
    elif type_name == 'TIME':
        if dialect == 'oracle' or dialect == 'pg':
            final_type = f'INTERVAL DAY TO SECOND{general_type['fraction']}'
        else:
            final_type = f'TIME{general_type['fraction']}'
    elif type_name == 'YEAR':
        if dialect == 'mysql':
            final_type = 'YEAR'
        elif dialect == 'pg':
            final_type = 'SMALLINT'
        elif dialect == 'oracle':
            final_type = 'NUMBER(4)'
    elif type_name == 'TIMESTAMP':
        final_type = 'TIMESTAMP'
    elif type_name == 'DATETIME':
        if dialect == 'mysql':
            final_type = 'DATETIME'
        else:
            final_type = 'TIMESTAMP'
    elif type_name == 'INTERVAL YEAR TO MONTH':
        if dialect == 'mysql':
            final_type = None
        else:
            final_type = 'INTERVAL YEAR TO MONTH'
    elif type_name == 'TIMESTAMPTZ':
        if dialect == 'mysql':
            final_type = 'TIMESTAMP'
        else:
            final_type = 'TIMESTAMP WITH TIME ZONE'
    elif type_name == 'VARCHAR':
        if dialect == 'oracle':
            final_type = f'VARCHAR2({general_type["length"]})'
        else:
            final_type = f'VARCHAR({general_type['length']})'
    elif type_name == 'ENUM':
        values = ''
        for value in general_type['values']:
            if values != '':
                values = values + ', '
            values = values + f"'{value}'"
        if dialect == 'mysql':
            final_type = f"ENUM({values})"
        elif dialect == 'pg':
            final_type = f"VARCHAR(100)"
            add_constraint = f"CONSTRAINT \"{col_name}_check\" CHECK(\"{col_name}\" IN ({values}))"
        elif dialect == 'oracle':
            final_type = f"VARCHAR2(100)"
            add_constraint = f"CONSTRAINT \"{col_name}_check\" CHECK(\"{col_name}\" IN ({values}))"
    elif type_name == 'NVARCHAR':
        if dialect == 'mysql':
            final_type = f"NVARCHAR({general_type['length']})"
        elif dialect == 'pg':
            final_type = f'VARCHAR({general_type['length']})'
        elif dialect == 'oracle':
            final_type = f"VARCHAR2({general_type['length']} CHAR)"
    elif type_name == 'CHAR':
        final_type = f"CHAR({general_type['length']})"
    elif type_name == 'TEXT':
        if dialect == 'oracle':
            final_type = f"VARCHAR2(4000)"
        else:
            final_type = 'TEXT'
    elif type_name == 'UUID':
        if dialect == 'mysql' or dialect == 'oracle':
            final_type = f"CHAR(36)"
        else:
            final_type = 'UUID'
    elif type_name == 'JSON':
        if dialect == 'mysql':
            final_type = 'JSON'
        elif dialect == 'pg':
            final_type = 'JSON'
        else:
            final_type = None
    elif type_name == 'JSONB':
        if dialect == 'mysql':
            final_type = 'JSON'
        elif dialect == 'pg':
            final_type = 'JSONB'
        else:
            final_type = None
    elif type_name == 'POINT':
        if dialect == 'oracle':
            final_type = 'SDO_GEOMETRY'
        elif dialect == 'pg':
            final_type = 'GEOMETRY'
        else:
            final_type = 'POINT'
    elif type_name == 'XML':
        if dialect == 'mysql':
            final_type = 'TEXT'
        elif dialect == 'pg':
            final_type = 'XML'
        elif dialect == 'oracle':
            final_type = 'XMLType'
    elif type_name == 'BLOB':
        if dialect == 'mysql' or dialect == 'oracle':
            final_type = 'BLOB'
        elif dialect == 'pg':
            final_type = 'BYTEA'
    elif type_name == 'ARRAY':
        if dialect == 'oracle':
            final_type = f'{col_name}_varray_type'
            ele_type, ele_constraint, ele_type_defs = build_type(general_type['ele_type'], 'sub' + col_name, dialect)
            type_def = f"CREATE TYPE {final_type} AS VARRAY({general_type['length']}) OF {ele_type};"
            type_defs = type_defs + ele_type_defs
            type_defs.append(type_def)
        elif dialect == 'mysql':
            final_type = 'JSON'
        elif dialect == 'pg':
            ele_type, ele_constraint, ele_type_defs = build_type(general_type['ele_type'], 'sub' + col_name, dialect)
            type_defs = type_defs + ele_type_defs
            final_type = ele_type + '[]'
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
    for file in os.listdir(os.path.join(get_proj_root_path(), 'data')):
        db_root_path = os.path.join(get_proj_root_path(), 'data', file)
        if os.path.exists(os.path.join(db_root_path, 'schema.json')):
            with open(os.path.join(db_root_path, 'schema.json'), 'r') as f:
                schema = json.load(f)
            for table in schema:
                if 'table' in table:
                    for col in table['cols']:
                        col_type = col['type']['type_name']
                        types[col_type] = types[col_type] + 1
    for type_name in types:
        print(f"{type_name}: {types[type_name]}")


def build_value(col: dict, value, dialect: str) -> str | None:
    if value is None:
        return 'NULL'
    col_name = col['col_name']
    type_name = col['type']['type_name']
    if isinstance(value, str):
        value = value.replace("'", "''")
    if type_name == 'INT':
        return str(value)
    elif type_name == 'BOOL':
        if dialect == 'oracle':
            if value:
                return '1'
            else:
                return '0'
        else:
            if value:
                return 'True'
            else:
                return 'False'
    elif type_name in ['DECIMAL', 'DOUBLE']:
        return str(value)
    elif type_name == 'DATE':
        assert isinstance(value, dict)
        if dialect == 'mysql':
            date_format = date_format_udf(value['format'])
            return f"STR_TO_DATE('{value['value']}', '{date_format}')"
        elif dialect == 'pg':
            return f"TO_DATE('{value['value']}', '{value['format']}')"
        elif dialect == 'oracle':
            return f"TO_DATE('{value['value']}', '{value['format']}')"
    elif type_name == 'TIME':
        assert isinstance(value, dict)
        if dialect == 'mysql':
            return f"CAST({value['value']} AS TIME({col['type']['fraction']})"
        else:
            parts = value['value'].split(':')
            negative = False
            hours = int(parts[0])
            if hours < 0:
                negative = True
                hours = -1 * hours
            minutes = int(parts[1])
            seconds = float(parts[2])
            days = hours // 24
            hours %= 24
            if negative:
                days = -1 * days
            if dialect == 'oracle':
                return (f"INTERVAL '{days} {hours:02}:{minutes:02}:{seconds:02}' "
                        f"DAY TO SECOND({col['type']['fraction']})")
            elif dialect == 'pg':
                return (f"'{days} days {hours}:{minutes}:{seconds:02}'::INTERVAL "
                        f"DAY TO SECOND({col['type']['fraction']})")
    elif type_name == 'YEAR':
        return str(value)
    elif type_name == 'TIMESTAMP':
        assert isinstance(value, dict)
        timestamp_obj = datetime.strptime(value['value'], date_format_udf(value['format']))
        formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
        if dialect == 'oracle':
            return f"TO_TIMESTAMP('{value['value']}', '{value['format']}')"
        elif dialect == 'pg':
            return f"'{formatted_timestamp_str}'::timestamp"
        elif dialect == 'mysql':
            return f"TIMESTAMP('{formatted_timestamp_str}')"
    elif type_name == 'DATETIME':
        assert isinstance(value, dict)
        date_format_udf(value['format'])
        timestamp_obj = datetime.strptime(value['value'], date_format_udf(value['format']))
        formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
        if dialect == 'mysql':
            return formatted_timestamp_str
        else:
            if dialect == 'oracle':
                return f"TO_TIMESTAMP('{formatted_timestamp_str}', 'yyyy-MM-dd HH24:mi:ss')"
            elif dialect == 'pg':
                return f"TIMESTAMP('{formatted_timestamp_str}')"
    elif type_name == 'INTERVAL YEAR TO MONTH':
        if dialect == 'mysql':
            return None
        else:
            assert isinstance(value, dict)
            if dialect == 'pg':
                if value['sign']:
                    value['year'] = -1 * value['year']
                return f"'{value['year']} years {value['month']} months'::INTERVAL YEAR TO MONTH"
            elif dialect == 'oracle':
                if value['sign']:
                    value['year'] = -1 * value['year']
                return f"to_yminterval('{value['year']}-{value['month']}')"
    elif type_name == 'TIMESTAMPTZ':
        # TODO
        return None
        # if dialect == 'mysql':
        #     final_type = 'TIMESTAMP'
        # else:
        #     final_type = 'TIMESTAMP WITH TIME ZONE'
    elif type_name in ['VARCHAR', 'CHAR', 'ENUM', 'NVARCHAR', 'TEXT']:
        return f"\'{value}\'"
    elif type_name == 'UUID':
        if dialect == 'mysql' or dialect == 'oracle':
            return f"\'{value}\'"
        else:
            return f"\'{value}\'::uuid"
    elif type_name == 'JSON':
        if dialect == 'mysql':
            return "\'" + json.dumps(value) + "\'"
        elif dialect == 'pg':
            return "\'" + json.dumps(value) + "\'::json"
        else:
            assert False
    elif type_name == 'JSONB':
        if dialect == 'mysql':
            return "\'" + json.dumps(value) + "\'"
        elif dialect == 'pg':
            return "\'" + json.dumps(value) + "\'::jsonb"
        else:
            assert False
    elif type_name == 'POINT':
        assert isinstance(value, dict)
        if value['longitude'] is None or value['latitude'] is None:
            return 'NULL'
        if dialect == 'oracle':
            return (f"SDO_GEOMETRY(2001, 4326, "
                    f"SDO_POINT_TYPE({value['longitude']}, {value['latitude']}, NULL), NULL, NULL)")
        elif dialect == 'pg':
            return f"ST_GeomFromText('POINT({value['longitude']} {value['latitude']})', 4326)"
        elif dialect == 'mysql':
            return f"ST_GeomFromText('POINT({value['latitude']} {value['longitude']})', 4326)"
    elif type_name == 'XML':
        if dialect == 'mysql':
            return f"'{value}'"
        elif dialect == 'pg':
            return f"\'{value}\'::xml"
        elif dialect == 'oracle':
            return f"xmltype(\'{value}\')"
    elif type_name == 'BLOB':
        assert False
        # if dialect =='mysql' or dialect == 'oracle':
        #     final_type = 'BLOB'
        # elif dialect == 'pg':
        #     final_type = 'BYTEA'
    elif type_name == 'ARRAY':
        if dialect == 'oracle':
            final_type = f'{col_name}_varray_type'
            ele_str = ''
            for ele in value:
                if ele_str != '':
                    ele_str = ele_str + ', '
                ele_str = ele_str + build_value({
                    'col_name': 'sub' + col_name,
                    'type': col['type']['ele_type']
                }, ele, dialect)
            return f"{final_type}({ele_str})"
        elif dialect == 'mysql':
            return f'\'{json.dumps(value)}\''
        elif dialect == 'pg':
            ele_str = ''
            for ele in value:
                if ele_str != '':
                    ele_str = ele_str + ', '
                ele_str = ele_str + build_value({
                    'col_name': 'sub' + col_name,
                    'type':  col['type']['ele_type']
                }, ele, dialect)
            return f"ARRAY[{ele_str}]"
    assert False
    # return final_type, add_constraint, type_defs
