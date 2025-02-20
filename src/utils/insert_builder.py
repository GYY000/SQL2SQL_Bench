# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: insert_builder$
# @Author: 10379
# @Time: 2025/2/18 21:52


"""
INT
BOOL
DECIMAL
DOUBLE
DATE
TIME
YEAR
TIMESTAMP
VARCHAR
TEXT
CHAR(36)
JSON
POINT
"""


def mysql_value_builder(type: str, value, format=None) -> str:
    if type == 'INT':
        if isinstance(value, int):
            return str(value)
        else:
            assert isinstance(value, str)
            return value
    elif type == 'BOOL':
        if value == 'True':
            pass
    elif type == 'DECIMAL':
        pass
    elif type == 'DOUBLE':
        pass
    elif type == 'DATE':
        pass
    elif type == 'TIME':
        pass
    elif type == 'YEAR':
        pass
    elif type == 'TIMESTAMP':
        pass
    elif type == 'VARCHAR':
        pass
    elif type == 'TEXT':
        pass
    elif type.startswith('CHAR'):
        pass
    elif type == 'JSON':
        pass
    elif type == 'POINT':
        pass
    else:
        assert False


def pg_value_builder(type: str, value) -> str:
    """
    DOUBLE PRECISION
    DATE
    TIME
    SMALLINT
    TIMESTAMP
    VARCHAR
    TEXT
    UUID
    JSON
    JSONB
    GEOMETRY
    """
    if not isinstance(value, str):
        value = str(value)
    if type == 'INT':
        pass
    elif type == 'BOOL':
        pass
    elif type == 'DECIMAL':
        pass
    elif type == 'DOUBLE PRECISION':
        pass
    elif type == 'DATE':
        pass
    elif type == 'TIME':
        pass
    elif type == 'SMALLINT':
        pass
    elif type == 'TIMESTAMP':
        pass
    elif type == 'TIMESTAMP WITH TIME ZONE':
        pass
    elif type == 'VARCHAR':
        pass
    elif type == 'TEXT':
        pass
    elif type == 'UUID':
        pass
    elif type == 'JSON':
        pass
    elif type == 'JSONB':
        pass
    elif type == 'GEOMETRY':
        pass
    else:
        assert False


def oracle_value_builder(type: str, value) -> str:
    """
        DATE
        NUMBER
        TIMESTAMP
        TIMESTAMP WITH TIME ZONE
        CLOB
        VARCHAR2(36)
        SDO_GEOMETRY
    """
    if not isinstance(value, str):
        value = str(value)
    if type == 'NUMBER':
        pass
    elif type == 'FLOAT(126)':
        pass
    elif type == 'DATE':
        pass
    elif type == 'TIMESTAMP':
        pass
    elif type == 'TIMESTAMP WITH TIME ZONE':
        pass
    elif type == 'CLOB':
        pass
    elif type.startswith('VARCHAR2'):
        pass
    elif type == 'SDO_GEOMETRY':
        pass
    else:
        assert False


def mysql_insert(table_schema, insert_values) -> str:
    table_name = table_schema['table_name']
    cols = ''
    values = ''
    for col in table_schema['cols']:
        if cols != '':
            cols = cols + ', '
        cols = cols + col['col_name']

    for value in insert_values:
        res_value = '('
        for col in table_schema['cols']:
            ele = value[col['col_name']]
            if res_value != '(':
                res_value = res_value + ', '
            res_value = res_value + mysql_value_builder(col['type']['mysql'], ele)
        values = values + res_value + ')\n'
    return f"INSERT INTO {table_name} ({cols}) VALUES\n{values}"


def pg_builder(table_schema, insert_values) -> str:
    table_name = table_schema['table_name']
    cols = ''
    values = ''
    for col in table_schema['cols']:
        if cols != '':
            cols = cols + ', '
        cols = cols + col['col_name']

    for value in insert_values:
        res_value = '('
        for col in table_schema['cols']:
            ele = value[col['col_name']]
            if res_value != '(':
                res_value = res_value + ', '
            res_value = res_value + pg_value_builder(col['type']['pg'], ele)
        values = values + res_value + ')\n'
    return f"INSERT INTO {table_name} ({cols}) VALUES\n{values};"


def oracle_11_builder(table_schema, insert_values) -> str:
    table_name = table_schema['table_name']
    values = ''
    cols = ''
    for col in table_schema['cols']:
        if cols != '':
            cols = cols + ', '
        cols = cols + col['col_name']

    for value in insert_values:
        res_value = '('
        for col in table_schema['cols']:
            ele = value[col['col_name']]
            if res_value != '(':
                res_value = res_value + ', '
            res_value = res_value + f"\tINTO {table_name} + ({cols}) (" + oracle_value_builder(col['type']['pg'],
                                                                                               ele) + ')\n'
        values = values + res_value
    return f"INSERT ALL\n{values}\nSELECT 1 FROM dual;"
