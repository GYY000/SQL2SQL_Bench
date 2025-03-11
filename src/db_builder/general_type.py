# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: general_type$
# @Author: 10379
# @Time: 2025/3/11 13:56

def build_type(general_type: dict, col_name: str, dialect: str):
    type_name = general_type['type_name']
    final_type = None
    add_constraint = None
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
        if dialect == 'oracle':
            final_type = 'TIMESTAMP'
        else:
            final_type = 'TIME'
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
            final_type = f"NVARCHAR2({general_type['length']})"
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
        else:
            final_type = 'XMLType'
    else:
        assert False
    return final_type, add_constraint


def type_statistic():
    pass