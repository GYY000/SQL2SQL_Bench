# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: insert_builder$
# @Author: 10379
# @Time: 2025/2/18 21:52
import json

from antlr_parser.Tree import TreeNode, try_fetch_nodes_by_route
from antlr_parser.parse_tree import parse_tree


def mysql_value_builder(type, value, format: str = '', timezone=None) -> str:
    if type == 'INT':
        if isinstance(value, int):
            return str(value)
        else:
            assert isinstance(value, str)
            return value
    elif type == 'BOOL':
        if value == True or value == 'True':
            return 'TRUE'
        else:
            return 'FALSE'
    elif type == 'DECIMAL' or type == 'DOUBLE':
        if isinstance(value, float):
            return str(value)
        else:
            assert isinstance(value, str)
            return value
    elif type == 'DATE':
        format = format.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
        return f"STR_TO_DATE('{value}', '{format}')"
    elif type == 'TIME':
        format = format.replace('HH24', '%H').replace('MI', '%i').replace('SS', '%s')
        return f"TIME(STR_TO_DATE('{value}', '{format}')"
    elif type == 'YEAR':
        if isinstance(value, int):
            return str(value)
        else:
            assert isinstance(value, str)
            return value
    elif type == 'TIMESTAMP':
        # TODO: change here in case with time zone
        format = (format.replace('YYYY', '%Y').replace('MM', '%m').replace('DD', '%d')
                  .replace('HH24', '%H').replace('MI', '%i').replace('SS', '%s'))
        return f"TIMESTAMP(STR_TO_DATE('{value}', '{format}'))"
    elif type.startswith('VARCHAR'):
        return "'value'"
    elif type == 'TEXT':
        return "'value'"
    elif type.startswith('CHAR'):
        return "'value'"
    elif type == 'JSON':
        return json.dumps(value)
    elif type == 'POINT':
        if isinstance(value, dict):
            return f"ST_GeomFromText('POINT({value['longitude']} {value['latitude']})', 4326)"
    else:
        assert False


def pg_value_builder(type: str, value, format=None, timezone=None) -> str:
    if not isinstance(value, str):
        value = str(value)
    if type == 'INT' or type == 'SMALLINT':
        if isinstance(value, int):
            return str(value)
        else:
            assert isinstance(value, str)
            return value
    elif type == 'BOOL':
        if value == True or value == 'True':
            return 'TRUE'
        else:
            return 'FALSE'
    elif type == 'DECIMAL' or type == 'DOUBLE PRECISION':
        if isinstance(value, float):
            return str(value)
        else:
            assert isinstance(value, str)
            return value
    elif type == 'DATE':
        return f"TO_DATE({value}, '{format}')"
    elif type == 'TIME':
        return f"TO_TIMESTAMP('{value}', '{format}')::TIME"
    elif type == 'TIMESTAMP':
        return f"TO_TIMESTAMP('{value}', '{format}')"
    elif type == 'TIMESTAMP WITH TIME ZONE':
        return f"TO_TIMESTAMP('{value}', '{format}') AT TIME ZONE '{timezone}'"
    elif type == 'VARCHAR' or type == 'TEXT':
        return f"\'{value}\'"
    elif type == 'UUID':
        return f"\'{value}\'::uuid"
    elif type == 'JSON' or type == 'JSONB':
        return "\'" + json.dumps(value) + "\'"
    elif type == 'GEOMETRY':
        if isinstance(value, dict):
            return f"ST_GeomFromText('POINT({value['longitude']} {value['latitude']})', 4326)"
    else:
        assert False


def oracle_value_builder(type: str, value, format=None, timezone=None) -> str:
    """
        DATE
        NUMBER
        TIMESTAMP
        TIMESTAMP WITH TIME ZONE
        CLOB
        VARCHAR2(36)
        SDO_GEOMETRY
    """
    if type == 'NUMBER' or type == 'FLOAT(126)':
        return str(value)
    elif type == 'DATE':
        return f"TO_DATE('{value}', '{format}')"
    elif type == 'TIMESTAMP':
        return f"TO_TIMESTAMP('{value}', '{format}')"
    elif type == 'TIMESTAMP WITH TIME ZONE':
        return f"TO_TIMESTAMP_TZ('{value} {timezone}:00', '{format} TZR')"
    elif type == 'CLOB' or type.startswith('VARCHAR2'):
        return f"'{value}'"
    elif type == 'SDO_GEOMETRY':
        return f"SDO_GEOMETRY(2001, 4326, SDO_POINT_TYPE({value['longitude']}, {value['latitude']}, NULL), NULL, NULL)"
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


def extract_values_from_insert(ins_sql: str, dialect: str):
    tree_node, _, _, _ = parse_tree(ins_sql, dialect)
    if tree_node is not None:
        node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
    else:
        print(f"Parsing error\n{ins_sql}")
        exit()
    if dialect == 'mysql':
        return node.extract_values_from_mysql_insert()
    elif dialect == 'pg':
        return node.extract_values_from_pg_insert()
    elif dialect == 'oracle':
        single_table_nodes = try_fetch_nodes_by_route(node, ['sql_script', 'unit_statement', 'data_manipulation_language_statements',
                                        'insert_statement', 'single_table_insert'])
        into_clause_node = single_table_nodes[0].get_child_by_value('insert_into_clause')
        table_name_node = into_clause_node.get_child_by_value('general_table_ref')
        column_list_nodes = try_fetch_nodes_by_route(into_clause_node, ['insert_into_clause',
                                                                        'paren_column_list', 'column_list', 'column_name'])
        assert isinstance(into_clause_node, TreeNode)
        value_clause_node = single_table_nodes[0].get_child_by_value('values_clause')
        value_list_nodes = try_fetch_nodes_by_route(value_clause_node, ['values_clause', 'expressions', 'expression'])
        assert len(column_list_nodes) == len(value_list_nodes)
        col_names = [str(col_node) for col_node in column_list_nodes]
        values = [str(value_node) for value_node in value_list_nodes]
        return str(table_name_node), col_names, values
    else:
        assert False
