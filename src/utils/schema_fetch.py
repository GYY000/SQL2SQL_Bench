import json

from antlr_parser.Tree import TreeNode, try_fetch_nodes_by_route
from antlr_parser.parse_tree import parse_tree

import mysql.connector

from db_builder.insert_builder import extract_values_from_insert
from utils.db_connector import mysql_sql_execute, pg_sql_execute
from utils.tools import extract_parameters

dbg = False


def strip_quote(string: str, quote: str):
    while string.startswith(quote):
        string = string[1:]
    while string.endswith(quote):
        string = string[:len(string) - 1]
    return string


def mysql_schema_fetch_table(schema_name: str):
    query = f"""
    SELECT 
        TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE
    FROM 
        INFORMATION_SCHEMA.COLUMNS 
    WHERE 
        TABLE_SCHEMA = '{schema_name}';
    """
    flag, res = mysql_sql_execute(schema_name, query)
    if not flag:
        exit()
    if dbg:
        type_sets = set()
    tables = {}
    foreign_keys = []
    for (table_name, column_name, data_type, type_len, is_nullable, precision, scale) in res:
        if table_name not in tables:
            tables[table_name] = {
                "table": table_name,
                "cols": []
            }
        if data_type.upper() in ['NUMERIC', "DECIMAL"]:
            if precision and scale is not None:
                data_type = f"{data_type.upper()}({precision}, {scale})"
            elif precision is not None:
                data_type = f"{data_type.upper()}({precision})"
        elif data_type.upper() == 'VARCHAR':
            if type_len is not None:
                data_type = f"VARCHAR({type_len})"
        else:
            if dbg:
                type_sets.add(data_type)

        table = tables[table_name]
        col = {
            "col_name": column_name,
            "type": {
                "mysql": data_type.upper()
            }
        }
        if is_nullable == 'NO':
            col['attribute'] = 'NOT NULL'
        table['cols'].append(col)
    query = f"""
        SELECT 
            CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE 
            TABLE_SCHEMA = '{schema_name}';
        """
    flag, res = mysql_sql_execute(schema_name, query)
    for (cons_name, tbl_name, col_name, ref_table_name, ref_col_name) in res:
        if cons_name == 'PRIMARY':
            if "primary_key" not in tables[tbl_name]:
                tables[tbl_name]['primary_key'] = []
            tables[tbl_name]['primary_key'].append(col_name)
        elif ref_table_name is not None:
            foreign_keys.append(
                {
                    "FK_table": tbl_name,
                    "FK_col": col_name,
                    "REF_table": ref_table_name,
                    "REF_col": ref_col_name
                }
            )
    if not flag:
        exit()
    res = []
    for name, table in tables.items():
        res.append(table)
    for f_key in foreign_keys:
        res.append(f_key)
    return res


def mysql_schema_fetch_ddl(ddl_sql: str):
    dialect = 'mysql'
    node, _, _, _ = parse_tree(ddl_sql, dialect)
    node = TreeNode.make_g4_tree_by_node(node, dialect)
    table_names = try_fetch_nodes_by_route(node, ['root', 'sqlStatements', 'sqlStatement',
                                                  'ddlStatement', 'createTable', 'tableName'])
    assert len(table_names) == 1
    table_name = strip_quote(str(table_names[0]), '`')
    col_def_nodes = try_fetch_nodes_by_route(node, ['root', 'sqlStatements', 'sqlStatement',
                                                    'ddlStatement', 'createTable', 'createDefinitions',
                                                    'createDefinition'])

    primary_key = []
    cols = []
    for node in col_def_nodes:
        if node.get_child_by_value('tableConstraint') is not None:
            table_constraint_nodes = node.get_children_by_value('tableConstraint')
            for table_constraint_node in table_constraint_nodes:
                if 'PRIMARY KEY' in str(table_constraint_node):
                    index_column_name_nodes = try_fetch_nodes_by_route(table_constraint_node,
                                                                       ['tableConstraint', 'indexColumnNames',
                                                                        'indexColumnName'])
                    for index_column_name_node in index_column_name_nodes:
                        primary_key.append(strip_quote(str(index_column_name_node), '`'))
            continue
        col_name_node = node.get_child_by_value('fullColumnName')
        type_def_node = node.get_child_by_value('columnDefinition')
        data_type_node = type_def_node.get_child_by_value('dataType')
        constraint_nodes = type_def_node.get_children_by_value('columnConstraint')
        not_null = False
        for constraint in constraint_nodes:
            assert isinstance(constraint, TreeNode)
            if 'PRIMARY KEY' in str(constraint):
                primary_key.append(str(col_name_node))
            elif "NOT NULL" in str(constraint):
                not_null = True
        if not_null:
            cols.append({
                "col_name": strip_quote(str(col_name_node), '`'),
                "type": {
                    "mysql": str(data_type_node).upper(),
                },
                "attribute": [
                    "NOT NULL"
                ]
            })
        else:
            cols.append({
                "col_name": strip_quote(str(col_name_node), '`'),
                "type": {
                    "mysql": str(data_type_node).upper(),
                }
            })

    if primary_key is not None:
        return {
            "table": table_name,
            "primary_key": primary_key,
            "cols": cols
        }
    else:
        return {
            "table": table_name,
            "cols": cols
        }


def get_pg_table_constraints(table, dbname):
    # 获取主键
    flag, res = pg_sql_execute(dbname, f"""
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE t.relname = '{table}' AND c.contype = 'p'
            ORDER BY array_position(c.conkey, a.attnum)
        """)
    primary_keys = [row[0] for row in res]

    # 获取外键
    flag, res = pg_sql_execute(dbname, f"""
            SELECT
                c.conname,
                a.attname AS column_name,
                c.confrelid::regclass AS referenced_table,
                af.attname AS referenced_column,
                cols.pos
            FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS cols(attnum, pos) ON TRUE
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = cols.attnum
            JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS confcols(attnum, pos) ON cols.pos = confcols.pos
            JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = confcols.attnum
            WHERE t.relname = '{table}' AND c.contype = 'f'
            ORDER BY c.conname, cols.pos
        """)
    foreign_keys = {}
    for row in res:
        conname, col, ref_table, ref_col, _ = row
        if conname not in foreign_keys:
            foreign_keys[conname] = {
                'columns': [],
                'referenced_table': ref_table,
                'referenced_columns': []
            }
        foreign_keys[conname]['columns'].append(col)
        foreign_keys[conname]['referenced_columns'].append(ref_col)

    return {
        'primary_keys': primary_keys,
        'foreign_keys': foreign_keys
    }


def pg_schema_fetch_table(schema_name: str):
    query = f"""
        SELECT 
            TABLE_NAME, COLUMN_NAME, data_type, character_maximum_length, is_nullable, numeric_precision, numeric_scale
        FROM 
            INFORMATION_SCHEMA.columns 
        WHERE 
             table_schema = 'public' and table_catalog = '{schema_name}';
    """
    flag, res = pg_sql_execute(schema_name, query)
    if not flag:
        exit()
    if dbg:
        type_sets = set()
    tables = {}
    foreign_keys = []
    for (table_name, column_name, data_type, type_len, is_nullable, precision, scale) in res:
        if table_name not in tables:
            tables[table_name] = {
                "table": table_name,
                "cols": []
            }
        if data_type.upper() in ['NUMERIC', "DECIMAL"]:
            if precision and scale is not None:
                data_type = f"{data_type.upper()}({precision}, {scale})"
            elif precision is not None:
                data_type = f"{data_type.upper()}({precision})"
        elif data_type.upper() == 'VARCHAR':
            if type_len is not None:
                data_type = f"VARCHAR({type_len})"
        else:
            if dbg:
                type_sets.add(data_type)

        table = tables[table_name]
        col = {
            "col_name": column_name,
            "type": {
                "pg": data_type.upper()
            }
        }
        if is_nullable == 'NO':
            col['attribute'] = 'NOT NULL'
        table['cols'].append(col)

    for table, value in tables.items():
        keys = get_pg_table_constraints(table, schema_name)
        if len(keys['primary_keys']) != 0:
            value['primary_key'] = keys['primary_keys']
        for fk_name, fk_info in keys['foreign_keys'].items():
            assert len(fk_info['columns']) == 1
            foreign_keys.append(
                {
                    "FK_table": table,
                    "FK_col": fk_info['columns'][0],
                    "REF_table": fk_info['referenced_table'],
                    "REF_col": fk_info['referenced_columns'][0]
                }
            )
    res = []
    for name, table in tables.items():
        res.append(table)
    for f_key in foreign_keys:
        res.append(f_key)
    return res



def pg_schema_fetch_ddl(ddl_sql: str):
    dialect = 'pg'
    node, _, _, _ = parse_tree(ddl_sql, dialect)
    node = TreeNode.make_g4_tree_by_node(node, dialect)
    table_names = try_fetch_nodes_by_route(node, ['root', 'stmtblock', 'stmtmulti',
                                                  'stmt', 'createstmt', 'qualified_name'])
    assert len(table_names) == 1
    table_name = strip_quote(str(table_names[0]), '`')
    col_def_nodes = try_fetch_nodes_by_route(node, ['root', 'stmtblock', 'stmtmulti',
                                                    'stmt', 'createstmt', 'opttableelementlist', 'tableelementlist',
                                                    'tableelement'])

    primary_key = []
    cols = []
    for node in col_def_nodes:
        if node.get_child_by_value('tableconstraint') is not None:
            table_constraint_nodes = node.get_children_by_value('tableconstraint')
            for table_constraint_node in table_constraint_nodes:
                cons_ele_node = table_constraint_node.get_child_by_value('constraintelem')
                if 'PRIMARY KEY' in str(cons_ele_node):
                    column_list_nodes = try_fetch_nodes_by_route(cons_ele_node,
                                                                 ['constraintelem', 'columnlist', 'columnElem'])
                    for node in column_list_nodes:
                        primary_key.append(strip_quote(str(node), '"'))
            continue
        col_def_node = node.get_child_by_value('columnDef')
        col_name_node = col_def_node.get_child_by_value('colid')
        data_type_node = col_def_node.get_child_by_value('typename').get_child_by_value('simpletypename')
        constraint_nodes = try_fetch_nodes_by_route(col_def_node, ['colquallist', 'colconstraint'])
        not_null = False
        for constraint in constraint_nodes:
            assert isinstance(constraint, TreeNode)
            if 'PRIMARY KEY' in str(constraint):
                primary_key.append(str(col_name_node))
            elif "NOT NULL" in str(constraint):
                not_null = True
        if not_null:
            cols.append({
                "col_name": strip_quote(str(col_name_node), '"'),
                "type": {
                    "pg": str(data_type_node).upper(),
                },
                "attribute": [
                    "NOT NULL"
                ]
            })
        else:
            cols.append({
                "col_name": strip_quote(str(col_name_node), '"'),
                "type": {
                    "pg": str(data_type_node),
                }
            })
    if primary_key is not None:
        return {
            "table": table_name,
            "primary_key": primary_key,
            "cols": cols
        }
    else:
        return {
            "table": table_name,
            "cols": cols
        }


def oracle_schema_fetch_from_table():
    pass


def oracle_fetch_index_from_ddl(ddl_sql: str):
    dialect = 'oracle'
    node, _, _, _ = parse_tree(ddl_sql, dialect)
    node = TreeNode.make_g4_tree_by_node(node, dialect)
    index_clause_nodes = try_fetch_nodes_by_route(node, ['sql_script', 'unit_statement', 'create_index', 'table_index_clause'])
    table_name = index_clause_nodes[0].get_child_by_value('tableview_name')
    col_name = index_clause_nodes[0].get_child_by_value('index_expr')
    print(f"index_table: {table_name}, index_col: {col_name}")
    return {
        "index_tbl": str(table_name),
        "index_col": str(col_name)
    }



def oracle_schema_fetch_from_ddl(ddl_sql: str):
    dialect = 'oracle'
    node, _, _, _ = parse_tree(ddl_sql, dialect)
    node = TreeNode.make_g4_tree_by_node(node, dialect)
    table_names = try_fetch_nodes_by_route(node, ['sql_script', 'unit_statement', 'create_table',
                                                  'table_name'])
    assert len(table_names) == 1
    table_name = strip_quote(str(table_names[0]), '"')
    col_def_nodes = try_fetch_nodes_by_route(node, ['sql_script', 'unit_statement', 'create_table',
                                                    'relational_table', 'relational_property'])

    primary_key = []
    cols = []
    for node in col_def_nodes:
        if node.get_child_by_value('out_of_line_constraint') is not None:
            table_constraint_node = node.get_child_by_value('out_of_line_constraint')
            if 'PRIMARY KEY' in str(table_constraint_node):
                cons_col_name_nodes = table_constraint_node.get_children_by_value('column_name')
                for cons_col_name_node in cons_col_name_nodes:
                    primary_key.append(strip_quote(str(cons_col_name_node), '"'))
            continue
        col_def_node = node.get_child_by_value('column_definition')
        col_name_node = col_def_node.get_child_by_value('column_name')
        if str(col_name_node).lower() == 'constraint':
            continue
        data_type_node = col_def_node.get_child_by_value('datatype')
        if data_type_node is None:
            data_type_node = col_def_node.get_child_by_value('regular_id')
            assert data_type_node is not None
        auto_gen = False
        if col_def_node.get_child_by_value('identity_clause') is not None:
            auto_gen = True
        constraint_nodes = try_fetch_nodes_by_route(col_def_node, ['column_definition', 'inline_constraint'])
        not_null = False
        for constraint in constraint_nodes:
            assert isinstance(constraint, TreeNode)
            if 'PRIMARY KEY' in str(constraint):
                primary_key.append(str(col_name_node))
            elif "NOT NULL" in str(constraint):
                not_null = True
        attribute = []
        if not_null:
            attribute.append('NOT NULL')
        if auto_gen:
            attribute.append('AUTO_INCREMENT')
        column = {
                "col_name": strip_quote(str(col_name_node), '"'),
                "type": {
                    "oracle": str(data_type_node).upper(),
                }
            }
        if len(attribute) > 0:
            column['attribute'] = attribute
        cols.append(column)
    if len(primary_key) > 0:
        return {
            "table": table_name,
            "primary_key": primary_key,
            "cols": cols
        }
    else:
        return {
            "table": table_name,
            "cols": cols
        }


def fetch_args(args: str) -> list:
    res = []
    for arg in args.split(','):
        res.append(int(arg))
    return res


def fetch_arg_strs_in_paren(args) -> str:
    return args[args.find('(') + 1: args.find(')')].strip()


def type_mapping(partial_schema, src_dialect):
    types = set()
    for table in partial_schema:
        for col in table['cols']:
            type = col['type'][src_dialect]
            assert isinstance(type, str)
            types.add(type)
            if src_dialect == 'mysql':
                pass
            elif src_dialect == 'pg':
                pass
            elif src_dialect == 'oracle':
                if type == 'SDO_GEOMETRY':
                    col['type']['mysql'] = 'POINT'
                    col['type']['pg'] = 'GEOMETRY'
                elif type.startswith('VARCHAR2'):
                    arg = fetch_args(fetch_arg_strs_in_paren(type))[0]
                    col['type']['mysql'] = f'VARCHAR({arg})'
                    col['type']['pg'] = f'VARCHAR({arg})'
                elif type.startswith('CHAR'):
                    arg = fetch_args(fetch_arg_strs_in_paren(type))[0]
                    col['type']['mysql'] = f'CHAR({arg})'
                    col['type']['pg'] = f'CHAR({arg})'
                elif type.startswith('NVARCHAR2'):
                    arg = fetch_args(fetch_arg_strs_in_paren(type))[0]
                    col['type']['mysql'] = f'VARCHAR({arg})'
                    col['type']['pg'] = f'VARCHAR({arg}) CHARACTER SET utf8mb4'
                elif type.startswith('NUMBER'):
                    if type == 'NUMBER':
                        col['type']['mysql'] = 'INT'
                        col['type']['pg'] = 'INT'
                    else:
                        args = fetch_args(fetch_arg_strs_in_paren(type))
                        if len(args) == 1:
                            col['type']['mysql'] = 'INT'
                            col['type']['pg'] = 'INT'
                        else:
                            assert len(args) == 2
                            col['type']['mysql'] = f'DECIMAL({args[0]},{args[1]})'
                            col['type']['pg'] = f'DECIMAL({args[0]},{args[1]})'
                elif type == 'XMLTYPE':
                    col['type']['mysql'] = 'TEXT'
                    col['type']['pg'] = 'XML'
                elif type == 'TIMESTAMP WITH TIME ZONE':
                    col['type']['mysql'] = 'TIMESTAMP'
                    col['type']['pg'] = 'TIMESTAMP WITH TIME ZONE'
                elif type == 'JSON':
                    col['type']['mysql'] = 'JSON'
                    col['type']['pg'] = 'JSON'
                elif type == 'INTERVAL YEAR TO MONTH':
                    col['type']['mysql'] = 'Unsupported'
                    col['type']['pg'] = 'INTERVAL YEAR TO MONTH'
                elif type == 'DATE':
                    col['type']['mysql'] = 'DATE'
                    col['type']['pg'] = 'DATE'
                elif type == 'TIMESTAMP':
                    col['type']['mysql'] = 'TIMESTAMP'
                    col['type']['pg'] = 'TIMESTAMP'
                elif type == 'BLOB':
                    col['type']['mysql'] = 'BLOB'
                    col['type']['pg'] = 'BYTEA'
                else:
                    print(type)
            else:
                assert False
    return partial_schema


def bird_schema_compare(schema1, schema2):
    col_rename_mapping = {}
    for table1 in schema1:
        if 'FK_table' in table1:
            continue
        for table2 in schema2:
            if 'FK_table' in table2:
                continue
            if table2['table'].upper() == table1['table'].upper():
                if table2['table'] != table1['table']:
                    print(f"table1: {table1['table']}, table2: {table2['table']}")
            else:
                continue
            if len(table1['cols']) != len(table2['cols']):
                raise ValueError
            for col1 in table1['cols']:
                for col2 in table2['cols']:
                    if col1['col_name'].upper() == col2['col_name'].upper():
                        if col1['col_name'] != col2['col_name']:
                            col_rename_mapping[col2['col_name']] = col1['col_name']
                            col2['col_name'] = col1['col_name']
                        if col1['type']['mysql'] == 'INT':
                            col2['type']['pg'] = 'INT'
                        elif col1['type']['mysql'] == 'DOUBLE':
                            col2['type']['pg'] = 'DOUBLE PRECISION'
                        elif col1['type']['mysql'] == 'VARCHAR(256)':
                            col2['type']['pg'] = 'VARCHAR(256)'
                        elif col1['type']['mysql'] == 'VARCHAR(255)':
                            col2['type']['pg'] = 'VARCHAR(255)'
                        elif col1['type']['mysql'] == 'DATETIME':
                            col2['type']['pg'] = 'TIMESTAMP'
                        else:
                            pass
                        if col1['col_name'].upper() != col2['col_name'].upper():
                            pass
                            # print(f"mysql_type: {col1['type']['mysql']}, pg_type: {col2['type']['pg']}")
    for table1 in schema1:
        if 'FK_table' in table1:
            flag = False
            for table2 in schema2:
                if 'FK_table' not in table2:
                    continue
                if (table2['FK_table'].upper() == table1['FK_table'].upper() and table2['FK_col'].upper() == table1['FK_col'].upper()
                        and table2['REF_table'].upper() == table1['REF_table'].upper() and
                        table2['REF_col'].upper() == table1['REF_col'].upper()):
                    flag = True
            if not flag:
                print(f"mysql_FK: table: {table1['FK_table']}, col: {table1['FK_col']}")
    for table2 in schema2:
        if 'FK_table' in table2:
            flag = False
            for table1 in schema1:
                if 'FK_table' not in table1:
                    continue
                if (table2['FK_table'].upper() == table1['FK_table'].upper() and table2['FK_col'].upper() == table1['FK_col'].upper()
                        and table2['REF_table'].upper() == table1['REF_table'].upper() and
                        table2['REF_col'].upper() == table1['REF_col'].upper()):
                    flag = True
            if not flag:
                print(f"pg_FK: table: {table2['FK_table']}, col: {table2['FK_col']}")
    with open('D:\\Coding\\SQL2SQL_Bench\\backup\\bird_ddl.json', 'w') as file:
        json.dump(schema1, file, indent=4)


def build_data(schema_path, data_path, dialect):
    with open(schema_path, 'r') as file:
        schema = json.load(file)
    table_mapping = {}
    table_values = {}
    for ele in schema:
        if 'table' in ele:
            table_name = ele['table']
            cols = ele['cols']
            table_mapping[table_name] = cols
    with open(data_path, 'r') as file:
        sqls = file.read().split(';')
        for sql in sqls:
            used_sql = sql.strip()
            if sql == '':
                continue
            table_name, cols, values = extract_values_from_insert(used_sql, dialect)
            value = []
            if table_name not in table_values:
                table_values[table_name] = []
            if table_name not in table_mapping:
                raise Exception(f"Table {table_name} not in schema")
            for i in range(len(cols)):
                col_info_node = None
                for col in table_mapping[table_name]:
                    if col["col_name"].upper() == cols[i].upper():
                        col_info_node = col
                        break
                col_type = col_info_node['type'][dialect]
                col_value = values[i]
                format = None
                if dialect == 'oracle':
                    if col_type == 'TIMESTAMP':
                        params = extract_parameters(col_value)
                        col_value = params[0].strip('\'')
                        format = params[1].strip('\'')
                    elif col_type == 'JSON':
                        print(extract_parameters(col_value)[0])
                        json_value = extract_parameters(col_value)[0].strip('\'')
                        print(json_value)
                        content = json.loads(json_value)
                        col_value = content
                    else:
                        pass
                value.append({
                    'col': cols[i],
                    'col_value': col_value,
                    "col_format": format,
                })
            table_values[table_name].append(value)
    res = []
    for tbl, tbl_values in table_values.items():
        res.append({
            "table": tbl,
            "values": tbl_values,
        })
    with open('D:\\Coding\\SQL2SQL_Bench\\backup\\data.json', 'w') as file:
        json.dump(res, file)

build_data("D:\\Coding\\SQL2SQL_Bench\\data\\oracle_customer_order\\schema.json", "D:\\Coding\\SQL2SQL_Bench\\backup\\test.sql", "oracle")