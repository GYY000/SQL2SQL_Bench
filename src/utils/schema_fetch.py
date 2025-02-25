import json

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree


def strip_quote(string: str, quote: str):
    while string.startswith(quote):
        string = string[1:]
    while string.endswith(quote):
        string = string[:len(string) - 1]
    return string


def try_fetch_nodes_by_route(root_node: TreeNode, path: list):
    if root_node.value == path[0]:
        if len(path) == 1:
            return [root_node]
        else:
            res = []
            for child in root_node.children:
                res = res + try_fetch_nodes_by_route(child, path[1:])
            return res
    else:
        return []


def mysql_schema_fetch(ddl_sql: str):
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
                    "mysql": str(data_type_node),
                },
                "attribute": [
                    "NOT NULL"
                ]
            })
        else:
            cols.append({
                "col_name": strip_quote(str(col_name_node), '`'),
                "type": {
                    "mysql": str(data_type_node),
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


def pg_schema_fetch(ddl_sql: str):
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
                    "pg": str(data_type_node),
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


def oracle_schema_fetch(ddl_sql: str):
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
        constraint_nodes = try_fetch_nodes_by_route(col_def_node, ['column_definition', 'inline_constraint'])
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
                    "oracle": str(data_type_node),
                },
                "attribute": [
                    "NOT NULL"
                ]
            })
        else:
            cols.append({
                "col_name": strip_quote(str(col_name_node), '"'),
                "type": {
                    "oracle": str(data_type_node),
                }
            })
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
                    pass
                elif type.startswith('NVARCHAR2'):
                    pass
                elif type.startswith('NUMBER'):
                    pass
                elif type == 'XMLTYPE':
                    pass
                elif type == 'TIMESTAMP WITH LOCAL TIME ZONE':
                    pass
                elif type == '':
                    pass
                else:
                    print(type)
            else:
                assert False
    # print(types)


with open('D:\\Coding\\SQL2SQL_Bench\\backup\\ora_schema.json', 'r') as file:
    schema = json.load(file)

    type_mapping(schema, 'oracle')
