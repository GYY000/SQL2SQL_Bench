# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: mysql_tree$
# @Author: 10379
# @Time: 2025/4/2 20:34
from antlr_parser.Tree import TreeNode
from utils.tools import get_table_col_name


# used for analyze mysql tree structure

def get_select_statement_node_from_root(root_node: TreeNode):
    node = root_node.get_child_by_value('sqlStatements')
    assert isinstance(node, TreeNode)
    select_statement_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                            'dmlStatement', 'selectStatement'])
    assert len(select_statement_node) == 1
    return select_statement_node[0]


def fetch_main_select_from_select_stmt_mysql(select_statement_node: TreeNode):
    node = select_statement_node
    if node.get_child_by_value('querySpecificationNointo') is not None:
        node = select_statement_node.get_child_by_value('querySpecificationNointo')
    elif select_statement_node.get_child_by_value('querySpecification') is not None:
        node = select_statement_node.get_child_by_value('querySpecification')
    elif node.get_child_by_value('queryExpressionNointo') is not None:
        node = select_statement_node.get_child_by_value('queryExpressionNointo')
        while node.get_child_by_value('querySpecificationNointo') is None:
            node = node.get_child_by_value('queryExpressionNointo')
            assert node is not None
        node = node.get_child_by_value('querySpecificationNointo')
    elif node.get_child_by_value('queryExpression') is not None:
        node = node.get_child_by_value('queryExpression')
        while node.get_child_by_value('querySpecification') is None:
            node = node.get_child_by_value('queryExpression')
            assert node is not None
        node = node.get_child_by_value('querySpecification')
    assert node.value == 'querySpecificationNointo' or node.value == 'querySpecification'
    return node


def fetch_all_simple_select_from_select_stmt_mysql(select_statement_node: TreeNode):
    node = select_statement_node
    nodes = node.find_all_nodes_of_values(['querySpecification', 'queryExpression',
                                           'querySpecificationNointo', 'queryExpressionNointo', 'unionStatement'])
    res = []
    for node in nodes:
        if node.value == 'querySpecificationNointo' or node.value == 'querySpecification':
            res.append(node)
        elif node.value == 'queryExpressionNointo':
            while node.get_child_by_value('querySpecificationNointo') is None:
                node = node.get_child_by_value('queryExpressionNointo')
            node = node.get_child_by_value('querySpecificationNointo')
            res.append(node)
        elif node.value == 'queryExpression':
            while node.get_child_by_value('querySpecification') is None:
                node = node.get_child_by_value('queryExpression')
            node = node.get_child_by_value('querySpecification')
            res.append(node)
        elif node.value == 'unionStatement':
            while node.get_child_by_value('querySpecificationNointo') is None:
                node = node.get_child_by_value('queryExpressionNointo')
            node = node.get_child_by_value('querySpecificationNointo')
            res.append(node)
        assert node.value == 'querySpecificationNointo' or node.value == 'querySpecification'
    return res


def rename_column_mysql(select_element_node: TreeNode, name_dict: dict, extend_name=None):
    if extend_name is None:
        extend_name = 'col'
    idx = 0
    while f"{extend_name.lower()}_{idx}" in name_dict:
        idx += 1
    name_dict[f"{extend_name.lower()}_{idx}"] = 1
    if select_element_node.get_child_by_value('uid') is not None:
        uid_node = select_element_node.get_child_by_value('uid')
        new_name_node = TreeNode(f"{extend_name.lower()}_{idx}", 'mysql', True)
        assert isinstance(uid_node, TreeNode)
        uid_node.children = [new_name_node]
    else:
        select_element_node.add_child(TreeNode('AS', 'mysql', True))
        select_element_node.add_child(TreeNode(f"{extend_name.lower()}_{idx}", 'mysql', True))
    return f"{extend_name}_{idx}"


def get_names_from_uid_lists(uid_list_node: TreeNode):
    uid_nodes = uid_list_node.get_children_by_value('uid')
    res = []
    for uid_node in uid_nodes:
        assert isinstance(uid_node, TreeNode)
        res.append(str(uid_node).strip('`'))
    return res


def rename_sql_mysql(select_stmt_node: TreeNode):
    res = []
    name_dict = {}
    select_main_node = fetch_main_select_from_select_stmt_mysql(select_stmt_node)
    select_elements = select_main_node.get_children_by_path(['selectElements', 'selectElement'])
    for select_element_node in select_elements:
        if select_element_node.get_child_by_value('functionCall') is not None or \
                select_element_node.get_child_by_value('expression') is not None:
            if select_element_node.get_child_by_value('uid') is None:
                col_name = rename_column_mysql(select_element_node, name_dict)
            else:
                col_name = str(select_element_node.get_child_by_value('uid')).strip('`')
                if col_name in name_dict:
                    col_name = rename_column_mysql(select_element_node, name_dict, col_name)
                else:
                    name_dict[col_name] = 1
            res.append(col_name)
        else:
            if select_element_node.get_child_by_value('uid') is not None:
                col_name = str(select_element_node.get_child_by_value('uid')).strip('`')
            elif select_element_node.get_child_by_value('fullId') is not None:
                raise ValueError('* is not Support yet')
            else:
                full_name_node = select_element_node.get_child_by_value('fullColumnName')
                if full_name_node is None:
                    assert False
                dotted_ids = full_name_node.get_children_by_value('dottedId')
                if len(dotted_ids) > 0:
                    dotted_id_node = dotted_ids[-1]
                    col_name = str(dotted_id_node).strip('.').strip('`')
                else:
                    col_name = str(full_name_node).strip('`')
            if col_name in name_dict:
                col_name = rename_column_mysql(select_element_node, name_dict, col_name)
            else:
                name_dict[col_name] = 1
            res.append(col_name)
    return res


def analyze_mysql_table_sources(table_sources_node: TreeNode, dialect: str, name_dict: dict):
    res = []
    for table_source_node in table_sources_node.get_children_by_path(['tableSource']):
        res = res + analyze_mysql_table_source(table_source_node, dialect, name_dict)
    return res


def analyze_mysql_table_source(table_source_node: TreeNode, dialect: str, name_dict: dict):
    table_source_item_node = table_source_node.get_child_by_value('tableSourceItem')
    assert table_source_item_node is not None  # TODO: haven't consider json table now
    res = []
    res = res + analyze_mysql_table_source_item(table_source_item_node, dialect, name_dict)
    for join_part_node in table_source_node.get_children_by_path(['joinPart']):
        res = res + analyze_mysql_join_part(join_part_node, dialect, name_dict)
    return res


def analyze_mysql_table_source_item(table_source_item_node: TreeNode, dialect: str, name_dict: dict):
    if table_source_item_node.get_child_by_value('LATERAL') is not None:
        lateral_flag = True
    else:
        lateral_flag = False
    if table_source_item_node.get_child_by_value('tableSources') is not None:
        return analyze_mysql_table_sources(table_source_item_node.get_child_by_value('tableSources'), dialect,
                                           name_dict)
    elif table_source_item_node.get_child_by_value('dmlStatement') is not None:
        dml_statement_node = table_source_item_node.get_child_by_value('dmlStatement')
        assert isinstance(dml_statement_node, TreeNode)
        assert dml_statement_node.get_child_by_value('selectStatement') is not None
        if table_source_item_node.get_child_by_value('uid') is not None:
            table_name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
        else:
            extension_name = 'subquery'
            if extension_name in name_dict:
                id = name_dict[extension_name]
                name_dict[extension_name] += 1
                table_name = extension_name + '_' + str(id)
            else:
                name_dict[extension_name] = 1
                table_name = extension_name
            new_uid_node = TreeNode('uid', 'mysql', False)
            new_uid_node.add_child(TreeNode(get_table_col_name(table_name, 'mysql').lower(), 'mysql', True))
            table_source_item_node.add_child(new_uid_node)
        rename_flag = False
        if table_source_item_node.get_child_by_value('uidList') is not None:
            column_names = get_names_from_uid_lists(table_source_item_node.get_child_by_value('uidList'))
        else:
            column_names = rename_sql_mysql(dml_statement_node.get_child_by_value('selectStatement'))
        return [{
            "type": "subquery",
            "name": table_name,
            "column_names": column_names,
            "sub_query_node": dml_statement_node.get_child_by_value('selectStatement'),
            "lateral": lateral_flag,  # if it's a lateral subquery
            "rename_flag": rename_flag
        }]
    else:
        table_name = str(table_source_item_node.get_child_by_value('tableName')).strip('`')
        column_names = None
        final_name = None
        if table_source_item_node.get_child_by_value('uidList') is not None:
            column_names = get_names_from_uid_lists(table_source_item_node.get_child_by_value('uidList'))
        if table_source_item_node.get_child_by_value('uid') is not None:
            table_alias_name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
            if table_alias_name in name_dict:
                extension_name = table_alias_name + '_' + str(name_dict[table_alias_name])
                name_dict[table_alias_name] += 1
                final_name = extension_name
                uid_node = table_source_item_node.get_child_by_value('uid')
                assert isinstance(uid_node, TreeNode)
                assert len(uid_node.children) == 1
                assert isinstance(uid_node.children[0], TreeNode)
                uid_node.children[0].value = get_table_col_name(extension_name, 'mysql').lower()
                uid_node.children[0].is_terminal = True
            else:
                name_dict[table_alias_name] = 1
                final_name = table_alias_name
        else:
            if table_name in name_dict:
                extension_name = table_name + '_' + str(name_dict[table_name])
                name_dict[table_name] += 1
                final_name = extension_name
                new_uid_node = TreeNode('uid', 'mysql', False)
                new_uid_node.add_child(TreeNode(get_table_col_name(final_name, 'mysql').lower(), 'mysql', True))
                table_source_item_node.add_child(new_uid_node)
            else:
                name_dict[table_name] = 1
        return [
            {
                "type": "table",
                "name": table_name,
                'alias': final_name,
                "column_names": column_names
            }
        ]


def analyze_mysql_join_part(join_part_node: TreeNode, dialect: str, name_dict: dict):
    table_source_item_node = join_part_node.get_child_by_value('tableSourceItem')
    assert table_source_item_node is not None
    return analyze_mysql_table_source_item(table_source_item_node, dialect, name_dict)
