# -*- coding: utf-8 -*-
# @Project: sql2sqlBench
# @Module: get_type$
# @Author: 10379
# @Time: 2024/12/6 12:59
from typing import Dict

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from utils.db_connector import *
from utils.tools import dialect_judge

# TODO: With Recursive has to be solved
def get_type(obj: str, dialect: str, db_name, is_table: bool) -> tuple[bool, list]:
    dialect_type = dialect_judge(dialect)
    match dialect_type:
        case 'mysql':
            return get_mysql_type(obj, db_name, is_table)
        case 'postgres':
            return get_pg_type(obj, db_name, is_table)
        case 'oracle':
            return get_oracle_type(obj, db_name, is_table)
        case _:
            assert False


def get_cte_type(cte: str, recursive: bool, db_name: str, dialect: str):
    dialect_type = dialect_judge(dialect)
    if recursive:
        sql = f"WITH RECURSIVE cte {cte} SELECT * FROM cte"
    else:
        sql = f"WITH cte {cte} SELECT * FROM cte"
    match dialect_type:
        case 'mysql':
            return get_mysql_type(sql, db_name, False)
        case 'postgres':
            return get_pg_type(sql, db_name, False)
        case 'oracle':
            return get_oracle_type(sql, db_name, False)
        case _:
            assert False


def get_usable_cols(db_name, sql: str, dialect: str):
    try:
        root_node, line, col, msg = parse_tree(sql, dialect)

        if root_node is None:
            raise ValueError(f"Parse error when executing ANTLR parser of {dialect}.\n"
                             f"The sql is {sql}")
        root_node = TreeNode.make_g4_tree_by_node(root_node, dialect)
    except ValueError as ve:
        raise ve
    dialect_type = dialect_judge(dialect)
    match dialect_type:
        case 'mysql':
            return get_mysql_usable_cols(db_name, root_node)
        case 'postgres':
            return get_pg_usable_cols(db_name, root_node)
        case 'oracle':
            return get_oracle_usable_cols(db_name, root_node)
        case _:
            assert False


def get_mysql_usable_cols(db_name, node: TreeNode) -> List:
    # find all the sub_query, with_query, and common table
    while (node.value != 'sqlStatements'
           and node.value != 'selectStatement' and node.value != 'dmlStatement'):
        node = node.children[0]
    if node.value == 'sqlStatements' and len(node.children) == 2:
        # have withStatement
        with_statement_node = node.children[0]
        sql_statement_node = node.children[1]
    else:
        with_statement_node = None
        sql_statement_node = node.children[0]
    cte_def = get_mysql_cte_def(db_name, with_statement_node)
    node_queue = [sql_statement_node]
    while True:
        specification_node = node_queue[0]
        node_queue.pop(0)
        if specification_node.value == 'querySpecification' or specification_node.value != 'querySpecificationNointo':
            break
        else:
            for child in specification_node.children:
                node_queue.append(child)
    from_node = None
    for child in specification_node.children:
        if child.value == 'fromClause':
            from_node = child
    if from_node is None:
        return []
    else:
        assert isinstance(from_node, TreeNode)
        table_sources = from_node.get_children_by_value('tableSources')
        return get_mysql_all_used_sources(db_name, cte_def, table_sources)


def get_mysql_all_used_sources(db_name: str, cte_def: List[Dict], table_sources_node: TreeNode) -> List:
    table_sources = table_sources_node.get_children_by_value('tableSource')
    res = []
    for table_source in table_sources:
        assert isinstance(table_source, TreeNode)
        res = res + get_mysql_table_source_item(db_name, cte_def,
                                                table_source.get_children_by_value('tableSourceItem')[0])
        join_parts = table_source.get_children_by_value('joinPart')
        for join_part in join_parts:
            assert isinstance(join_part, TreeNode)
            res = res + get_mysql_table_source_item(db_name, cte_def,
                                                    join_part.get_children_by_value('tableSourceItem')[0])
    return res


def get_mysql_table_source_item(db_name: str, cte_def: List[Dict], table_source_item: TreeNode) -> List:
    if len(table_source_item.get_children_by_value('tableName')) > 0:
        table_node = table_source_item.get_children_by_value('tableName')[0]
        table_name = str(table_node)
        for item in cte_def:
            assert isinstance(table_node, TreeNode)
            if item['tbl_name'] == str(table_node):
                return [item]
        flag, temp_res = get_type(table_name, 'mysql', db_name, True)
        if not flag:
            raise ValueError(f"table {table_name} is not find in {db_name}")
        table = {
            "tbl_name": table_name,
            "cols": []
        }
        for item in temp_res:
            table['cols'].append({
                "col": item['col'],
                "type": item['type']
            })
        return [table]
    elif len(table_source_item.get_children_by_value('selectStatement')) > 0:
        flag, types = get_type(str(table_source_item.get_child_by_value('selectStatement')), 'mysql', db_name, False)
        name = str(table_source_item.get_child_by_value('uid'))
        if not flag:
            raise ValueError(f"get type of sql: {str(table_source_item.get_child_by_value('selectStatement'))} Failed")
        return [
            {
                "tbl_name": name,
                "cols": types
            }
        ]
    else:
        assert len(table_source_item.get_children_by_value('tableSources')) > 0
        return get_mysql_all_used_sources(db_name, cte_def,
                                          table_source_item.get_children_by_value('tableSources')[0])


def get_mysql_cte_def(db_name: str, with_statement_node: TreeNode) -> List[Dict]:
    if with_statement_node is None:
        return []
    ctes = with_statement_node.get_children_by_value('commonTableExpressions')
    res = []
    for cte in ctes:
        res = res + get_mysql_cte(db_name, cte)
    return res


def get_mysql_cte(db_name, cte_node: TreeNode) -> List:
    cte_name_node = cte_node.get_child_by_value('cteName')
    name = str(cte_name_node)
    column_names = [str(item) for item in cte_node.get_children_by_value('cteColumnName')]
    flag, types = get_type(str(cte_node.get_child_by_value('dmlStatement')), 'mysql', db_name, False)
    if not flag:
        raise ValueError(f"get type of sql: {str(cte_node.get_child_by_value('dmlStatement'))}")
    if len(column_names) > 0:
        assert len(types) == len(column_names)
        table = {
            "tbl_name": name,
            "cols": []
        }
        for i in range(len(types)):
            table['cols'].append({
                "col": column_names[i],
                "type": types[i]['type']
            })
    else:
        table = {
            "tbl_name": name,
            "cols": types
        }
    res = [table]

    if cte_node.get_children_by_value('commonTableExpressions') > 0:
        for child in cte_node.get_children_by_value('commonTableExpressions'):
            res = res + get_mysql_cte(db_name, child)
    return res


def get_pg_usable_cols(db_name: str, node: TreeNode):
    queue = []
    queue.append(node)
    while True:
        head_node = queue[0]
        queue.pop(0)
        if head_node.value == 'select_no_parens':
            break
        else:
            for child in head_node.children:
                queue.append(child)
    if len(head_node.get_children_by_value('with_clause')) > 0:
        with_node = head_node.get_children_by_value('with_clause')[0]
        select_node = head_node.get_children_by_value('select_clause')[0]
    else:
        with_node = None
        select_node = head_node.get_children_by_value('select_clause')[0]
    cte_defs = get_pg_cte_def(db_name, with_node)
    select_clause_node = (select_node.
                          get_child_by_value('simple_select_intersect').
                          get_child_by_value('simple_select_pramary'))
    if len(select_clause_node.get_child_by_value('from_clause')) is None:
        assert len(select_clause_node.get_children_by_value('select_with_parens')) > 0
        return get_pg_usable_cols(db_name, select_clause_node.get_children_by_value('select_with_parens'))
    else:
        from_clause_node = select_clause_node.get_child_by_value('from_clause')
        if from_clause_node.get_child_by_value('non_ansi_join') is not None:
            table_refs = from_clause_node.get_child_by_value('non_ansi_join').get_children_by_value('table_ref')
        else:
            table_refs = from_clause_node.get_children_value('table_ref')
        res = []
        for table_ref_node in table_refs:
            assert isinstance(table_ref_node, TreeNode)
            res = res + get_pg_table_ref(db_name, table_ref_node)
        return res


def get_pg_table_ref(db_name: str, table_ref_node: TreeNode) -> List[Dict]:

    pass


def get_pg_cte_def(db_name: str, with_statement_node: TreeNode) -> List[Dict]:
    if with_statement_node is None:
        return []
    ctes = with_statement_node.get_child_by_value('cte_list').get_children_by_value('common_table_expr')
    res = []
    for cte in ctes:
        res = res + get_pg_cte(db_name, cte)
    return res


def get_pg_cte(db_name, cte_node: TreeNode):
    name = str(cte_node.get_child_by_value('name'))
    column_nodes = cte_node.get_child_by_value('opt_name_list')
    if column_nodes is not None:
        name_list = (column_nodes.get_child_by_value('name_list')
                     .get_children_by_value('name'))
        name_list = [str(node) for node in name_list]
    else:
        name_list = []
    flag, types = get_type(str(cte_node.get_child_by_value('preparablestmt')), 'postgres', db_name, False)
    if not flag:
        raise ValueError(f"get type of sql: {str(cte_node.get_child_by_value('preparablestmt'))}")
    if len(name_list) > 0:
        assert len(types) == len(name_list)
        table = {
            "tbl_name": name,
            "cols": []
        }
        for i in range(len(types)):
            table['cols'].append({
                "col": name_list[i],
                "type": types[i]['type']
            })
    else:
        table = {
            "tbl_name": name,
            "cols": types
        }
    return [table]


def get_oracle_usable_cols(db_name: str, node: TreeNode):
    return None
