# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: fetch_operand_type$
# @Author: 10379
# @Time: 2025/5/17 12:56
from antlr_parser.Tree import TreeNode
from antlr_parser.mysql_tree import fetch_main_select_from_select_stmt_mysql, get_select_statement_node_from_root
from antlr_parser.oracle_tree import fetch_main_select_from_subquery_oracle
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import fetch_main_select_from_select_stmt_pg
from sql_gen.generator.ele_type.type_conversion import type_mapping
from utils.db_connector import get_mysql_type, get_pg_type, get_oracle_type


def fetch_with_ctes(selectStatement: TreeNode, dialect: str):
    cur_node = selectStatement.father
    ctes = []
    if dialect == 'mysql':
        while cur_node is not None:
            assert isinstance(cur_node, TreeNode)
            if cur_node.value == 'sqlStatements':
                temp_cte_nodes = cur_node.get_children_by_path(['sqlStatement', 'dmlStatement',
                                                                'withStatement', 'commonTableExpression'])
            else:
                temp_cte_nodes = cur_node.get_children_by_path(['dmlStatement', 'withStatement',
                                                                'commonTableExpression'])
            ctes = temp_cte_nodes + ctes
            cur_node = cur_node.father
    elif dialect == 'pg':
        while cur_node is not None:
            assert isinstance(cur_node, TreeNode)
            with_stmt_nodes = cur_node.get_children_by_path(['with_clause', 'cte_list', 'common_table_expr'])
            ctes = with_stmt_nodes + ctes
            cur_node = cur_node.father
    else:
        while cur_node is not None:
            assert isinstance(cur_node, TreeNode)
            with_stmt_nodes = cur_node.get_children_by_path(['with_clause', 'with_factoring_clause'])
            ctes = with_stmt_nodes + ctes
            cur_node = cur_node.father
    return ctes


def fetch_operand_type(db_name, sql_root_node: TreeNode, operand: TreeNode, dialect: str):
    new_operand_node = TreeNode(str(operand), operand.dialect, True)
    if dialect == 'mysql':
        assert sql_root_node.value == 'selectStatement'
        main_select_node = fetch_main_select_from_select_stmt_mysql(sql_root_node)
        ori_select_elements_node = main_select_node.get_child_by_value('selectElements')
        main_select_node.replace_child(ori_select_elements_node, new_operand_node)
        # remove order by


        order_by_node = main_select_node.get_child_by_value('orderByClause')
        if order_by_node is not None:
            empty_node = TreeNode('', dialect, True)
            main_select_node.replace_child(order_by_node, empty_node)
        with_clauses = fetch_with_ctes(sql_root_node, dialect)
        if len(with_clauses) > 0:
            ctes = "WITH RECURSIVE "
            for with_clause in with_clauses:
                ctes += str(with_clause) + "\n\t"
            sql = ctes + str(main_select_node)
        else:
            sql = str(main_select_node)
        flag, res = get_mysql_type(sql, db_name, False)
        if not flag:
            print("Error in fetching operand type")
        main_select_node.replace_child(new_operand_node, ori_select_elements_node)
        if order_by_node is not None:
            main_select_node.replace_child(empty_node, order_by_node)
        return type_mapping('mysql', res[0]['type'])
    elif dialect == 'pg':
        assert sql_root_node.value == 'simple_select_pramary'
        main_select_node = fetch_main_select_from_select_stmt_pg(sql_root_node)
        target_list_node = main_select_node.get_child_by_value('opt_target_list')
        if target_list_node is None:
            target_list_node = main_select_node.get_child_by_value('target_list')
        main_select_node.replace_child(target_list_node, new_operand_node)
        with_clauses = fetch_with_ctes(sql_root_node, dialect)
        if len(with_clauses) > 0:
            ctes = "WITH RECURSIVE "
            for with_clause in with_clauses:
                ctes += str(with_clause) + "\n\t"
            sql = ctes + str(main_select_node)
        else:
            sql = str(main_select_node)
        flag, res = get_pg_type(sql, db_name, False)
        if not flag:
            print("Error in fetching operand type")
        main_select_node.replace_child(new_operand_node, target_list_node)
        return type_mapping('pg', res[0]['type'])
    else:
        assert dialect == 'oracle'
        assert sql_root_node.value == 'query_block'
        target_list_node = sql_root_node.get_children_by_path(['selected_list'])
        sql_root_node.replace_child(target_list_node, new_operand_node)

        order_by_node = sql_root_node.get_child_by_value('order_by_clause')
        if order_by_node is not None:
            empty_node = TreeNode('', dialect, True)
            sql_root_node.replace_child(order_by_node, empty_node)

        with_clauses = fetch_with_ctes(sql_root_node, dialect)
        if len(with_clauses) > 0:
            ctes = "WITH RECURSIVE "
            for with_clause in with_clauses:
                ctes += str(with_clause) + "\n\t"
            sql = ctes + str(sql_root_node)
        else:
            sql = str(sql_root_node)
        flag, res = get_oracle_type(sql, db_name, False)
        if not flag:
            print("Error in fetching operand type")
        sql_root_node.replace_child(new_operand_node, target_list_node)
        if order_by_node is not None:
            sql_root_node.replace_child(empty_node, order_by_node)
        return type_mapping('oracle', res[0]['type'])






sql = "WITH cte AS (SELECT CAST('2023-10-10' AS DATE) cola, 1 AS colb FROM dual) SELECT cola FROM cte"
node, _, _, _ = parse_tree(sql, 'mysql')
assert node is not None
node = TreeNode.make_g4_tree_by_node(node, 'mysql')
fetch_operand_type("bird", get_select_statement_node_from_root(node), TreeNode('cola', 'mysql', True), 'mysql')
