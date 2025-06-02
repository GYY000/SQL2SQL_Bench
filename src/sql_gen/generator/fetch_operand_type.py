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
from sql_gen.generator.ele_type.type_def import BoolType
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


def fetch_operand_type(db_name, operand: TreeNode, dialect: str):
    new_operand_node = TreeNode(str(operand), operand.dialect, True)
    order_by_mode = False
    if dialect == 'mysql':
        while len(operand.children) == 1:
            operand = operand.children[0]
            if operand.value == 'predicate':
                if not(len(operand.children) == 1 and operand.children[0].value == 'expressionAtom'):
                    return BoolType()
        father_node = operand
        sql_root_node = None
        while father_node is not None:
            if father_node.value == 'orderByClause':
                order_by_mode = True
            if order_by_mode and father_node.value in ['queryExpressionNointo', 'queryExpression']:
                sql_root_node = fetch_main_select_from_select_stmt_mysql(father_node)
                break
            elif order_by_mode and father_node.value == 'selectStatement':
                sql_root_node = father_node
                break
            elif not order_by_mode:
                if father_node.value == 'querySpecificationNointo':
                    sql_root_node = father_node
                    break
                elif father_node.value == 'querySpecification':
                    sql_root_node = father_node
                    break
            father_node = father_node.father
        assert sql_root_node is not None
        if order_by_mode:
            with_clauses = fetch_with_ctes(sql_root_node, dialect)
            ctes = ''
            if len(with_clauses) > 0:
                ctes = "WITH RECURSIVE "
                for with_clause in with_clauses:
                    ctes += str(with_clause) + "\n\t"
            sql = f"{ctes}SELECT {str(operand)} FROM ({sql_root_node}) AS temp"
            flag, res = get_mysql_type(db_name, sql, False)
            if not flag:
                print(sql)
                print("Error in fetching operand type")
            return type_mapping('mysql', res[0]['type'])
        else:
            assert sql_root_node.value == 'querySpecification' or sql_root_node.value == 'querySpecificationNointo'
            ori_select_elements_node = sql_root_node.get_child_by_value('selectElements')
            sql_root_node.replace_child(ori_select_elements_node, new_operand_node)
            # remove order by
            order_by_node = sql_root_node.get_child_by_value('orderByClause')
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
            flag, res = get_mysql_type(db_name, sql, False)
            if not flag:
                print(sql)
                print("Error in fetching operand type")
            sql_root_node.replace_child(new_operand_node, ori_select_elements_node)
            if order_by_node is not None:
                sql_root_node.replace_child(empty_node, order_by_node)
            print(res)
            return type_mapping('mysql', res[0]['type'])
    elif dialect == 'pg':
        father_node = operand
        sql_root_node = None
        while father_node is not None:
            if father_node.value == 'sort_clause':
                order_by_mode = True
            if father_node.value == 'selectstmt':
                sql_root_node = father_node
                break
            elif not order_by_mode and father_node.value == 'simple_select_pramary':
                sql_root_node = father_node
                break
            father_node = father_node.father
        assert sql_root_node is not None
        if order_by_mode:
            with_clauses = fetch_with_ctes(sql_root_node, dialect)
            ctes = ''
            if len(with_clauses) > 0:
                ctes = "WITH RECURSIVE "
                for with_clause in with_clauses:
                    ctes += str(with_clause) + "\n\t"
            sql = f"{ctes}SELECT {str(operand)} FROM ({sql_root_node}) AS temp"
            flag, res = get_pg_type(db_name, sql, False)
            if not flag:
                print("Error in fetching operand type")
            return type_mapping('pg', res[0]['type'])
        else:
            assert sql_root_node.value == 'simple_select_pramary'
            main_select_node = sql_root_node
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
            flag, res = get_pg_type(db_name, sql, False)
            if not flag:
                print(sql)
                print("Error in fetching operand type")
            main_select_node.replace_child(new_operand_node, target_list_node)
            return type_mapping('pg', res[0]['type'])
    else:
        assert dialect == 'oracle'
        father_node = operand
        sql_root_node = None
        while father_node is not None:
            if father_node.value == 'order_by_clause':
                order_by_mode = True
            if father_node.value == 'select_statement':
                sql_root_node = father_node
                break
            elif not order_by_mode and father_node.value == 'query_block':
                sql_root_node = father_node
                break
            father_node = father_node.father
        assert sql_root_node is not None
        if order_by_mode:
            with_clauses = fetch_with_ctes(sql_root_node, dialect)
            ctes = ''
            if len(with_clauses) > 0:
                ctes = "WITH RECURSIVE "
                for with_clause in with_clauses:
                    ctes += str(with_clause) + "\n\t"
            sql = f"{ctes}SELECT {str(operand)} FROM ({sql_root_node}) AS temp"
            flag, res = get_oracle_type(db_name, sql, False)
            if not flag:
                print(sql)
                print("Error in fetching operand type")
            return type_mapping('oracle', res[0]['type'])
        else:
            assert sql_root_node.value == 'query_block'
            target_list_node = sql_root_node.get_child_by_value('selected_list')
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
            flag, res = get_oracle_type(db_name, sql, False)
            if not flag:
                print(sql)
                print("Error in fetching operand type")
            sql_root_node.replace_child(new_operand_node, target_list_node)
            if order_by_node is not None:
                sql_root_node.replace_child(empty_node, order_by_node)
            return type_mapping('oracle', res[0]['type'])
