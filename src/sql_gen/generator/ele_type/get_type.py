# -*- coding: utf-8 -*-
# @Project: sql2sqlBench
# @Module: get_type$
# @Author: 10379
# @Time: 2024/12/6 12:59
from typing import Dict

from antlr_parser.Tree import TreeNode
from antlr_parser.get_structure import get_pg_select_primary
from antlr_parser.parse_tree import parse_tree
from utils.db_connector import *
from utils.tools import dialect_judge, add_quote


# TODO: With Recursive has to be solved
def get_type(obj: str, dialect: str, db_name, is_table: bool) -> tuple[bool, list]:
    dialect_type = dialect_judge(dialect)
    match dialect_type:
        case 'mysql':
            return get_mysql_type(db_name, obj, is_table)
        case 'postgres':
            return get_pg_type(db_name, obj, is_table)
        case 'oracle':
            return get_oracle_type(db_name, obj, is_table)
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
            return get_mysql_type(db_name, sql, False)
        case 'postgres':
            return get_pg_type(db_name, sql, False)
        case 'oracle':
            return get_oracle_type(db_name, sql, False)
        case _:
            assert False


def get_usable_cols(db_name, sql: str, dialect: str) -> tuple[List, List, object]:
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


def get_mysql_usable_cols(db_name, node: TreeNode) -> tuple[List, List, object]:
    """
    :param db_name: 所连接的数据库名
    :param node: sql语句解析得到的 ANTLR 根节点
    :return: tuple[List[Operand]]:非聚合函数可用列，
             tuple[List[Operand]]:聚合函数可用列
             TreeNode/None       :group_by的节点
    """
    # find all the sub_query, with_query, and common table
    node = node.get_child_by_value('sqlStatements')
    assert isinstance(node, TreeNode)
    for sql_statement in node.get_children_by_value('sqlStatement'):
        dml_statement = sql_statement.get_child_by_value('dmlStatement')
        assert isinstance(dml_statement, TreeNode)
        if dml_statement.get_child_by_value('selectStatement') is None:
            continue
        else:
            node = dml_statement.get_child_by_value('selectStatement')
    assert node.value == 'selectStatement'
    if node.get_child_by_value('UNION') is not None:
        # TODO: strategy to use sql have UNION is to be solved
        return [], [], None
    else:
        if node.get_child_by_value('querySpecificationNointo') is not None:
            node = node.get_child_by_value('querySpecificationNointo')
        elif node.get_child_by_value('querySpecification') is not None:
            node = node.get_child_by_value('querySpecification')
        elif node.get_child_by_value('queryExpressionNointo') is not None:
            node = node.get_child_by_value('queryExpressionNointo')
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
        group_by_node = node.get_child_by_value('groupByClause')
        if group_by_node is None:
            clone_node = node.clone()
            select_elements_node = clone_node.get_child_by_value('selectElements')
            assert isinstance(select_elements_node, TreeNode)
            select_elements_node.value = ' * '
            select_elements_node.is_terminal = True
            flag, res = get_mysql_type(db_name, str(clone_node), False)
            if not flag:
                raise ValueError(f"can't get types of {str(clone_node)}")
            normal_ops = []
            for ele in res:
                normal_ops.append(Operand(add_quote('mysql', ele['col']), ele['type']))
            return normal_ops, [], None
        else:
            assert isinstance(group_by_node, TreeNode)
            normal_nodes = []
            aggregate_nodes = []
            expression_nodes = group_by_node.get_children_by_value('groupByItem')
            for expression_node in expression_nodes:
                assert isinstance(expression_node, TreeNode)
                node_expr = expression_node.get_child_by_value('expression')
                assert node_expr is not None
                normal_nodes.append(node_expr)
            clone_node = node.clone()
            select_elements_node = clone_node.get_child_by_value('selectElements')
            assert isinstance(select_elements_node, TreeNode)
            new_str = ''
            for node in normal_nodes:
                if new_str != '':
                    new_str = new_str + ', '
                new_str = new_str + str(node)
            select_elements_node.value = ' ' + new_str + ' '
            select_elements_node.is_terminal = True
            flag, res = get_mysql_type(db_name, str(clone_node), False)
            if not flag:
                raise ValueError(f"can't get types of {str(clone_node)} db: {db_name}")
            else:
                for col in res:
                    type = col['type']
                    normal_nodes.append(Operand(str(node), type, 'mysql'))
            select_elements_node.value = ' * '
            flag, res = get_mysql_type(db_name, str(clone_node), False)
            if not flag:
                raise ValueError(f"can't get types of {str(clone_node)} reason: {res}")
            for ele in res:
                aggregate_nodes.append(Operand(add_quote('mysql', ele['col']), ele['type'], 'mysql'))
            return normal_nodes, aggregate_nodes, group_by_node


def parse_pg_group_by(group_list_node: TreeNode) -> List:
    group_by_item_nodes = group_list_node.get_children_by_value('group_by_list')
    if len(group_by_item_nodes) > 1:
        # No Node has cube or grouping sets or roll up
        res = []
        for children in group_by_item_nodes:
            res.append(str(children))
        return res
    elif len(group_by_item_nodes) == 1:
        group_by_item = group_by_item_nodes[0]
        assert isinstance(group_by_item, TreeNode)
        if group_by_item.get_child_by_value('empty_grouping_set') is not None:
            return []
        elif group_by_item.get_child_by_value('a_expr') is not None:
            return [str(group_by_item.get_child_by_value('a_expr'))]
        elif group_by_item.get_child_by_value('cube_clause') is not None:
            cube_clause_node = group_by_item.get_child_by_value('cube_clause')
            expr_list_node = cube_clause_node.get_child_by_value('expr_list')
            res = []
            for expr in expr_list_node.get_children_by_value('a_expr'):
                res.append(str(expr))
            return res
        elif group_by_item.get_child_by_value('rollup_clause') is not None:
            rollup_clause_node = group_by_item.get_child_by_value('rollup_clause')
            expr_list_node = rollup_clause_node.get_child_by_value('expr_list')
            res = []
            for expr in expr_list_node.get_children_by_value('a_expr'):
                res.append(str(expr))
            return res
        elif group_by_item.get_child_by_value('grouping_sets_clause') is not None:
            sets_clause_node = group_by_item.get_child_by_value('grouping_sets_clause')
            group_by_list_node = sets_clause_node.get_child_by_value('group_by_list')
            res = []
            for item_node in group_by_list_node.get_children_by_value('group_by_item'):
                assert item_node.get_child_by_value('group_expr_list') is not None
                expr_list = group_by_item.get_child_by_value('group_expr_list').get_child_by_value('expr_list')
                for a_expr in expr_list.get_child_by_value('a_expr'):
                    res.append(a_expr)
            return res
        else:
            assert False
    else:
        assert False


def get_pg_usable_cols(db_name, node: TreeNode) -> tuple[List, List, object]:
    select_node = node
    simple_select_primary_node = get_pg_select_primary(select_node)
    clone_node = simple_select_primary_node.clone()
    tgt_node = clone_node.get_child_by_value('opt_target_list')
    if tgt_node is None:
        tgt_node = clone_node.get_child_by_value('target_list')
    assert isinstance(tgt_node, TreeNode)
    tgt_node.is_terminal = True
    tgt_node.value = '*'
    flag, res = get_pg_type(db_name, str(clone_node), False)
    if not flag:
        raise ValueError(f"can't get types of {str(clone_node)}")
    normal_ops = []
    for ele in res:
        normal_ops.append(Operand(add_quote('pg', ele['col']), ele['type'], 'pg'))
    if simple_select_primary_node.get_child_by_value('group_clause') is not None:
        group_node = simple_select_primary_node.get_child_by_value('group_clause')
        assert isinstance(group_node, TreeNode)
        group_list = group_node.get_child_by_value('group_list')
        group_by_ops = parse_pg_group_by(group_list)
        clone_node = simple_select_primary_node.clone()
        select_elements_node = clone_node.get_child_by_value('opt_target_list')
        if select_elements_node is None:
            select_elements_node = clone_node.get_child_by_value('target_list')
        assert isinstance(select_elements_node, TreeNode)
        new_str = ''
        for node in group_by_ops:
            if new_str != '':
                new_str = new_str + ', '
            new_str = new_str + str(node)
        select_elements_node.value = ' ' + new_str + ' '
        select_elements_node.is_terminal = True
        clone_node.rm_child_by_value('group_clause')
        clone_node.rm_child_by_value('having_clause')
        flag, res = get_pg_type(db_name, str(clone_node), False)
        group_by_ops = []
        if not flag:
            raise ValueError(f"can't get types of {str(clone_node)}")
        else:
            for col in res:
                type = col['type']
                group_by_ops.append(Operand(str(node), type, 'pg'))
        return group_by_ops, normal_ops, group_node
    else:
        return normal_ops, [], None


def get_oracle_usable_cols(db_name: str, node: TreeNode):
    return [], [], None


# print(";".join(['a', 'b']))
# sql = (
#     "SELECT `chainid` FROM `gasstations`")
# print(get_mysql_type(sql, 'bird', False))
# dialect = 'mysql'
# cols, _, _ = get_usable_cols('bird', sql, 'mysql')
#
# for col in cols:
#     print(col)
