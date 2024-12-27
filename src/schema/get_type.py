# -*- coding: utf-8 -*-
# @Project: sql2sqlBench
# @Module: get_type$
# @Author: 10379
# @Time: 2024/12/6 12:59
from typing import Dict

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from generator.Operand import Operand
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


def get_mysql_usable_cols(db_name, node: TreeNode) -> tuple[List, List, object]:
    """
    :param db_name: 所连接的数据库名
    :param node: sql语句解析得到的 ANTLR 根节点
    :return: tuple[List[Operand]]:非聚合函数可用列，
             tuple[List[Operand]]:聚合函数可用列
             TreeNode/None       :group_by的节点
    """
    # find all the sub_query, with_query, and common table
    while (node.value != 'selectStatement'):
        node = node.children[0]
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
        print(node.value)
        assert node.value == 'querySpecificationNointo' or node.value == 'querySpecification'
        group_by_node = node.get_child_by_value('groupByClause')
        if group_by_node is None:
            clone_node = node.clone()
            select_elements_node = clone_node.get_child_by_value('selectElements')
            assert isinstance(select_elements_node, TreeNode)
            select_elements_node.value = ' * '
            select_elements_node.is_terminal = True
            flag, res = get_mysql_type(str(clone_node), db_name, False)
            if not flag:
                raise ValueError(f"can't get types of {str(clone_node)}")
            normal_ops = []
            for ele in res:
                normal_ops.append(Operand(ele['col'], ele['type']))
            return normal_ops, [], None
        else:
            # 对于GROUP BY，聚合函数中可以使用的为SELECT * FROM xxx 去除GROUP BY时的所有列
            # 非聚合函数所能用的则为 GROUP BY中，所包含的各表达式
            assert isinstance(group_by_node, TreeNode)
            normal_nodes = []
            aggregate_nodes = []

            expression_nodes = group_by_node.get_children_by_value('groupByItem')
            for expression_node in expression_nodes:
                assert isinstance(expression_node, TreeNode)
                node = expression_node.get_child_by_value('expression')
                assert node is not None
                normal_nodes.append(node)

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
            flag, res = get_mysql_type(str(clone_node), db_name, False)
            if not flag:
                raise ValueError(f"can't get types of {str(clone_node)}")
            else:
                for col in res:
                    type = col['type']
                    normal_nodes.append(Operand(str(node), type))
            select_elements_node.value = ' * '
            flag, res = get_mysql_type(str(clone_node), db_name, False)
            if not flag:
                raise ValueError(f"can't get types of {str(clone_node)}")
            for ele in res:
                aggregate_nodes.append(Operand(ele['col'], ele['type']))
            return normal_nodes, aggregate_nodes, group_by_node


def get_pg_usable_cols(db_name, node: TreeNode) -> List:

    return None


def get_oracle_usable_cols(db_name: str, node: TreeNode):
    return None


normal, aggr, _ = get_usable_cols('BIRD', 'SELECT * FROM account', 'mysql')
print(normal)
