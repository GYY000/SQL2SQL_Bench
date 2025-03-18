# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: generate$
# @Author: 10379
# @Time: 2024/12/28 19:31
import json
import os.path
import random

from antlr_parser.Tree import TreeNode
from antlr_parser.get_structure import get_pg_select_primary
from antlr_parser.parse_tree import parse_tree
from sql_gen.generator.point_parser import *
from db_builder.get_type import get_usable_cols
from utils.tools import get_proj_root_path, strip_quote


def fulfill_cond(src_sql: str, dialect: str) -> bool:
    node, _, _, _ = parse_tree(src_sql, dialect)
    node = TreeNode.make_g4_tree_by_node(node, dialect)
    res = get_select_list_node(node, dialect) is not None
    if not res:
        print(dialect)
    return res


def get_select_list_node(node: TreeNode, dialect: str):
    if dialect == 'mysql':
        node = node.get_child_by_value('sqlStatements')
        assert isinstance(node, TreeNode)
        for sql_statement in node.get_children_by_value('sqlStatement'):
            dml_statement = sql_statement.get_child_by_value('dmlStatement')
            assert isinstance(dml_statement, TreeNode)
            if dml_statement.get_child_by_value('selectStatement') is None:
                continue
            else:
                node = dml_statement.get_child_by_value('selectStatement')
                break
        assert node.value == 'selectStatement'
        if node.get_child_by_value('UNION') is not None:
            # TODO: deal with union except and intersect...
            return None
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
            node = node.get_child_by_value('selectElements')
            assert node is not None
            return node
    elif dialect == 'pg':
        node = get_pg_select_primary(node)
        assert node is not None
        target_list = node.get_child_by_value('target_list')
        if target_list is None:
            opt_target_list = node.get_child_by_value('opt_target_list')
            target_list = opt_target_list.get_child_by_value('target_list')
        assert target_list is not None
        return target_list
    elif dialect == 'oracle':
        pass


def insert_select_list_pos(sql: str, dialect: str, insert_content: str) -> str:
    node, _, _, _ = parse_tree(sql, dialect)
    if node is None:
        return ''
    node = TreeNode.make_g4_tree_by_node(node, dialect)
    if dialect == 'mysql':
        select_lists_node = get_select_list_node(node, dialect)
        assert isinstance(select_lists_node, TreeNode)
        if select_lists_node.get_child_by_value('*') is not None:
            select_lists_node.rm_child_by_value('*')
            select_lists_node.add_child(TreeNode(insert_content, dialect, True))
        else:
            select_lists_node.add_child(TreeNode(', ' + insert_content, dialect, True))
        return str(node)
    elif dialect == 'pg':
        select_lists_node = get_select_list_node(node, dialect)
        target_els = select_lists_node.get_children_by_value('target_el')
        for target_el in target_els:
            if str(target_el) == '*':
                select_lists_node.rm_child(target_el)
        if len(select_lists_node.get_children_by_value('target_el')) == 0:
            select_lists_node.add_child(TreeNode(insert_content, dialect, True))
        else:
            select_lists_node.add_child(TreeNode(', ' + insert_content, dialect, True))
        return str(node)
    elif dialect == 'oracle':
        pass
    else:
        assert False


def gen_by_insert(point: Point, src_dialect: str, tgt_dialect: str, dbname: str = 'bird') -> Dict:
    queries_path = os.path.join(get_proj_root_path(), 'data', dbname, 'query', f"{src_dialect}_{tgt_dialect}.json")
    with open(queries_path, 'r') as file:
        queries = json.load(file)

    query = random.choice(queries)
    while not fulfill_cond(query[src_dialect], src_dialect) or not fulfill_cond(query[tgt_dialect],
                                                                                tgt_dialect):
        query = random.choice(queries)
    src_sql = query[src_dialect]
    tgt_sql = query[tgt_dialect]

    src_normal_cols, src_aggregate_cols, src_group_by_node = get_usable_cols(dbname, src_sql, src_dialect)
    tgt_normal_cols, tgt_aggregate_cols, tgt_group_by_node = get_usable_cols(dbname, tgt_sql, tgt_dialect)

    # print(src_normal_cols)
    # print(tgt_normal_cols)
    col_pairs = []
    for src_col in src_normal_cols:
        tgt_col = None
        for col in tgt_normal_cols:
            if strip_quote(src_dialect, src_col.value).lower() == strip_quote(tgt_dialect, col.value).lower():
                tgt_col = col
        if tgt_col is not None:
            col_pairs.append({
                src_dialect: src_col,
                tgt_dialect: tgt_col
            })

    src_piece, tgt_piece = point.instr_full_content(col_pairs)
    src_sql = insert_select_list_pos(src_sql, src_dialect, src_piece)
    tgt_sql = insert_select_list_pos(tgt_sql, tgt_dialect, tgt_piece)
    return {
        src_dialect: src_sql,
        tgt_dialect: tgt_sql
    }


def gen_by_transform(src_pattern: Point, src_dialect: str, tgt_dialect: str) -> Dict:
    return {
        src_dialect: "",
        tgt_dialect: ""
    }


def generate_by_point(point: Dict, src_dialect: str, tgt_dialect: str) -> Dict:
    used_point = parse_point(point, src_dialect, tgt_dialect)
    if point["type"] == 'insert':
        return gen_by_insert(used_point, src_dialect, tgt_dialect)
    else:
        return gen_by_transform(used_point, src_dialect, tgt_dialect)


def test():
    with open('D:\\Coding\\SQL2SQL_Bench\\conv_point\\test_point.json', 'r') as file:
        points = json.load(file)
    src_dialect = 'mysql'
    tgt_dialect = 'pg'

    for point in points:
        res = generate_by_point(point, 'mysql', 'pg')
        print(res[src_dialect])
        print(res[tgt_dialect])


test()
