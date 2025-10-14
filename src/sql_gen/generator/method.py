# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: method$
# @Author: 10379
# @Time: 2025/5/10 12:24
import copy
import json
import os
import random
import re

from antlr_parser.Tree import TreeNode
from antlr_parser.general_tree_analysis import inside_aggregate_function, fetch_query_body_node, fetch_all_ctes, \
    build_ctes
from antlr_parser.mysql_tree import fetch_all_simple_select_from_select_stmt_mysql
from antlr_parser.oracle_tree import fetch_all_simple_select_from_subquery_oracle
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import fetch_all_simple_select_from_select_stmt_pg
from sql_gen.generator.ele_type.operand_analysis import analysis_ctes
from sql_gen.generator.ele_type.type_conversion import type_mapping
from sql_gen.generator.ele_type.type_def import BaseType, is_num_type, is_str_type, is_time_type, IntGeneralType, \
    NullType
from sql_gen.generator.element.Pattern import ForSlot
from utils.ExecutionEnv import ExecutionEnv
from utils.tools import get_db_ids, get_proj_root_path


def fetch_fulfilled_sqls(points: list[dict], src_dialect, tgt_dialect, db_id: str = None):
    if db_id is not None:
        db_ids = [db_id]
    else:
        db_ids = get_db_ids()
    all_sqls = []
    for db in db_ids:
        sql_root_path = os.path.join(get_proj_root_path(), 'SQL', db)
        if os.path.exists(os.path.join(sql_root_path, 'no_points')):
            path1 = os.path.join(sql_root_path, 'no_points', f'{src_dialect}_{tgt_dialect}.json')
            path2 = os.path.join(sql_root_path, 'no_points', f'{tgt_dialect}_{src_dialect}.json')
            if os.path.exists(path1):
                with open(path1, 'r') as file:
                    sqls = json.load(file)
            elif os.path.exists(path2):
                with open(path2, 'r') as file:
                    sqls = json.load(file)
            else:
                assert False
            assert sqls is not None
            for sql in sqls:
                flag = True
                if 'points' in sql:
                    if len(sql['points']) > 0:
                        print(path1)
                        print(path2)
                        print(sql)
                point_list = sql.get('points', [])
                for point in point_list:
                    if point not in points:
                        flag = False
                        break
                if flag:
                    all_sqls.append(sql)
        path = os.path.join(sql_root_path, 'points', f'{src_dialect}_{tgt_dialect}.json')
        if os.path.exists(path):
            with open(path, 'r') as file:
                sqls = json.load(file)
            for sql in sqls:
                flag = True
                for point in sql['points']:
                    if point not in points:
                        flag = False
                        break
                if flag:
                    all_sqls.append(sql)
    for sql in all_sqls:
        if 'points' not in sql:
            sql['points'] = []
    random.shuffle(all_sqls)
    return all_sqls


def mark_in_aggregate_slot(node: TreeNode, dialect: str, aggregate_slot_set: set):
    if node.slot is not None:
        if inside_aggregate_function(dialect, node):
            aggregate_slot_set.add(node.slot)
        return
    if len(node.for_loop_sub_trees) > 0:
        for i in range(len(node.for_loop_sub_trees)):
            sub_tree = node.for_loop_sub_trees[i]
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            first_tree.father = node.father
            new_set = set()
            for child in first_tree.children:
                mark_in_aggregate_slot(child, dialect, new_set)
            for_loop = node.for_loop_slot[i]
            assert isinstance(for_loop, ForSlot)
            for j in range(len(for_loop.sub_ele_slots)):
                if for_loop.sub_ele_slots[j] in new_set:
                    aggregate_slot_set.add(for_loop.ele_slots[j])
        return
    for child in node.children:
        assert isinstance(child, TreeNode)
        mark_in_aggregate_slot(child, dialect, aggregate_slot_set)


def merge_trans_points(points1: list[dict], points2: list[dict]):
    res = copy.deepcopy(points1)
    for point in points2:
        flag = False
        for exist_point in res:
            if exist_point['point'] == point['point']:
                exist_point['num'] = exist_point['num'] + point['num']
                flag = True
        if not flag:
            res.append(point)
    return res


def add_point_to_point_dict(points1: list[dict], point):
    flag = False
    if isinstance(point, str):
        point_name = point
    elif isinstance(point, dict):
        point_name = point['Desc']
    else:
        point_name = point.point_name
    for exist_point in points1:
        if exist_point['point'] == point_name:
            exist_point['num'] = exist_point['num'] + 1
            flag = True
    if not flag:
        points1.append({
            "point": point_name,
            "num": 1
        })


def has_limit_order_by(select_body_node: TreeNode, dialect: str):
    if dialect == 'mysql':
        if select_body_node.get_child_by_value('orderByClause') is not None or select_body_node.get_child_by_value(
                'limitClause') is not None:
            return True
        if len(select_body_node.get_children_by_value('unionStatement')) == 0:
            # only one selectStatement
            if select_body_node.get_child_by_value('querySpecification') is not None:
                node = select_body_node.get_child_by_value('querySpecification')
            elif select_body_node.get_child_by_value('querySpecificationNointo') is not None:
                node = select_body_node.get_child_by_value('querySpecificationNointo')
            else:
                node = None
            if isinstance(node, TreeNode) and node.get_child_by_value(
                    'orderByClause') is not None or node.get_child_by_value('limitClause') is not None:
                return True
        else:
            union_statement_node = select_body_node.get_children_by_value('unionStatement')[-1]
            if union_statement_node.get_child_by_value('querySpecificationNointo') is not None:
                node = select_body_node.get_child_by_value('querySpecificationNointo')
                if isinstance(node, TreeNode) and node.get_child_by_value(
                        'orderByClause') is not None or node.get_child_by_value('limitClause') is not None:
                    return True
    elif dialect == 'pg':
        # select_no_parens
        if (select_body_node.get_child_by_value('opt_sort_clause') is not None or
                select_body_node.get_child_by_value('opt_select_limit') is not None or
                select_body_node.get_child_by_value(
                    'select_limit') is not None):
            return True
    elif dialect == 'oracle':
        if (select_body_node.get_child_by_value('order_by_clause') is not None or
                select_body_node.get_child_by_value('offset_clause') is not None):
            return True
        main_body_node = select_body_node.get_children_by_path(['select_only_statement', 'subquery'])
        assert len(main_body_node) == 1
        main_body_node = main_body_node[0]
        if len(main_body_node.get_children_by_value('subquery_operation_part')) == 0:
            if main_body_node.get_child_by_value('subquery_basic_elements') is not None:
                node = main_body_node.get_child_by_value('subquery_basic_elements')
                if node.get_child_by_value('query_block') is not None:
                    node = node.get_child_by_value('query_block')
                    if (node.get_child_by_value('order_by_clause') is not None or
                            node.get_child_by_value('offset_clause') is not None):
                        return True
        else:
            union_statement_node = main_body_node.get_children_by_value('subquery_operation_part')[-1]
            if union_statement_node.get_child_by_value('subquery_basic_elements') is not None:
                node = union_statement_node.get_child_by_value('subquery_basic_elements')
                if node.get_child_by_value('query_block') is not None:
                    node = node.get_child_by_value('query_block')
                    if (node.get_child_by_value('order_by_clause') is not None or
                            node.get_child_by_value('offset_clause') is not None):
                        return True
    else:
        assert False
    return False


def rm_outer_limit_order_by(select_body_node: TreeNode, dialect: str):
    pass


def fetch_all_select_stmts(root_node: TreeNode, dialect: str):
    if dialect == 'pg':
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        simple_select_nodes = fetch_all_simple_select_from_select_stmt_pg(select_stmt_node)
    elif dialect == 'mysql':
        select_statement_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                'dmlStatement', 'selectStatement'])
        assert len(select_statement_node) == 1
        select_stmt_node = select_statement_node[0]
        simple_select_nodes = fetch_all_simple_select_from_select_stmt_mysql(select_stmt_node)
    elif dialect == 'oracle':
        subquery_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                        'select_statement', 'select_only_statement', 'subquery'])
        if len(subquery_node) != 1:
            print('FOR UPDATE haven\'t been supported yet')
            assert False
        select_stmt_node = subquery_node[0]
        simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(select_stmt_node)
    else:
        assert False
    return simple_select_nodes


def rm_select_node(select_stmts: list[TreeNode], src_dialect: str, index_i):
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return None
            to_rm_node = selectElement_nodes[index_i]
            i = 0
            for child in selectElements_node.children:
                if to_rm_node == child:
                    if i >= 1 and selectElements_node.children[i - 1].value == ',':
                        selectElements_node.rm_child(selectElements_node.children[i - 1])
                    selectElements_node.rm_child(to_rm_node)
                    break
                i = i + 1
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return False
            to_rm_node = selectElement_nodes[index_i]
            i = 0
            for child in target_list_node.children:
                if to_rm_node == child:
                    if i >= 1 and target_list_node.children[i - 1].value == ',':
                        target_list_node.rm_child(target_list_node.children[i - 1])
                    assert isinstance(target_list_node, TreeNode)
                    target_list_node.rm_child(to_rm_node)
                    break
                i = i + 1
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return False
            to_rm_node = selectElement_nodes[index_i]
            i = 0
            for child in selectElements_node.children:
                if to_rm_node == child:
                    if i >= 1 and selectElements_node.children[i - 1].value == ',':
                        selectElements_node.rm_child(selectElements_node.children[i - 1])
                    selectElements_node.rm_child(to_rm_node)
                    break
                i = i + 1
        else:
            assert False


def add_null_node(select_stmts: list[TreeNode], src_dialect: str, ori_type, use_null_flag: bool = False):
    if is_num_type(ori_type):
        if use_null_flag:
            null_node = TreeNode('CAST(NULL AS REAL)', src_dialect, True)
        else:
            null_node = TreeNode('0.0', src_dialect, True)
    elif is_time_type(ori_type):
        if use_null_flag:
            null_node = TreeNode('CAST(null AS DATE)', src_dialect, True)
        else:
            null_node = TreeNode('DATE \'2025-05-30\'', src_dialect, True)
    else:
        null_node = TreeNode('NULL', src_dialect, True)
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return None
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.add_child(new_mark_node)
            new_select_element_node = TreeNode('selectElement', src_dialect, False)
            new_select_element_node.add_child(null_node)
            selectElements_node.add_child(new_select_element_node)
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return False
            new_mark_node = TreeNode(',', src_dialect, True)
            target_list_node.add_child(new_mark_node)
            new_select_element_node = TreeNode('target_el', src_dialect, False)
            new_select_element_node.add_child(null_node)
            target_list_node.add_child(new_select_element_node)
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return False
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.add_child(new_mark_node)
            new_select_element_node = TreeNode('select_list_elements', src_dialect, False)
            new_select_element_node.add_child(null_node)
            selectElements_node.add_child(new_select_element_node)
        else:
            assert False


def reorder_col(select_stmts: list[TreeNode], src_dialect: str, index_i, index_j):
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            child_j = selectElement_nodes[index_j]
            for i in range(len(selectElements_node.children)):
                if selectElements_node.children[i] == child_i:
                    selectElements_node.children[i] = child_j
                elif selectElements_node.children[i] == child_j:
                    selectElements_node.children[i] = child_i
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            child_j = selectElement_nodes[index_j]
            for i in range(len(target_list_node.children)):
                if target_list_node.children[i] == child_i:
                    target_list_node.children[i] = child_j
                elif target_list_node.children[i] == child_j:
                    target_list_node.children[i] = child_i
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            child_j = selectElement_nodes[index_j]
            for i in range(len(selectElements_node.children)):
                if selectElements_node.children[i] == child_i:
                    selectElements_node.children[i] = child_j
                elif selectElements_node.children[i] == child_j:
                    selectElements_node.children[i] = child_i
        else:
            assert False
    return True


def add_col_null(select_stmts: list[TreeNode], src_dialect: str, index_i: int, ori_type, null_mode=False):
    if isinstance(ori_type, IntGeneralType):
        null_node = TreeNode('0', src_dialect, True)
    elif is_num_type(ori_type):
        if null_mode:
            null_node = TreeNode('CAST(NULL AS REAL)', src_dialect, True)
        else:
            null_node = TreeNode('0.0', src_dialect, True)
    else:
        null_node = TreeNode('NULL', src_dialect, True)
    if src_dialect == 'mysql':
        father_node = TreeNode('selectElement', src_dialect, False)
    elif src_dialect == 'pg':
        father_node = TreeNode('target_el', src_dialect, False)
    elif src_dialect == 'oracle':
        father_node = TreeNode('select_list_elements', src_dialect, False)
    else:
        assert False
    father_node.add_child(null_node)
    for select_stmt in select_stmts:
        if src_dialect == 'mysql':
            selectElements_node = select_stmt.get_child_by_value('selectElements')
            selectElement_nodes = selectElements_node.get_children_by_value('selectElement')
            if len(selectElement_nodes) == 0:
                return None
            child_i = selectElement_nodes[index_i]
            insert_idx = None
            for i, child in enumerate(selectElements_node.children):
                if child == child_i:
                    insert_idx = i
            selectElements_node.children.insert(insert_idx, father_node)
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.children.insert(insert_idx + 1, new_mark_node)
        elif src_dialect == 'pg':
            if select_stmt.get_child_by_value('opt_target_list') is not None:
                opt_target_list = select_stmt.get_child_by_value('opt_target_list')
                target_list_node = opt_target_list.get_child_by_value('target_list')
            else:
                target_list_node = select_stmt.get_child_by_value('target_list')
            selectElement_nodes = target_list_node.get_children_by_value('target_el')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            insert_idx = None
            for i, child in enumerate(target_list_node.children):
                if child == child_i:
                    insert_idx = i
            target_list_node.children.insert(insert_idx, father_node)
            new_mark_node = TreeNode(',', src_dialect, True)
            target_list_node.children.insert(insert_idx + 1, new_mark_node)
        elif src_dialect == 'oracle':
            selectElements_node = select_stmt.get_child_by_value('selected_list')
            selectElement_nodes = selectElements_node.get_children_by_value('select_list_elements')
            if len(selectElement_nodes) == 0:
                return False
            child_i = selectElement_nodes[index_i]
            insert_idx = None
            for i, child in enumerate(selectElements_node.children):
                if child == child_i:
                    insert_idx = i
            selectElements_node.children.insert(insert_idx, father_node)
            new_mark_node = TreeNode(',', src_dialect, True)
            selectElements_node.children.insert(insert_idx + 1, new_mark_node)
        else:
            assert False
    return True


def rep_select_stmt_with_cte(root_node: TreeNode, select_stmt_nodes: list[TreeNode], src_dialect):
    flag, ctes = fetch_all_ctes(root_node, src_dialect)
    if not flag:
        return None
    rep_node_dict = {}
    for index, select_stmt_node in enumerate(select_stmt_nodes):
        select_stmt_sql = str(select_stmt_node)
        pattern = r"^SELECT\s+\*\s+FROM\s+((`[^`]*`)|(\"[^\"]*\")|(\[[^\]]*\])|([a-zA-Z_][a-zA-Z0-9_]*))\s*;?\s*$"
        match = re.match(pattern, select_stmt_sql, re.IGNORECASE)
        if not match:
            continue
        full_match = match.group(1)
        if full_match.startswith('`') and full_match.endswith('`'):
            to_match_table_name = full_match[1:-1]
        elif full_match.startswith('"') and full_match.endswith('"'):
            to_match_table_name = full_match[1:-1]
        elif full_match.startswith('[') and full_match.endswith(']'):
            to_match_table_name = full_match[1:-1]
        else:
            to_match_table_name = full_match
        for cte in ctes['cte_list']:
            if cte['cte_name'].lower() == to_match_table_name.lower():
                rep_node_dict[index] = cte['query']
                break
    for key, value in rep_node_dict.items():
        select_stmt_nodes[index] = value


def process_to_add_sql(sql1_types: list[BaseType] | None, sql1: str | TreeNode, sql2: str | TreeNode, src_dialect: str,
                       execute_env: ExecutionEnv, del_mode: bool, use_null_flag: bool = False):
    if sql1_types is None:
        flag, res = execute_env.fetch_type(sql1, False)
        if not flag:
            return None, None
        sql1_types = [type_mapping(src_dialect, t['type']) for t in res]

    if isinstance(sql1, str):
        sql1_root_node, _, _, _ = parse_tree(sql1, src_dialect)
        if sql1_root_node is None:
            return None
        sql1_root_node = TreeNode.make_g4_tree_by_node(sql1_root_node, src_dialect)
    else:
        sql1_root_node = sql1
    if isinstance(sql2, str):
        sql2_root_node, _, _, _ = parse_tree(sql2, src_dialect)
        if sql2_root_node is None:
            return None
        sql2_root_node = TreeNode.make_g4_tree_by_node(sql2_root_node, src_dialect)
    else:
        sql2_root_node = sql2
    flag, res = execute_env.fetch_type(sql2, False)
    sql2_types = [type_mapping(src_dialect, t['type']) for t in res]
    i = 0
    query1_select_stmts = fetch_all_select_stmts(sql1_root_node, src_dialect)
    rep_select_stmt_with_cte(sql1_root_node, query1_select_stmts, src_dialect)
    query2_select_stmts = fetch_all_select_stmts(sql2_root_node, src_dialect)
    rep_select_stmt_with_cte(sql2_root_node, query2_select_stmts, src_dialect)
    for sql1_type in sql1_types:
        flag = False
        if i >= len(sql2_types):
            break
        if is_num_type(sql1_type):
            for j in range(len(sql2_types)):
                if j < i:
                    continue
                if is_num_type(sql2_types[j]):
                    reorder_col(query2_select_stmts, src_dialect, i, j)
                    sql2_types[j], sql2_types[i] = sql2_types[i], sql2_types[j]
                    flag = True
        if is_str_type(sql1_type):
            for j in range(len(sql2_types)):
                if j < i:
                    continue
                if is_str_type(sql2_types[j]):
                    reorder_col(query2_select_stmts, src_dialect, i, j)
                    sql2_types[j], sql2_types[i] = sql2_types[i], sql2_types[j]
                    flag = True
        if is_time_type(sql1_type):
            for j in range(len(sql2_types)):
                if j < i:
                    continue
                if is_time_type(sql2_types[j]):
                    reorder_col(query2_select_stmts, src_dialect, i, j)
                    sql2_types[j], sql2_types[i] = sql2_types[i], sql2_types[j]
                    flag = True
        if not flag:
            add_col_null(query2_select_stmts, src_dialect, i, sql1_type)
            sql2_types.insert(i, NullType())
        i += 1
    if len(sql1_types) < len(sql2_types):
        sql1_types_len = len(sql1_types)
        j = len(sql2_types) - 1
        t = 0
        while j >= len(sql1_types):
            if del_mode:
                rm_select_node(query2_select_stmts, src_dialect, j)
            else:
                add_null_node(query1_select_stmts, src_dialect, sql2_types[sql1_types_len + t], use_null_flag)
            j -= 1
            t += 1
    if len(sql1_types) > len(sql2_types):
        j = len(sql1_types) - 1
        t = 0
        sql2_types_len = len(sql2_types)
        while j >= len(sql2_types):
            add_null_node(query2_select_stmts, src_dialect, sql1_types[sql2_types_len + t], use_null_flag)
            j -= 1
            t += 1
    return str(sql1_root_node), str(sql2_root_node)


def merge_query(query_dict1: dict | None, query_dict2: dict,
                execute_env: ExecutionEnv, del_mode: bool = False, use_null_flag: bool = False):
    cte_alias_set = set()
    if query_dict1 is None:
        return query_dict2
    src_dialect = None
    tgt_dialect = None
    for key, value in query_dict1.items():
        if src_dialect is None:
            src_dialect = key
        elif tgt_dialect is None:
            tgt_dialect = key
    query1 = query_dict1[src_dialect]
    query2 = query_dict2[src_dialect]
    points = merge_trans_points(query_dict1['points'], query_dict2['points'])
    query1, query2 = process_to_add_sql(None, query1, query2, src_dialect,
                                        execute_env, del_mode, use_null_flag)
    if query1 is None:
        return None

    query1_root_node, _, _, _ = parse_tree(query1, src_dialect)
    if query1_root_node is None:
        return None
    else:
        query1_root_node = TreeNode.make_g4_tree_by_node(query1_root_node, src_dialect)
    query2_root_node, _, _, _ = parse_tree(query2, src_dialect)
    if query2_root_node is None:
        return None
    else:
        query2_root_node = TreeNode.make_g4_tree_by_node(query2_root_node, src_dialect)
    flag1, query1_ctes = analysis_ctes(query1_root_node, src_dialect)
    flag2, query2_ctes = analysis_ctes(query2_root_node, src_dialect)
    flag = True
    for cte1 in query1_ctes['cte_list']:
        cte_alias_set.add(cte1['cte_name'])
        for cte2 in query2_ctes['cte_list']:
            cte_alias_set.add(cte2['cte_name'])
            if cte1['cte_name'] == cte2['cte_name']:
                flag = False
    if not flag:
        return None
    final_ctes = {
        "is_recursive": query1_ctes['is_recursive'] and query2_ctes['is_recursive'],
        'cte_list': query1_ctes['cte_list'] + query2_ctes['cte_list']
    }

    query1_select_body_node = fetch_query_body_node(query1_root_node, src_dialect)
    query2_select_body_node = fetch_query_body_node(query2_root_node, src_dialect)
    query_body_str = ''
    if has_limit_order_by(query1_select_body_node, src_dialect):
        i = 0
        while f'cte{i}' in cte_alias_set:
            i += 1
        cte_alias_set.add(f'cte{i}')
        final_ctes['cte_list'].append({
            'cte_name': f"cte{i}",
            'query': str(query1_select_body_node),
            'column_list': None,
            'cte_name_type_pairs': []
        })
        query_body_str += f'SELECT * FROM cte{i}'
    else:
        query_body_str = str(query1_select_body_node)
    if has_limit_order_by(query2_select_body_node, src_dialect):
        i = 0
        while f'cte{i}' in cte_alias_set:
            i += 1
        final_ctes['cte_list'].append({
            'cte_name': f"cte{i}",
            'query': str(query2_select_body_node),
            'column_list': None,
            'cte_name_type_pairs': []
        })
        query_body_str += f' UNION ALL SELECT * FROM cte{i}'
    else:
        query_body_str += f' UNION ALL {str(query2_select_body_node)}'
    merged_query = build_ctes(final_ctes, src_dialect) + ' ' + query_body_str
    return {
        src_dialect: merged_query.strip(),
        tgt_dialect: '',
        "points": points
    }


def points_equal(points1: list[dict], points2: list[dict]):
    if len(points1) != len(points2):
        return False
    for p1 in points1:
        flag = False
        for p2 in points2:
            if p1['point'] == p2['point'] and p1['num'] == p2['num']:
                flag = True
        if not flag:
            return False
    return True
