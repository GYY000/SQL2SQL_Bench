# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: add_pattern_point$
# @Author: 10379
# @Time: 2025/7/6 10:35
import json
import os
import random

from antlr_parser.Tree import TreeNode
from antlr_parser.mysql_tree import fetch_all_simple_select_from_select_stmt_mysql, \
    fetch_main_select_from_select_stmt_mysql
from antlr_parser.oracle_tree import fetch_all_simple_select_from_subquery_oracle
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import dfs_select_clause, get_pg_main_select_node_from_select_stmt, \
    fetch_all_simple_select_from_select_stmt_pg
from sql_gen.generator.ele_type.operand_analysis import analysis_sql
from sql_gen.generator.ele_type.type_def import BaseType, ListType
from sql_gen.generator.element.Operand import Operand
from sql_gen.generator.element.Pattern import ForSlot, ValueSlot, UdfFunction
from sql_gen.generator.element.Point import Point
from sql_gen.generator.method import mark_in_aggregate_slot, merge_query
from sql_gen.generator.pattern_tree_parser import get_pattern_value, parse_pattern_tree
from utils.ExecutionEnv import ExecutionEnv
from utils.tools import get_proj_root_path, get_db_ids, get_all_db_name


class SelectPattern:
    def __init__(self):
        self.where_cond_variable = None
        self.from_tables_variable = None
        self.group_by_cols_variable = None
        self.select_list_variable = None
        self.having_cond_variable = None


def fetch_no_points_sql(src_dialect: str, tgt_dialect: str, db_id: str | None):
    if db_id is None:
        db_ids = get_db_ids()
    else:
        db_ids = [db_id]
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
                all_sqls.append(sql)
    for sql in all_sqls:
        if 'points' not in sql:
            sql['points'] = []
    return all_sqls


def analyze_sql_statement(tree_node: TreeNode, dialect: str) -> dict | None:
    """
    {
        "select_list": [],
        "from_tables": [],
        "where_cond": None,
        "group_by_cols": None,
        "having_cond": None
    }
    """
    if dialect == 'pg':
        select_stmt_node = tree_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        # TODO: CTE
        select_stmt_node = select_stmt_node[0]
        select_main_node = get_pg_main_select_node_from_select_stmt(select_stmt_node)
        with_clause_node = select_main_node.get_child_by_value('with_clause')
        if with_clause_node is not None:
            return None
        select_clause_node = select_main_node.get_child_by_value('select_clause')
        assert select_clause_node is not None
        first_intersect_nodes = select_clause_node.get_children_by_value('simple_select_intersect')
        assert len(first_intersect_nodes) > 0
        first_intersect_node = first_intersect_nodes[0]
        first_pramary_nodes = first_intersect_node.get_children_by_value('simple_select_pramary')
        assert len(first_pramary_nodes) > 0
        first_pramary_node = first_pramary_nodes[0]
        if first_pramary_node.get_child_by_value('opt_target_list') is not None:
            opt_target_list_node = first_pramary_node.get_child_by_value('opt_target_list')
            target_list_node = opt_target_list_node.get_child_by_value('target_list')
        else:
            target_list_node = first_pramary_node.get_child_by_value('target_list')
        assert isinstance(target_list_node, TreeNode)
        select_list_ops = []
        for target_el in target_list_node.get_children_by_value('target_el'):
            select_list_ops.append(target_el)
        having_clause_node = first_pramary_node.get_child_by_value('having_clause')
        having_cond = None
        if having_clause_node is not None:
            having_cond = having_clause_node.get_child_by_value('a_expr')
        where_cond = None
        if first_pramary_node.get_child_by_value('where_clause') is not None:
            where_cond = first_pramary_node.get_child_by_value('where_clause').get_child_by_value('a_expr')
        group_by_clause_node = first_pramary_node.get_child_by_value('group_clause')
        group_by_ops = []
        if group_by_clause_node is not None:
            group_by_list_node = group_by_clause_node.get_child_by_value('group_by_list')
            for group_by_item_node in group_by_list_node.get_children_by_value('group_by_item'):
                if group_by_item_node.get_child_by_value('a_expr') is not None:
                    group_by_ops.append(group_by_item_node.get_child_by_value('a_expr'))
        from_clause_node = first_pramary_node.get_child_by_value('from_clause')
        from_tables = []
        if from_clause_node is not None:
            from_list_node = from_clause_node.get_child_by_value('from_list')
            assert from_list_node is not None
            for table_ref_node in from_list_node.get_children_by_value('table_ref'):
                from_tables.append(table_ref_node)
        # print({
        #     "having_cond": having_cond,
        #     "select_list": select_list_ops,
        #     "where_cond": where_cond,
        #     "group_by_cols": group_by_ops,
        #     "from_tables": from_tables
        # })
        return {
            "having_cond": having_cond,
            "select_list": select_list_ops,
            "where_cond": where_cond,
            "group_by_cols": group_by_ops,
            "from_tables": from_tables
        }
    elif dialect == 'mysql':
        select_stmt_node = tree_node.get_children_by_path(['sqlStatements',
                                                           'sqlStatement',
                                                           'dmlStatement', 'selectStatement'])
        select_stmt_node = select_stmt_node[0]
        select_main_node = fetch_main_select_from_select_stmt_mysql(select_stmt_node)
        with_clause_node = tree_node.get_children_by_path(['sqlStatements',
                                                           'sqlStatement',
                                                           'dmlStatement', 'withStatement'])
        if len(with_clause_node) > 0:
            return None
        target_list_node = select_main_node.get_child_by_value('selectElements')
        assert isinstance(target_list_node, TreeNode)
        select_list_ops = []
        for target_el in target_list_node.get_children_by_value('selectElement'):
            select_list_ops.append(target_el)
        having_clause_node = select_main_node.get_child_by_value('havingClause')
        having_cond = None
        if having_clause_node is not None:
            having_cond = having_clause_node.get_child_by_value('expression')
        from_clause_node = select_main_node.get_child_by_value('fromClause')
        from_tables = []
        if from_clause_node is not None:
            from_list_node = from_clause_node.get_child_by_value('tableSources')
            assert from_list_node is not None
            for table_ref_node in from_list_node.get_children_by_value('tableSource'):
                from_tables.append(table_ref_node)
        where_cond = None
        if from_clause_node.get_child_by_value('WHERE') is not None:
            where_cond = from_clause_node.get_child_by_value('expression')
        group_by_clause_node = select_main_node.get_child_by_value('groupByClause')
        group_by_ops = []
        if group_by_clause_node is not None:
            for group_by_item_node in group_by_clause_node.get_children_by_value('groupByItem'):
                if group_by_item_node.get_child_by_value('expression') is not None:
                    group_by_ops.append(group_by_item_node.get_child_by_value('expression'))
        # print({
        #     "having_cond": having_cond,
        #     "select_list": select_list_ops,
        #     "where_cond": where_cond,
        #     "group_by_cols": group_by_ops,
        #     "from_tables": from_tables
        # })
        return {
            "having_cond": having_cond,
            "select_list": select_list_ops,
            "where_cond": where_cond,
            "group_by_cols": group_by_ops,
            "from_tables": from_tables
        }
    else:
        assert dialect == 'oracle'
        select_stmt_node = tree_node.get_children_by_path(['unit_statement',
                                                           'data_manipulation_language_statements',
                                                           'select_statement', 'select_only_statement'])
        assert len(select_stmt_node) > 0
        select_stmt_node = select_stmt_node[0]
        with_clause_node = select_stmt_node.get_child_by_value('with_clause')
        if with_clause_node is not None:
            return None
        subquery_node = select_stmt_node.get_child_by_value('subquery')
        assert subquery_node is not None
        first_select_nodes = subquery_node.get_children_by_value('subquery_basic_elements')
        assert len(first_select_nodes) > 0
        first_select_node = first_select_nodes[0]
        query_block_node = first_select_node.get_child_by_value('query_block')
        assert query_block_node is not None
        target_list_node = query_block_node.get_child_by_value('selected_list')
        assert isinstance(target_list_node, TreeNode)
        select_list_ops = []
        for target_el in target_list_node.get_children_by_value('select_list_elements'):
            select_list_ops.append(target_el)
        having_clause_node = query_block_node.get_children_by_path(['group_by_clause', 'having_clause'])
        having_cond = None
        if len(having_clause_node) > 0:
            having_clause_node = having_clause_node[0]
            having_cond = having_clause_node.get_child_by_value('condition')
        where_cond = None
        if query_block_node.get_child_by_value('where_clause') is not None:
            where_cond = query_block_node.get_child_by_value('where_clause').get_child_by_value('condition')
        group_by_clause_node = query_block_node.get_child_by_value('group_by_clause')
        group_by_ops = []
        if group_by_clause_node is not None:
            for group_by_item_node in group_by_clause_node.get_children_by_value('group_by_elements'):
                if group_by_item_node.get_child_by_value('expression') is not None:
                    group_by_ops.append(group_by_item_node.get_child_by_value('expression'))
        from_clause_node = query_block_node.get_child_by_value('from_clause')
        from_tables = []
        if from_clause_node is not None:
            from_list_node = from_clause_node.get_child_by_value('table_ref_list')
            assert from_list_node is not None
            for table_ref_node in from_list_node.get_children_by_value('table_ref'):
                from_tables.append(table_ref_node)
        # print({
        #     "having_cond": having_cond,
        #     "select_list": select_list_ops,
        #     "where_cond": where_cond,
        #     "group_by_cols": group_by_ops,
        #     "from_tables": from_tables
        # })
        return {
            "having_cond": having_cond,
            "select_list": select_list_ops,
            "where_cond": where_cond,
            "group_by_cols": group_by_ops,
            "from_tables": from_tables
        }


def fetch_fulfilling_sql(select_pattern: SelectPattern, src_dialect: str, tgt_dialect: str, cur_sql, cur_sql_only_mode):
    if cur_sql_only_mode:
        assert cur_sql is not None
        all_sqls = [cur_sql]
    else:
        all_sqls = fetch_no_points_sql(src_dialect, tgt_dialect, None)
    random.shuffle(all_sqls)
    for sql in all_sqls:
        tree_node, _, _, _ = parse_tree(sql[src_dialect], src_dialect)
        assert tree_node is not None
        tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
        sql_components = analyze_sql_statement(tree_node, src_dialect)
        if sql_components is None:
            continue
        if select_pattern.where_cond_variable is not None and sql_components['where_cond'] is None:
            continue
        if (select_pattern.group_by_cols_variable is not None and
                len(select_pattern.group_by_cols_variable) != 0 and
                len(sql_components['group_by_cols']) == 0):
            continue
        if select_pattern.having_cond_variable is not None and sql_components['having_cond'] is None:
            continue
        if len(select_pattern.from_tables_variable) != 0 and len(sql_components['from_tables']) == 0:
            continue
        if len(sql_components['group_by_cols']) != 0 and select_pattern.group_by_cols_variable is None:
            continue
        final_flag = True
        for var in select_pattern.from_tables_variable:
            if isinstance(var, dict):
                flag = False
                for table_ref in sql_components['from_tables']:
                    if table_ref.get_child_by_value('join_qual') is not None:
                        flag = True
                        break
                if not flag:
                    final_flag = False
                    break
        if not final_flag:
            continue
        return sql, sql_components
    return None, None


def analyze_group_by_clause_node(node: TreeNode, dialect: str):
    group_by_variables = []
    # TODO:need further revision
    if dialect == 'pg':
        group_by_list_node = node.get_child_by_value('group_by_list')
        assert isinstance(group_by_list_node, TreeNode)
        for i in range(len(group_by_list_node.for_loop_sub_trees)):
            sub_tree = group_by_list_node.for_loop_sub_trees[i]
            slot = group_by_list_node.for_loop_slot[i]
            assert isinstance(slot, ForSlot)
            if len(slot.ele_slots) != 1:
                return []
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            table_ref_node = first_tree.get_child_by_value('group_by_item')
            assert isinstance(table_ref_node, TreeNode)
            if table_ref_node.slot is not None:
                if len(slot.ele_slots) > 1:
                    continue
                group_by_variables.append(slot.ele_slots[0])
        # cube_clause | rollup_clause | grouping_sets_clause
        for group_by_item_node in group_by_list_node.get_children_by_value('group_by_item'):
            assert isinstance(group_by_item_node, TreeNode)
            if group_by_item_node.get_child_by_value('cube_clause') is not None:
                cube_clause_node = group_by_item_node.get_child_by_value('cube_clause')
                assert isinstance(cube_clause_node, TreeNode)
                expr_list_node = cube_clause_node.get_child_by_value('expr_list')
                assert isinstance(expr_list_node, TreeNode)
                if len(expr_list_node.for_loop_slot) > 0:
                    assert len(expr_list_node.for_loop_slot) == 1
                    if len(expr_list_node.for_loop_slot[0].ele_slots) != 1:
                        assert False
                    slot = expr_list_node.for_loop_slot[0].ele_slots[0]
                    group_by_variables.append(slot)
            elif group_by_item_node.get_child_by_value('rollup_clause') is not None:
                rollup_clause_node = group_by_item_node.get_child_by_value('rollup_clause')
                assert isinstance(rollup_clause_node, TreeNode)
                expr_list_node = rollup_clause_node.get_child_by_value('expr_list')
                assert isinstance(expr_list_node, TreeNode)
                if len(expr_list_node.for_loop_slot) > 0:
                    assert len(expr_list_node.for_loop_slot) == 1
                    if len(expr_list_node.for_loop_slot[0].ele_slots) != 1:
                        assert False
                    slot = expr_list_node.for_loop_slot[0].ele_slots[0]
                    group_by_variables.append(slot)
            elif group_by_item_node.get_child_by_value('grouping_sets_clause') is not None:
                grouping_sets_node = group_by_item_node.get_child_by_value('grouping_sets_clause')
                grouping_sets_clause_node = grouping_sets_node.get_child_by_value('group_by_list')
                assert isinstance(grouping_sets_clause_node, TreeNode)
                if len(grouping_sets_clause_node.for_loop_slot[0].ele_slots) != 1:
                    assert False
                slot = grouping_sets_clause_node.for_loop_slot[0].ele_slots[0]
                group_by_variables.append(slot)
    elif dialect == 'mysql':
        for i in range(len(node.for_loop_sub_trees)):
            sub_tree = node.for_loop_sub_trees[i]
            slot = node.for_loop_slot[i]
            assert isinstance(slot, ForSlot)
            if len(slot.ele_slots) != 1:
                return None
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            group_by_elements_node = first_tree.get_child_by_value('groupByItem')
            assert isinstance(group_by_elements_node, TreeNode)
            if group_by_elements_node.slot is not None:
                if len(slot.ele_slots) > 1:
                    continue
                group_by_variables.append(slot.ele_slots[0])
    else:
        assert dialect == 'oracle'
        for i in range(len(node.for_loop_sub_trees)):
            sub_tree = node.for_loop_sub_trees[i]
            slot = node.for_loop_slot[i]
            assert isinstance(slot, ForSlot)
            if len(slot.ele_slots) != 1:
                return None
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            group_by_elements_node = first_tree.get_child_by_value('group_by_elements')
            assert isinstance(group_by_elements_node, TreeNode)
            if group_by_elements_node.slot is not None:
                if len(slot.ele_slots) > 1:
                    continue
                group_by_variables.append(slot.ele_slots[0])
        # cube_clause | rollup_clause | grouping_sets_clause
        for group_by_item_node in node.get_children_by_value('group_by_elements'):
            assert isinstance(group_by_item_node, TreeNode)
            if group_by_item_node.get_child_by_value('rollup_cube_clause') is not None:
                cube_clause_node = group_by_item_node.get_child_by_value('rollup_cube_clause')
                assert isinstance(cube_clause_node, TreeNode)
                if len(cube_clause_node.for_loop_slot) > 0:
                    assert len(cube_clause_node.for_loop_slot) == 1
                    if len(cube_clause_node.for_loop_slot[0].ele_slots) != 1:
                        assert False
                    slot = cube_clause_node.for_loop_slot[0].ele_slots[0]
                    group_by_variables.append(slot)
            elif group_by_item_node.get_child_by_value('grouping_sets_clause') is not None:
                grouping_sets_node = group_by_item_node.get_child_by_value('grouping_sets_clause')
                assert isinstance(grouping_sets_node, TreeNode)
                if len(grouping_sets_node.for_loop_slot) > 0:
                    assert len(grouping_sets_node.for_loop_slot) == 1
                    slot = grouping_sets_node.for_loop_slot[0].ele_slots[0]
                    group_by_variables.append(slot)
    return group_by_variables


def analyze_from_clause_node(node: TreeNode, dialect: str):
    if dialect == 'pg':
        from_list_node = node.get_child_by_value('from_list')
        assert from_list_node is not None
        assert isinstance(from_list_node, TreeNode)
        tables_slots = []
        for i in range(len(from_list_node.for_loop_sub_trees)):
            sub_tree = from_list_node.for_loop_sub_trees[i]
            slot = from_list_node.for_loop_slot[i]
            assert isinstance(slot, ForSlot)
            if len(slot.ele_slots) != 1:
                return None
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            table_ref_node = first_tree.get_child_by_value('table_ref')
            assert isinstance(table_ref_node, TreeNode)
            if table_ref_node.slot is not None:
                tables_slots.append(slot.ele_slots[0])
        for table_ref in from_list_node.get_children_by_value('table_ref'):
            if table_ref.slot is not None and table_ref.for_slot_ancestor is None:
                tables_slots.append(table_ref.slot)
            else:
                if table_ref.get_child_by_value('join') is not None:
                    to_join_table_ref = table_ref.get_child_by_value('table_ref')
                    join_cond = table_ref.get_children_by_path(['join_qual', 'a_expr'])
                    assert len(join_cond) == 1
                    join_cond = join_cond[0]
                    assert to_join_table_ref is not None
                    assert to_join_table_ref.slot is not None and join_cond.slot is not None
                    be_joined_table = table_ref.get_child_by_value('relation_expr')
                    assert be_joined_table is not None and be_joined_table.slot is not None
                    tables_slots.append({
                        'be_joined_table': be_joined_table.slot,
                        'join_cond': join_cond.slot,
                        'join_tbl': to_join_table_ref.slot
                    })
        return tables_slots
    elif dialect == 'mysql':
        from_list_node = node.get_child_by_value('tableSources')
        assert from_list_node is not None
        assert isinstance(from_list_node, TreeNode)
        tables_slots = []
        for i in range(len(from_list_node.for_loop_sub_trees)):
            sub_tree = from_list_node.for_loop_sub_trees[i]
            slot = from_list_node.for_loop_slot[i]
            assert isinstance(slot, ForSlot)
            if len(slot.ele_slots) != 1:
                return None
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            table_ref_node = first_tree.get_child_by_value('tableSource')
            assert isinstance(table_ref_node, TreeNode)
            if table_ref_node.slot is not None:
                tables_slots.append(slot.ele_slots[0])
        for table_ref in from_list_node.get_children_by_value('tableSource'):
            if table_ref.slot is not None and table_ref.for_slot_ancestor is None:
                tables_slots.append(table_ref.slot)
            else:
                if table_ref.get_child_by_value('joinPart') is not None:
                    join_part_node = table_ref.get_child_by_value('joinPart')
                    to_join_table_ref = join_part_node.get_child_by_value('tableSourceItem')
                    join_cond = join_part_node.get_children_by_path(['joinSpec', 'expression'])
                    assert len(join_cond) == 1
                    join_cond = join_cond[0]
                    assert to_join_table_ref is not None
                    assert to_join_table_ref.slot is not None and join_cond.slot is not None
                    be_joined_table = table_ref.get_child_by_value('tableSourceItem')
                    assert be_joined_table is not None and be_joined_table.slot is not None
                    tables_slots.append({
                        'be_joined_table': be_joined_table.slot,
                        'join_cond': join_cond.slot,
                        'join_tbl': to_join_table_ref.slot
                    })
        return tables_slots
    elif dialect == 'oracle':
        from_list_node = node.get_child_by_value('table_ref_list')
        assert from_list_node is not None
        assert isinstance(from_list_node, TreeNode)
        tables_slots = []
        for i in range(len(from_list_node.for_loop_sub_trees)):
            sub_tree = from_list_node.for_loop_sub_trees[i]
            slot = from_list_node.for_loop_slot[i]
            assert isinstance(slot, ForSlot)
            if len(slot.ele_slots) != 1:
                return None
            first_tree = sub_tree['first_tree']
            assert isinstance(first_tree, TreeNode)
            table_ref_node = first_tree.get_child_by_value('table_ref')
            assert isinstance(table_ref_node, TreeNode)
            if table_ref_node.slot is not None:
                tables_slots.append(slot.ele_slots[0])
        for table_ref in from_list_node.get_children_by_value('table_ref'):
            if table_ref.slot is not None and table_ref.for_slot_ancestor is None:
                tables_slots.append(table_ref.slot)
            else:
                if table_ref.get_child_by_value('join_clause') is not None:
                    join_clause_node = table_ref.get_child_by_value('join_clause')
                    to_join_table_ref = join_clause_node.get_child_by_value('table_ref_aux')
                    join_cond = join_clause_node.get_children_by_path(['join_on_part', 'condition'])
                    assert len(join_cond) == 1
                    join_cond = join_cond[0]
                    assert to_join_table_ref is not None
                    assert to_join_table_ref.slot is not None and join_cond.slot is not None
                    be_joined_table = table_ref.get_child_by_value('table_ref_aux')
                    assert be_joined_table is not None and be_joined_table.slot is not None
                    tables_slots.append({
                        'be_joined_table': be_joined_table.slot,
                        'join_cond': join_cond.slot,
                        'join_tbl': to_join_table_ref.slot
                    })
        return tables_slots


def analyze_select_stmt(tree_node: TreeNode, dialect: str):
    select_pattern = SelectPattern()
    if dialect == 'mysql':
        assert tree_node.value == 'querySpecification' or tree_node.value == 'querySpecificationNointo'
        select_list_variables = []
        target_list_node = tree_node.get_child_by_value('selectElements')
        assert isinstance(target_list_node, TreeNode)
        for for_slot in target_list_node.for_loop_slot:
            assert isinstance(for_slot, ForSlot)
            if len(for_slot.ele_slots) != 1:
                continue
            select_list_variables.append(for_slot.ele_slots[0])
        from_clause_node = tree_node.get_child_by_value('fromClause')
        from_clause_variables = analyze_from_clause_node(from_clause_node, dialect)
        where_clause_node = from_clause_node.get_child_by_value('WHERE')
        where_clause_variable = None
        if where_clause_node is not None:
            a_expr_node = from_clause_node.get_child_by_value('expression')
            if a_expr_node is not None and a_expr_node.slot is not None:
                where_clause_variable = a_expr_node.slot
        group_by_clause_node = tree_node.get_child_by_value('groupByClause')
        group_by_variable = []
        if group_by_clause_node is not None:
            group_by_variable = analyze_group_by_clause_node(group_by_clause_node, dialect)
        having_clause_node = tree_node.get_child_by_value('havingClause')
        having_clause_variable = None
        if having_clause_node is not None:
            a_expr_node = having_clause_node.get_child_by_value('expression')
            if a_expr_node is not None and a_expr_node.slot is not None:
                having_clause_variable = a_expr_node.slot
        select_pattern.select_list_variable = select_list_variables
        select_pattern.from_tables_variable = from_clause_variables
        select_pattern.where_cond_variable = where_clause_variable
        select_pattern.group_by_cols_variable = group_by_variable
        select_pattern.having_cond_variable = having_clause_variable
        return select_pattern
    elif dialect == 'pg':
        assert tree_node.value == 'simple_select_pramary'
        select_list_variables = []
        if tree_node.get_child_by_value('opt_target_list') is not None:
            target_list_node = tree_node.get_child_by_value('opt_target_list')
            assert isinstance(target_list_node, TreeNode)
            target_list_node = target_list_node.get_child_by_value('target_list')
        else:
            target_list_node = tree_node.get_child_by_value('target_list')
        assert isinstance(target_list_node, TreeNode)
        for for_slot in target_list_node.for_loop_slot:
            assert isinstance(for_slot, ForSlot)
            if len(for_slot.ele_slots) != 1:
                continue
            select_list_variables.append(for_slot.ele_slots[0])
        from_clause_node = tree_node.get_child_by_value('from_clause')
        from_clause_variables = analyze_from_clause_node(from_clause_node, dialect)
        where_clause_node = tree_node.get_child_by_value('where_clause')
        where_clause_variable = None
        if where_clause_node is not None:
            a_expr_node = where_clause_node.get_child_by_value('a_expr')
            if a_expr_node is not None and a_expr_node.slot is not None:
                where_clause_variable = a_expr_node.slot
        group_by_clause_node = tree_node.get_child_by_value('group_clause')
        group_by_variable = []
        if group_by_clause_node is not None:
            group_by_variable = analyze_group_by_clause_node(group_by_clause_node, dialect)
        having_clause_node = tree_node.get_child_by_value('having_clause')
        having_clause_variable = None
        if having_clause_node is not None:
            a_expr_node = having_clause_node.get_child_by_value('a_expr')
            if a_expr_node is not None and a_expr_node.slot is not None:
                having_clause_variable = a_expr_node.slot
        select_pattern.select_list_variable = select_list_variables
        select_pattern.from_tables_variable = from_clause_variables
        select_pattern.where_cond_variable = where_clause_variable
        select_pattern.group_by_cols_variable = group_by_variable
        select_pattern.having_cond_variable = having_clause_variable
        return select_pattern
    else:
        assert dialect == 'oracle'
        assert tree_node.value == 'query_block'
        select_list_variables = []
        target_list_node = tree_node.get_child_by_value('selected_list')
        assert isinstance(target_list_node, TreeNode)
        for for_slot in target_list_node.for_loop_slot:
            assert isinstance(for_slot, ForSlot)
            if len(for_slot.ele_slots) != 1:
                continue
            select_list_variables.append(for_slot.ele_slots[0])
        from_clause_node = tree_node.get_child_by_value('from_clause')
        from_clause_variables = analyze_from_clause_node(from_clause_node, dialect)
        where_clause_node = tree_node.get_child_by_value('where_clause')
        where_clause_variable = None
        if where_clause_node is not None:
            condition_node = where_clause_node.get_child_by_value('condition')
            if condition_node is not None and condition_node.slot is not None:
                where_clause_variable = condition_node.slot
        group_by_clause_node = tree_node.get_child_by_value('group_by_clause')
        group_by_variable = []
        having_clause_variable = None
        if group_by_clause_node is not None:
            group_by_variable = analyze_group_by_clause_node(group_by_clause_node, dialect)
            having_clause_node = group_by_clause_node.get_child_by_value('having_clause')
            if having_clause_node is not None:
                cond_node = having_clause_node.get_child_by_value('condition')
                if cond_node is not None and cond_node.slot is not None:
                    having_clause_variable = cond_node.slot
        select_pattern.select_list_variable = select_list_variables
        select_pattern.from_tables_variable = from_clause_variables
        select_pattern.where_cond_variable = where_clause_variable
        select_pattern.group_by_cols_variable = group_by_variable
        select_pattern.having_cond_variable = having_clause_variable
        return select_pattern


def get_child_pattern_value(for_slot: ForSlot, for_slot_tree, var_value_map):
    assert isinstance(for_slot, ForSlot)
    res = ''
    for j in range(len(var_value_map[for_slot.ele_slots[0]])):
        for i in range(len(for_slot.sub_ele_slots)):
            var_value_map[for_slot.sub_ele_slots[i]] = var_value_map[for_slot.ele_slots[i]][j]
            if j != 0:
                res = res + for_slot.strip_str
            first_tree_node = for_slot_tree['first_tree']
            value, _, _ = get_pattern_value(first_tree_node, var_value_map)
            assert value != ''
            res = res + value
    return res


def rep_slot_with_value(root_node: TreeNode, dialect, variable_to_value_map):
    if root_node.slot is not None:
        if isinstance(root_node.slot, ValueSlot):
            if root_node.slot in variable_to_value_map:
                new_node = TreeNode(variable_to_value_map[root_node.slot].str_value(), dialect, True)
                root_node.father.replace_child(root_node, new_node)
            return None, None
        else:
            assert isinstance(root_node.slot, ForSlot)
            assert root_node.for_slot_ancestor is not None
            return root_node.for_slot_ancestor, root_node.for_slot_ancestor_id
    i = 0
    while i < len(root_node.children):
        for_slot_ancestor, for_slot_ancestor_id = rep_slot_with_value(root_node.children[i], dialect,
                                                                      variable_to_value_map)
        if for_slot_ancestor is not None:
            if root_node != for_slot_ancestor:
                return for_slot_ancestor, for_slot_ancestor_id
            else:
                for_slot = root_node.for_loop_slot[for_slot_ancestor_id]
                flag = True
                assert isinstance(for_slot, ForSlot)
                for ele_slot in for_slot.ele_slots:
                    if ele_slot not in variable_to_value_map:
                        flag = False
                if not flag:
                    i = i + len(root_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
                    continue
                for_slot_tree_node = root_node.for_loop_sub_trees[for_slot_ancestor_id]
                value = get_child_pattern_value(for_slot, for_slot_tree_node, variable_to_value_map)
                rep_nodes = []
                max_i = i + len(root_node.for_loop_sub_trees[for_slot_ancestor_id]['first_tree'].children)
                k = i
                while k < max_i:
                    rep_nodes.append(root_node.children[i])
                    k += 1
                i = i + 1
                new_node = TreeNode(value, dialect, True)
                for node in rep_nodes:
                    if new_node is not None:
                        root_node.replace_child(node, new_node)
                        new_node = None
                    else:
                        root_node.children.remove(node)
        else:
            i = i + 1
    return None, None


def generate_select_stmt_pattern(select_stmt_node: TreeNode, src_dialect: str, tgt_dialect: str,
                                 variable_value_map: dict, cur_sql, only_cur_sql_mode: bool):
    select_pattern = analyze_select_stmt(select_stmt_node, src_dialect)
    sql, sql_components = fetch_fulfilling_sql(select_pattern, src_dialect, tgt_dialect, cur_sql, only_cur_sql_mode)
    if sql is None:
        return False
    flag = False
    if len(select_pattern.from_tables_variable) > 0:
        to_join_from_table = []
        table_list = []
        for ele in select_pattern.from_tables_variable:
            if isinstance(ele, dict):
                to_join_from_table.append(ele)
            else:
                table_list.append(ele)
        for join_value in to_join_from_table:
            """
            {
                'be_joined_table': be_joined_table.slot,
                'join_cond': join_cond.slot,
                'join_tbl': to_join_table_ref.slot
            }
            """
            for table_ref_node in sql_components['from_tables']:
                if src_dialect == 'pg':
                    if table_ref_node.get_child_by_value('join_qual') is not None:
                        join_qual_node = table_ref_node.get_child_by_value('join_qual')
                        assert isinstance(join_qual_node, TreeNode)
                        a_expr_node = join_qual_node.get_child_by_value('a_expr')
                        assert isinstance(a_expr_node, TreeNode)
                        variable_value_map[join_value['join_cond']] = Operand(a_expr_node, BaseType(''))
                        join_tbl_node = table_ref_node.get_child_by_value('table_ref')
                        assert join_tbl_node is not None
                        variable_value_map[join_value['join_tbl']] = Operand(join_tbl_node, BaseType(''))
                        be_joined_table_node = table_ref_node.get_child_by_value('relation_expr')
                        if be_joined_table_node is None:
                            be_joined_table_node = table_ref_node.get_child_by_value('select_with_parens')
                        alias_node = table_ref_node.get_child_by_value('opt_alias_clause')
                        if alias_node is not None:
                            final_node = TreeNode('father_node', src_dialect, False)
                            final_node.add_child(be_joined_table_node)
                            final_node.add_child(alias_node)
                            variable_value_map[join_value['be_joined_table']] = Operand(final_node, BaseType(''))
                        else:
                            variable_value_map[join_value['be_joined_table']] = Operand(be_joined_table_node,
                                                                                        BaseType(''))
                        sql_components['from_tables'].remove(table_ref_node)
                        break
                elif src_dialect == 'oracle':
                    if table_ref_node.get_child_by_value('join_clause') is not None:
                        join_clause_node = table_ref_node.get_child_by_value('join_clause')
                        assert isinstance(join_clause_node, TreeNode)
                        join_cond_node = join_clause_node.get_children_by_path(['join_on_part', 'condition'])
                        assert len(join_cond_node) == 1
                        join_cond_node = join_cond_node[0]
                        assert isinstance(join_cond_node, TreeNode)
                        variable_value_map[join_value['join_cond']] = Operand(join_cond_node, BaseType(''))
                        join_tbl_node = join_clause_node.get_child_by_value('table_ref_aux')
                        assert join_tbl_node is not None
                        variable_value_map[join_value['join_tbl']] = Operand(table_ref_node, BaseType(''))
                        be_joined_table_node = table_ref_node.get_child_by_value('table_ref_aux')
                        assert be_joined_table_node is not None
                        variable_value_map[join_value['be_joined_table']] = Operand(be_joined_table_node, BaseType(''))
                        sql_components['from_tables'].remove(table_ref_node)
                        break
                elif src_dialect == 'mysql':
                    if table_ref_node.get_child_by_value('joinPart') is not None:
                        join_clause_node = table_ref_node.get_child_by_value('joinPart')
                        assert isinstance(join_clause_node, TreeNode)
                        join_cond_node = join_clause_node.get_children_by_path(['joinSpec', 'expression'])
                        assert len(join_cond_node) == 1
                        join_cond_node = join_cond_node[0]
                        assert isinstance(join_cond_node, TreeNode)
                        variable_value_map[join_value['join_cond']] = Operand(join_cond_node, BaseType(''))
                        join_tbl_node = join_clause_node.get_child_by_value('tableSourceItem')
                        assert join_tbl_node is not None
                        variable_value_map[join_value['join_tbl']] = Operand(table_ref_node, BaseType(''))
                        be_joined_table_node = table_ref_node.get_child_by_value('tableSourceItem')
                        assert be_joined_table_node is not None
                        variable_value_map[join_value['be_joined_table']] = Operand(be_joined_table_node, BaseType(''))
                        sql_components['from_tables'].remove(table_ref_node)
                        break
                else:
                    assert False
        assert len(table_list) == 0 or len(table_list) == 1
        if len(table_list) == 1:
            res = []
            for table_ref_node in sql_components['from_tables']:
                res.append(Operand(table_ref_node, BaseType('')))
            variable_value_map[table_list[0]] = res
            flag = True
    if select_pattern.where_cond_variable is not None:
        assert sql_components['where_cond'] is not None
        variable_value_map[select_pattern.where_cond_variable] = Operand(sql_components['where_cond'], BaseType(''))
    if select_pattern.having_cond_variable is not None:
        assert sql_components['having_cond'] is not None
        variable_value_map[select_pattern.having_cond_variable] = Operand(sql_components['having_cond'], BaseType(''))
    if len(select_pattern.group_by_cols_variable) > 0:
        assert len(select_pattern.group_by_cols_variable) == 1
        group_by_list_vars = []
        group_by_ele_vars = []
        for var in select_pattern.group_by_cols_variable:
            if isinstance(var.get_type(), ListType):
                group_by_list_vars.append(var)
            else:
                group_by_ele_vars.append(var)
        for var in group_by_ele_vars:
            if len(sql_components['group_by_cols']) > 0:
                variable_value_map[var] = Operand(sql_components['group_by_cols'][0], BaseType(''))
                sql_components['group_by_cols'].pop(0)
            else:
                assert False
        assert len(group_by_list_vars) <= 1
        if len(group_by_list_vars) == 1:
            group_by_var = group_by_list_vars[0]
            assert isinstance(group_by_var, ValueSlot)
            if isinstance(group_by_var.get_type(), ListType) and isinstance(group_by_var.get_type().element_type,
                                                                            ListType):
                if len(sql_components['group_by_cols']) == 1:
                    res1 = [Operand(sql_components['group_by_cols'][0], BaseType(''))]
                    variable_value_map[group_by_var] = [res1]
                else:
                    assert len(sql_components['group_by_cols']) > 1
                    res1 = [Operand(op, BaseType('')) for op in sql_components['group_by_cols']]
                    res2 = [Operand(op, BaseType('')) for op in sql_components['group_by_cols']]
                    res2.pop(random.randint(0, len(res2) - 1))
                    variable_value_map[group_by_var] = [res1, res2]
            elif isinstance(group_by_var.get_type(), ListType):
                res = [Operand(op, BaseType('')) for op in sql_components['group_by_cols']]
                variable_value_map[group_by_var] = res
    if flag:
        if select_pattern.select_list_variable is not None:
            select_var = []
            select_list_var = []
            for var in select_pattern.select_list_variable:
                if isinstance(var.get_type(), ListType):
                    select_list_var.append(var)
                else:
                    select_var.append(var)
            for var in select_var:
                if var in variable_value_map:
                    continue
                if len(sql_components['select_list']) > 0:
                    variable_value_map[var] = Operand(sql_components['select_list'][0], BaseType(''))
                    sql_components['select_list'].pop(0)
                else:
                    assert False
            assert len(select_list_var) <= 1
            if len(select_list_var) == 1 and select_list_var[0] not in variable_value_map:
                res = [Operand(op, BaseType('')) for op in sql_components['select_list']]
                variable_value_map[select_list_var[0]] = res
    rep_slot_with_value(select_stmt_node, src_dialect, variable_value_map)
    return True


def dfs_fulfill_node(tree_node, src_dialect, variable_value_map: dict, node_to_usable_cols: dict):
    pass


def fetch_upper_select(tree_node: TreeNode, dialect: str):
    if dialect == 'mysql':
        while (tree_node is not None and tree_node.value != 'querySpecificationNointo'
               and tree_node.value != 'querySpecification'):
            tree_node = tree_node.father
        assert tree_node.value == 'querySpecificationNointo' or tree_node.value == 'querySpecification'
        return tree_node
    elif dialect == 'pg':
        while (tree_node is not None and tree_node.value != 'simple_select_pramary'
               and tree_node.value != 'select_no_parens'):
            tree_node = tree_node.father
        assert tree_node.value == 'simple_select_pramary' or tree_node.value == 'select_no_parens'
        if tree_node.value == 'select_no_parens':
            tree_node = fetch_all_simple_select_from_select_stmt_pg(tree_node)
        return tree_node
    elif dialect == 'oracle':
        while tree_node is not None and tree_node.value != 'query_block':
            tree_node = tree_node.father
        assert tree_node.value == 'query_block'
        return tree_node
    else:
        assert False


def fulfill_remain_variable_tree(tree_node: TreeNode, src_dialect: str,
                                 variable_value_map: dict, node_to_usable_cols: dict,
                                 aggregate_slot_set: set, execute_env: ExecutionEnv,
                                 specify_slot: ValueSlot | None = None):
    upper_select_node = fetch_upper_select(tree_node, src_dialect)
    if src_dialect == 'mysql':
        group_by_clause_node = upper_select_node.get_child_by_value('groupByClause')
    elif src_dialect == 'pg':
        group_by_clause_node = upper_select_node.get_child_by_value('group_clause')
    elif src_dialect == 'oracle':
        group_by_clause_node = upper_select_node.get_child_by_value('group_by_clause')
    else:
        assert False
    if (tree_node.slot is not None or len(tree_node.for_loop_sub_trees) > 0
            or tree_node.ori_pattern_string is not None):
        if upper_select_node not in node_to_usable_cols:
            # TODO: haven't consider WITH cte right know
            if src_dialect == 'mysql':
                from_clause_node = upper_select_node.get_child_by_value('fromClause')
                group_by_clause_node = upper_select_node.get_child_by_value('groupByClause')
            elif src_dialect == 'pg':
                from_clause_node = upper_select_node.get_child_by_value('from_clause')
                group_by_clause_node = upper_select_node.get_child_by_value('group_clause')
            elif src_dialect == 'oracle':
                from_clause_node = upper_select_node.get_child_by_value('from_clause')
                group_by_clause_node = upper_select_node.get_child_by_value('group_by_clause')
            else:
                assert False
            assert from_clause_node is not None
            from_clause, _, _ = get_pattern_value(from_clause_node, variable_value_map)
            sql = f"SELECT * {from_clause}"
            if group_by_clause_node is not None:
                group_by_clause, _, _ = get_pattern_value(group_by_clause_node, variable_value_map)
                sql = sql + f" {group_by_clause}"
            res = analysis_sql(sql, src_dialect)
            select_stmts = res['select_stmts']
            assert len(select_stmts) == 1
            select_stmt = select_stmts[0]
            node_to_usable_cols[upper_select_node] = {
                "cols": select_stmt['cols'],
                "group_by_cols": select_stmt['group_by_cols']
            }
    if tree_node.slot is not None:
        if tree_node.slot in variable_value_map:
            return None, None
        if specify_slot is not None and specify_slot != tree_node.slot:
            return None, None
        if isinstance(tree_node.slot, ForSlot):
            return tree_node.for_slot_ancestor, tree_node.for_slot_ancestor_id
        if group_by_clause_node is not None and tree_node.slot not in aggregate_slot_set:
            usable_cols = node_to_usable_cols[upper_select_node]['group_by_cols']
        else:
            usable_cols = node_to_usable_cols[upper_select_node]['cols']
        assert isinstance(tree_node.slot, ValueSlot)
        if tree_node.slot.udf_func is not None:
            udf_func = tree_node.slot.udf_func
            assert isinstance(udf_func, UdfFunction)
            for arg_slot in udf_func.arg_slots:
                if isinstance(arg_slot, ValueSlot) and arg_slot not in variable_value_map:
                    fulfill_remain_variable_tree(upper_select_node, src_dialect,
                                                 variable_value_map, node_to_usable_cols,
                                                 aggregate_slot_set, execute_env, arg_slot)
                    assert arg_slot in variable_value_map
            value = udf_func.execute(variable_value_map, upper_select_node, execute_env, src_dialect, True)
            variable_value_map[tree_node.slot] = value
        else:
            value = tree_node.slot.generate_value(usable_cols, tree_node, variable_value_map,
                                                  None, src_dialect, execute_env)
            variable_value_map[tree_node.slot] = value
    elif tree_node.ori_pattern_string is not None:
        for slot_pos_pair in tree_node.pos_to_slot:
            slot = slot_pos_pair['slot']
            gen_value = slot.generate_value([], tree_node, variable_value_map,
                                            None, src_dialect, execute_env)
            variable_value_map[slot] = gen_value
    i = 0
    while i < len(tree_node.children):
        for_ancestor, for_ancestor_id = fulfill_remain_variable_tree(tree_node.children[i], src_dialect,
                                                                     variable_value_map,
                                                                     node_to_usable_cols, aggregate_slot_set,
                                                                     execute_env)
        if for_ancestor is not None:
            if tree_node != for_ancestor:
                return for_ancestor, for_ancestor_id
            else:
                for_slot = tree_node.for_loop_slot[for_ancestor_id]
                for_slot_tree_node = tree_node.for_loop_sub_trees[for_ancestor_id]
                assert isinstance(for_slot, ForSlot)
                loop_ele_length = None
                for slot in for_slot.ele_slots:
                    if slot in variable_value_map:
                        loop_ele_length = len(variable_value_map[slot])
                for slot in for_slot.ele_slots:
                    if slot.udf_func is not None:
                        udf_func = slot.udf_func
                        assert isinstance(udf_func, UdfFunction)
                        for arg_slot in udf_func.arg_slots:
                            if isinstance(arg_slot, ValueSlot) and arg_slot not in variable_value_map:
                                fulfill_remain_variable_tree(upper_select_node, src_dialect,
                                                             variable_value_map, node_to_usable_cols,
                                                             aggregate_slot_set, execute_env, arg_slot)
                                assert arg_slot in variable_value_map
                        value = udf_func.execute(variable_value_map, upper_select_node, execute_env, src_dialect, True)
                        variable_value_map[slot] = value
                        assert isinstance(value, list)
                        if loop_ele_length is None:
                            loop_ele_length = len(value)
                        else:
                            assert loop_ele_length == len(value)
                if loop_ele_length is None:
                    loop_ele_length = random.randint(1, 3)
                for slot in for_slot.ele_slots:
                    if slot in variable_value_map:
                        continue
                    if group_by_clause_node is not None and slot in aggregate_slot_set:
                        usable_cols = node_to_usable_cols[upper_select_node]['cols']
                    else:
                        usable_cols = node_to_usable_cols[upper_select_node]['group_by_cols']
                    gen_value = slot.generate_value(usable_cols, tree_node, variable_value_map, loop_ele_length,
                                                    src_dialect, execute_env)
                    variable_value_map[slot] = gen_value
                i = i + len(tree_node.for_loop_sub_trees[for_ancestor_id]['first_tree'].children)
        else:
            i = i + 1
    return None, None
    #
    # if len(tree_node.for_loop_sub_trees) > 0:
    #     if specify_slot is not None:
    #         flag = False
    #         for for_loop in tree_node.for_loop_slot:
    #             for slot in for_loop.ele_slots:
    #                 if slot == specify_slot:
    #                     flag = True
    #         if not flag:
    #             return
    #     for for_loop in tree_node.for_loop_slot:
    #         assert isinstance(for_loop, ForSlot)
    #         loop_ele_length = None
    #         for slot in for_loop.ele_slots:
    #             if slot in variable_value_map:
    #                 loop_ele_length = len(variable_value_map[slot])
    #         for slot in for_loop.ele_slots:
    #             if slot.udf_func is not None:
    #                 udf_func = slot.udf_func
    #                 assert isinstance(udf_func, UdfFunction)
    #                 for arg_slot in udf_func.arg_slots:
    #                     if isinstance(arg_slot, ValueSlot) and arg_slot not in variable_value_map:
    #                         fulfill_remain_variable_tree(upper_select_node, src_dialect, db_name,
    #                                                      variable_value_map, node_to_usable_cols,
    #                                                      aggregate_slot_set, execute_env, arg_slot)
    #                         assert arg_slot in variable_value_map
    #                 value = udf_func.execute(variable_value_map, upper_select_node, execute_env, src_dialect)
    #                 variable_value_map[slot] = value
    #                 assert isinstance(value, list)
    #                 if loop_ele_length is None:
    #                     loop_ele_length = len(value)
    #                 else:
    #                     assert loop_ele_length == len(value)
    #         if loop_ele_length is None:
    #             loop_ele_length = random.randint(1, 3)
    #         for slot in for_loop.ele_slots:
    #             if slot in variable_value_map:
    #                 continue
    #             if group_by_clause_node is not None and slot in aggregate_slot_set:
    #                 usable_cols = node_to_usable_cols[upper_select_node]['cols']
    #             else:
    #                 usable_cols = node_to_usable_cols[upper_select_node]['group_by_cols']
    #             gen_value = slot.generate_value(usable_cols, tree_node, variable_value_map, loop_ele_length)
    #             variable_value_map[slot] = gen_value
    # elif tree_node.ori_pattern_string is not None:
    #     assert False
    # else:
    #     for child in tree_node.children:
    #         fulfill_remain_variable_tree(child, src_dialect,
    #                                      db_name, variable_value_map,
    #                                      node_to_usable_cols, aggregate_slot_set, execute_env)


def add_pattern_point(point: Point, cur_sql: None | dict, backup_points: list[dict], aggressive_flag: bool,
                      execute_env: ExecutionEnv, only_cur_sql_mode: bool) -> dict | None:
    src_pattern_tree = parse_pattern_tree(point.point_type, point.src_pattern, point.src_dialect)
    node = src_pattern_tree
    variable_map = {}
    src_dialect = point.src_dialect
    tgt_dialect = point.tgt_dialect
    if src_dialect == 'mysql':
        if node.value == 'querySpecification' or node.value == 'querySpecificationNointo':
            flag = generate_select_stmt_pattern(node, src_dialect, tgt_dialect, variable_map, cur_sql, only_cur_sql_mode)
            if not flag:
                return None
        else:
            if node.value == 'root':
                node = node.get_child_by_value('sqlStatements')
                assert node is not None
            if node.value == 'sqlStatements':
                node = node.get_child_by_value('sqlStatement')
                assert node is not None
            if node.value == 'sqlStatement':
                node = node.get_child_by_value('dmlStatement')
                assert node is not None
            if node.value == 'dmlStatement':
                node = node.get_child_by_value('selectStatement')
                assert node is not None
            simple_select_nodes = fetch_all_simple_select_from_select_stmt_mysql(node)
            assert len(simple_select_nodes) == 1
            for select_node in simple_select_nodes:
                flag = generate_select_stmt_pattern(select_node, src_dialect, tgt_dialect, variable_map, cur_sql, only_cur_sql_mode)
                if not flag:
                    return None
    elif src_dialect == 'pg':
        if node.value == 'simple_select_pramary':
            flag = generate_select_stmt_pattern(node, src_dialect, tgt_dialect, variable_map, cur_sql,
                                                only_cur_sql_mode)
            if not flag:
                return None
        else:
            if node.value == 'root':
                node = node.get_child_by_value('stmtblock')
                assert node is not None
            if node.value == 'stmtblock':
                node = node.get_child_by_value('stmtmulti')
                assert node is not None
            if node.value == 'stmtmulti':
                node = node.get_child_by_value('stmt')
                assert node is not None
            if node.value == 'stmt':
                node = node.get_child_by_value('selectstmt')
                assert node is not None
            if node.value != 'select_no_parens':
                while node.get_child_by_value('select_no_parens') is None:
                    assert node.get_child_by_value('select_with_parens') is not None
                    node = node.get_child_by_value('select_with_parens')
                assert node.get_child_by_value('select_no_parens') is not None
                node = node.get_child_by_value('select_no_parens')
            select_clause_node = node.get_child_by_value('select_clause')
            select_stmts_tbd = dfs_select_clause(select_clause_node)
            for select_node in select_stmts_tbd:
                flag = generate_select_stmt_pattern(select_node, src_dialect, tgt_dialect, variable_map, cur_sql,
                                                    only_cur_sql_mode)
                if not flag:
                    return None
    else:
        assert src_dialect == 'oracle'
        if node.value == 'query_block':
            generate_select_stmt_pattern(node, src_dialect, tgt_dialect, variable_map, cur_sql, only_cur_sql_mode)
        else:
            if node.value == 'sql_script':
                node = node.get_child_by_value('unit_statement')
            if node.value == 'unit_statement':
                node = node.get_child_by_value('data_manipulation_language_statements')
            if node.value == 'select_statement':
                node = node.get_child_by_value('select_only_statement')
            if node.value == 'select_only_statement':
                node = node.get_child_by_value('subquery')
            assert node is not None
            simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(node)
            assert len(simple_select_nodes) == 1
            for select_node in simple_select_nodes:
                flag = generate_select_stmt_pattern(select_node, src_dialect, tgt_dialect, variable_map, cur_sql,
                                                    only_cur_sql_mode)
                if not flag:
                    return None
    if point.tag is not None and 'DB PARAMETER' in point.tag:
        for key, value in point.tag['DB PARAMETER'].items():
            flag = execute_env.add_param(key, value)
            if not flag:
                return None
    aggregate_slot_set = set()
    mark_in_aggregate_slot(src_pattern_tree, point.src_dialect, aggregate_slot_set)
    fulfill_remain_variable_tree(src_pattern_tree, src_dialect, variable_map,
                                 {}, aggregate_slot_set, execute_env)
    value, _, _ = get_pattern_value(src_pattern_tree, variable_map)
    gen_sql = {
        src_dialect: value,
        tgt_dialect: '',
        "points": [{
            "point": point.point_name,
            "num": 1
        }]
    }
    if only_cur_sql_mode:
        return gen_sql
    return merge_query(cur_sql, gen_sql, execute_env)
