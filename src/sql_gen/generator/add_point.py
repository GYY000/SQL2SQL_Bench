# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: add_point$
# @Author: 10379
# @Time: 2025/5/9 19:24
import json
import os
import random

from mysql.connector.utils import print_buffer

from antlr_parser.Tree import TreeNode
from sql_gen.generator.add_pattern_point import add_pattern_point
from sql_gen.generator.ele_type.operand_analysis import analysis_sql
from sql_gen.generator.ele_type.type_def import BaseType, NullType, is_num_type, is_str_type, is_time_type, \
    AnyValueType, OptionType, DateType, BoolType, TimestampType, IntType, StringGeneralType, NumberType, IntervalType, \
    IntGeneralType
from sql_gen.generator.element.Operand import Operand, ColumnOp
from sql_gen.generator.element.Pattern import ValueSlot, ForSlot
from sql_gen.generator.element.Point import Point
from sql_gen.generator.method import mark_in_aggregate_slot, fetch_fulfilled_sqls, merge_query, \
    add_point_to_point_dict, merge_trans_points
from sql_gen.generator.pattern_tree_parser import parse_pattern_tree
from sql_gen.generator.point_type.TranPointType import ClauseType, ExpressionType, LiteralType, ReservedKeywordType
from utils.CISpacelessSet import CISpacelessSet
from utils.ExecutionEnv import ExecutionEnv
from utils.tools import get_used_reserved_keyword_list, get_db_ids, get_data_path, \
    get_proj_root_path, load_gen_param

gen_param = load_gen_param()
possibility_of_using_new_sql = gen_param['using_new_sql_pos']


def get_sql_points(sql_dict: dict):
    return sql_dict.get('points', [])


def gen_no_transfer_value_type(type_to_build: BaseType, select_stmt: dict) -> Operand:
    for col in select_stmt['cols']:
        assert isinstance(col, ColumnOp)
        if strict_match(col.op_type, type_to_build):
            if random.randint(1, 5) == 1:
                return col
    if isinstance(type_to_build, IntGeneralType):
        res = Operand('0', IntGeneralType())
    elif is_num_type(type_to_build):
        res = Operand('0.0', NumberType())
    elif is_time_type(type_to_build):
        if random.randint(1, 2) == 1:
            res = Operand('CAST(null AS DATE)', type_to_build)
        else:
            res = Operand('DATE \'2025-05-30\'', type_to_build)
    elif isinstance(type_to_build, BoolType):
        res = Operand('TRUE', type_to_build)
    else:
        res = Operand('NULL', NullType())
    return res


def strict_match(type1: BaseType, type2: BaseType):
    if isinstance(type1, AnyValueType):
        return True
    return type(type1).__name__ == type(type2).__name__


def loose_match(type1, type2):
    if isinstance(type1, AnyValueType):
        return True
    if type(type1).__name__ == type(type2).__name__:
        return True
    elif is_num_type(type1) and is_num_type(type2):
        return True
    elif is_str_type(type1) and is_str_type(type2):
        return True
    elif is_time_type(type1) and is_time_type(type2):
        return True
    return False


def gen_pattern_value_function(point: Point, select_stmt: dict, backup_points: list[dict],
                               select_stmt_node: TreeNode, inside_aggregate_flag: bool,
                               execution_env: ExecutionEnv, where_flag=False) -> tuple[
    bool, list, Operand | None]:
    # if not inside aggregate then only cols inside the group by can be used
    add_points = []
    pattern = parse_pattern_tree(point.point_type, point.src_pattern, point.src_dialect)
    aggregate_slot_set = set()
    if not inside_aggregate_flag:
        mark_in_aggregate_slot(pattern, point.src_dialect, aggregate_slot_set)
    if len(aggregate_slot_set) > 0 and where_flag:
        return False, [], None
    if inside_aggregate_flag or len(aggregate_slot_set) > 0:
        if select_stmt['group_by_cols'] is None:
            return True, add_points, gen_no_transfer_value_type(point.return_type, select_stmt)
    filled_value = {}
    slot_to_point_number = {}
    added_points_slot = {}
    for ele in point.src_pattern.elements:
        if isinstance(ele, ValueSlot):
            if ele in added_points_slot:
                add_points += added_points_slot[ele]
                continue
            if (select_stmt['group_by_cols'] is None or inside_aggregate_flag or
                    ele in aggregate_slot_set or where_flag):
                use_group_by_cols_flag = False
            else:
                use_group_by_cols_flag = True
            flag, slot_add_points = fulfill_value_slot(ele, select_stmt, backup_points, use_group_by_cols_flag,
                                                       filled_value, slot_to_point_number, execution_env)
            add_points = merge_trans_points(add_points, slot_add_points)
            added_points_slot[ele] = slot_add_points
            if not flag:
                return False, [], None
        elif isinstance(ele, ForSlot):
            length = None
            for slot in ele.ele_slots:
                if slot in filled_value:
                    length = len(filled_value[slot])
            if length is None:
                length = random.randint(1, 3)
            for slot in ele.ele_slots:
                if (select_stmt['group_by_cols'] is None or inside_aggregate_flag
                        or slot in aggregate_slot_set or where_flag):
                    use_group_by_cols_flag = False
                else:
                    use_group_by_cols_flag = True
                flag, slot_add_points = fulfill_value_slot(slot, select_stmt, backup_points, use_group_by_cols_flag,
                                                           filled_value, slot_to_point_number, execution_env, length)
                add_points = merge_trans_points(add_points, slot_add_points)
                added_points_slot[slot] = slot_add_points
                if not flag:
                    return False, [], None
    add_point_to_point_dict(add_points, point)
    return True, add_points, Operand(
        point.src_pattern.fulfill_pattern({}, filled_value, select_stmt_node,
                                          execution_env, execution_env.dialect, True), point.return_type)


def fulfill_value_slot(slot: ValueSlot, select_stmt, backup_points: list[dict], use_group_by_flag: bool,
                       filled_value: dict, slot_to_point_number,
                       execution_env: ExecutionEnv, length=None) -> tuple[bool, list]:
    """
    {
        "cte": ctes,
        "select_stmts": select_stmts,
        "root_node": root_node
    }
    {
        "select_root_node": simple_select_node,
        "type": 'UNION',
        "cols": cols,
        "group_by_cols": group_by_cols
    }
    """
    new_add_points = []
    if slot in filled_value:
        assert slot in slot_to_point_number
        return True, slot_to_point_number[slot]

    if slot.udf_func is not None:
        for arg_slot in slot.udf_func.arg_slots:
            if isinstance(arg_slot, ValueSlot):
                flag, _ = fulfill_value_slot(arg_slot, select_stmt, backup_points, use_group_by_flag,
                                             filled_value, slot_to_point_number, execution_env)
                if not flag:
                    return False, new_add_points
        func_res = slot.udf_func.execute(filled_value, select_stmt['select_root_node'], execution_env,
                                         execution_env.dialect, True)
        if func_res is None:
            return False, new_add_points
        filled_value[slot] = func_res
        slot_to_point_number[slot] = new_add_points
        return True, new_add_points
    else:
        if use_group_by_flag:
            ops = select_stmt['group_by_cols']
        else:
            ops = select_stmt['cols']
        value = slot.generate_value(ops, select_stmt['select_root_node'], filled_value, length, execution_env.dialect,
                                    execution_env)
        if value is None:
            return False, new_add_points
        slot_to_point_number[slot] = new_add_points
        filled_value[slot] = value
    return True, new_add_points


reserved_keywords_list = get_used_reserved_keyword_list()


def get_used_alias_name(point, existing_alias):
    ori_name = point.point_name
    used_name = ''
    flag = True
    for word in ori_name:
        if not (word.isalnum() or word == ' '):
            flag = False
            break
    if flag:
        for splits in ori_name.split():
            if used_name != '':
                used_name = used_name + '_'
            used_name = used_name + splits
        used_name = used_name + 'COL'
    else:
        used_name = 'COL_ALIAS'
    num = 0
    used_name = used_name[:20]
    if (used_name.upper() in existing_alias
            or used_name.upper() in reserved_keywords_list['mysql'] or used_name.upper() in reserved_keywords_list['pg']
            or used_name.upper() in reserved_keywords_list['oracle']):
        while used_name + f'_{num}' in existing_alias:
            num += 1
        used_name = used_name + f'_{num}'
    return used_name


def add_op_to_select(op: Operand, point, src_dialect: str, tgt_dialect, select_stmt_node: TreeNode):
    if src_dialect == 'mysql':
        assert select_stmt_node.value == 'querySpecificationNointo' or select_stmt_node.value == 'querySpecification'
        select_elements_node = select_stmt_node.get_child_by_value('selectElements')
        assert select_elements_node is not None
        alias_list = []
        if select_elements_node.get_child_by_value('*') is not None:
            assert isinstance(select_elements_node, TreeNode)
            select_elements_node.children = []
        else:
            select_element_nodes = select_elements_node.get_children_by_value('selectElement')
            for select_element_node in select_element_nodes:
                alias = None
                if select_element_node.get_child_by_value('uid') is not None:
                    alias = str(select_element_node.get_child_by_value('uid')).strip('`')
                elif select_element_node.get_child_by_value('fullColumnName') is not None:
                    full_column_name_node = select_element_node.get_child_by_value('fullColumnName')
                    if full_column_name_node.get_child_by_value('dottedId') is not None:
                        alias = str(full_column_name_node.get_child_by_value('dottedId')).strip('.').strip('`')
                    else:
                        assert full_column_name_node.get_child_by_value('uid') is not None
                        alias = str(full_column_name_node.get_child_by_value('uid')).strip('`')
                if alias is not None:
                    alias_list.append(alias.upper())
            select_elements_node.add_child(TreeNode(',', src_dialect, True))
        select_element_node = TreeNode('selectElement', src_dialect, False)
        select_element_node.add_child(TreeNode(op.str_value(), src_dialect, True))
        if tgt_dialect != 'oracle':
            select_element_node.add_child(TreeNode('AS', src_dialect, True))
        select_element_node.add_child(TreeNode(get_used_alias_name(point, alias_list), src_dialect, True))
        select_elements_node.add_child(select_element_node)
    elif src_dialect == 'pg':
        assert select_stmt_node.value == 'simple_select_pramary'
        target_list_node = select_stmt_node.get_child_by_value('target_list')
        if target_list_node is None:
            opt_target_list = select_stmt_node.get_child_by_value('opt_target_list')
            target_list_node = opt_target_list.get_child_by_value('target_list')
        assert target_list_node is not None
        target_els = target_list_node.get_children_by_value('target_el')
        flag = False
        for target_el in target_els:
            if str(target_el) == '*':
                flag = True
        alis_list = []
        if flag:
            target_list_node.children = []
        else:
            target_list_node.add_child(TreeNode(',', src_dialect, True))
            for target_el in target_els:
                if target_el.get_child_by_value('collabel') is not None:
                    alis_list.append(str(target_el.get_child_by_value('collabel')).strip('"'))
                elif target_el.get_child_by_value('identifier') is not None:
                    alis_list.append(str(target_el.get_child_by_value('identifier')).strip('"'))
                else:
                    alis_list.append(str(target_el.get_child_by_value('a_expr')).strip('"'))
        target_el_node = TreeNode('target_el', src_dialect, False)
        target_el_node.add_child(TreeNode(op.str_value(), src_dialect, True))
        if tgt_dialect != 'oracle':
            target_el_node.add_child(TreeNode('AS', src_dialect, True))
        target_el_node.add_child(TreeNode(get_used_alias_name(point, alis_list), src_dialect, True))
        target_list_node.add_child(target_el_node)
    elif src_dialect == 'oracle':
        assert select_stmt_node.value == 'query_block'
        selected_list_node = select_stmt_node.get_child_by_value('selected_list')
        assert selected_list_node is not None
        alias_list = []
        if selected_list_node.get_child_by_value('*') is not None:
            selected_list_node.children = []
        else:
            for element in selected_list_node.get_children_by_value('select_list_elements'):
                if element.get_child_by_value('column_alias') is not None:
                    column_alias_node = element.get_child_by_value('column_alias')
                    if column_alias_node.get_child_by_value('identifier') is not None:
                        alias_list.append(str(column_alias_node.get_child_by_value('identifier')).strip('"').upper())
                    elif column_alias_node.get_child_by_value('quoted_string') is not None:
                        alias_list.append(
                            str(column_alias_node.get_child_by_value('string_literal')).strip('"').upper())
                else:
                    assert element.get_child_by_value('expression') is not None
                    alias_list.append(str(element.get_child_by_value('expression')).strip('"').upper())
            selected_list_node.add_child(TreeNode(',', src_dialect, True))
        select_list_elements_node = TreeNode('select_list_elements', src_dialect, False)
        expression_node = TreeNode('expression', src_dialect, False)
        expression_node.add_child(TreeNode(op.str_value(), src_dialect, True))
        select_list_elements_node.add_child(expression_node)
        select_list_elements_node.add_child(TreeNode(get_used_alias_name(point, alias_list), src_dialect, True))
        selected_list_node.add_child(select_list_elements_node)
    else:
        assert False


def add_op_to_where(op: Operand, dialect: str, select_stmt_node: TreeNode):
    if dialect == 'mysql':
        assert select_stmt_node.value == 'querySpecificationNointo' or select_stmt_node.value == 'querySpecification'
        from_clause_node = select_stmt_node.get_child_by_value('fromClause')
        assert from_clause_node is not None
        where_node = from_clause_node.get_child_by_value('WHERE')
        if where_node is None:
            from_clause_node.add_child(TreeNode('WHERE', dialect, True))
            where_expr_node = TreeNode('expression', dialect, False)
            where_expr_node.add_child(TreeNode(op.str_value(), dialect, True))
            from_clause_node.add_child(where_expr_node)
        else:
            where_expr_node = from_clause_node.get_child_by_value('expression')
            assert where_expr_node is not None
            where_expr_node.add_child(TreeNode('AND', dialect, True))
            where_expr_node.add_child(TreeNode(op.str_value(), dialect, True))
    elif dialect == 'pg':
        assert select_stmt_node.value == 'simple_select_pramary'
        assert select_stmt_node.get_child_by_value('SELECT') is not None
        if select_stmt_node.get_child_by_value('where_clause') is not None:
            where_clause_node = select_stmt_node.get_child_by_value('where_clause')
            a_expr_node = where_clause_node.get_child_by_value('a_expr')
            a_expr_node.add_child(TreeNode('AND', dialect, True))
        else:
            i = 0
            for i in range(len(select_stmt_node.children)):
                if (select_stmt_node.children[i].value == 'target_list' or
                        select_stmt_node.children[i].value == 'opt_target_list'):
                    break
            if len(select_stmt_node.children) > i + 1 and select_stmt_node.children[i + 1].value == 'into_clause':
                i = i + 1
            if len(select_stmt_node.children) > i + 1 and select_stmt_node.children[i + 1].value == 'from_clause':
                i = i + 1
            where_clause_node = TreeNode('where_clause', dialect, False)
            select_stmt_node.children.insert(i + 1, where_clause_node)
            where_clause_node.father = select_stmt_node
            where_clause_node.add_child(TreeNode('WHERE', dialect, True))
            a_expr_node = TreeNode('a_expr', dialect, False)
            where_clause_node.add_child(a_expr_node)
        a_expr_node.add_child(TreeNode(op.str_value(), dialect, True))
    else:
        assert dialect == 'oracle'
        assert select_stmt_node.value == 'query_block'
        if select_stmt_node.get_child_by_value('where_clause') is not None:
            where_clause_node = select_stmt_node.get_child_by_value('where_clause')
            condition_node = where_clause_node.get_child_by_value('condition')
            condition_node.add_child(TreeNode('AND', dialect, True))
        else:
            i = 0
            for i in range(len(select_stmt_node.children)):
                if (select_stmt_node.children[i].value == 'selected_list' or
                        select_stmt_node.children[i].value == 'selected_list'):
                    break
            if len(select_stmt_node.children) > i + 1 and select_stmt_node.children[i + 1].value == 'into_clause':
                i = i + 1
            if len(select_stmt_node.children) > i + 1 and select_stmt_node.children[i + 1].value == 'from_clause':
                i = i + 1
            where_clause_node = TreeNode('where_clause', dialect, False)
            select_stmt_node.children.insert(i + 1, where_clause_node)
            where_clause_node.father = select_stmt_node
            where_clause_node.add_child(TreeNode('WHERE', dialect, True))
            condition_node = TreeNode('condition', dialect, False)
            where_clause_node.add_child(condition_node)
        condition_node.add_child(TreeNode(op.str_value(), dialect, True))


def augment_literal(op: Operand, op_type: BaseType, select_stmt: dict, execution_env, predicate_flag: bool,
                    src_dialect: str, tgt_dialect: str) -> tuple[Operand | None, BaseType | None]:
    # currently only used for literal type translation points
    if isinstance(op_type, DateType):
        can_be_used_date_values = []
        group_by_date_cols = []
        for col_op in select_stmt['cols']:
            if isinstance(col_op, Operand) and loose_match(col_op.op_type, DateType()):
                can_be_used_date_values.append(col_op)
        if select_stmt['group_by_cols'] is not None:
            for col_op in select_stmt['group_by_cols']:
                if isinstance(col_op, Operand) and loose_match(col_op.op_type, DateType()):
                    group_by_date_cols.append(col_op)
        if len(can_be_used_date_values) == 0:
            return None, None
        if predicate_flag:
            used_op = random.choice(can_be_used_date_values)
            # predicate is only used for WHERE Clause
            if random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(MONTH FROM {op.str_value()}) = EXTRACT(MONTH FROM {used_op.str_value()})",
                                 BoolType())
            elif random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(YEAR FROM {op.str_value()}) - EXTRACT(YEAR FROM {used_op.str_value()}) < 2",
                                 BoolType())
            else:
                new_op = Operand(
                    f"EXTRACT(DAY FROM {op.str_value()}) - EXTRACT(DAY FROM {used_op.str_value()}) < {random.randint(1, 5)}",
                    BoolType())
            return new_op, BoolType()
        else:
            aggregate_flag = False
            if select_stmt['group_by_cols'] is not None:
                if len(group_by_date_cols) > 0:
                    used_op = random.choice(group_by_date_cols)
                else:
                    used_op = random.choice(can_be_used_date_values)
                    aggregate_flag = True
            else:
                used_op = random.choice(can_be_used_date_values)
            if random.randint(0, 1) == 0 and src_dialect in ['pg', 'oracle'] and tgt_dialect in ['pg', 'oracle']:
                new_op = Operand(f"{op.str_value()} - {used_op.str_value()}", IntType())
            if random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(YEAR FROM {op.str_value()}) - EXTRACT(YEAR FROM {used_op.str_value()})",
                                 IntType())
            elif random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(MONTH FROM {op.str_value()}) - EXTRACT(MONTH FROM {used_op.str_value()})",
                                 IntType())
            else:
                new_op = Operand(f"EXTRACT(DAY FROM {op.str_value()}) - EXTRACT(DAY FROM {used_op.str_value()})",
                                 IntType())
            if aggregate_flag:
                new_op = Operand(f"AVG({new_op.str_value()})", IntType())
            return new_op, IntType()
    elif isinstance(op_type, TimestampType):
        timestamp_ops = []
        group_by_timestamp_ops = []
        date_ops = []
        group_by_date_ops = []
        if select_stmt['group_by_cols'] is not None:
            for col_op in select_stmt['group_by_cols']:
                if isinstance(col_op, Operand) and isinstance(col_op.op_type, TimestampType):
                    group_by_timestamp_ops.append(col_op)
                if isinstance(col_op, Operand) and isinstance(col_op.op_type, DateType):
                    group_by_date_ops.append(col_op)
        for col_op in select_stmt['cols']:
            if isinstance(col_op, Operand) and isinstance(col_op.op_type, DateType):
                date_ops.append(col_op)
            elif isinstance(col_op, Operand) and isinstance(col_op.op_type, TimestampType):
                timestamp_ops.append(col_op)
        if len(timestamp_ops + group_by_timestamp_ops + date_ops + group_by_date_ops) == 0:
            return None, None
        if predicate_flag:
            if len(timestamp_ops) == 0 and len(date_ops) == 0:
                return None, None
            if len(timestamp_ops) < 2:
                timestamp_ops = timestamp_ops + date_ops
            used_op = random.choice(timestamp_ops)
            # predicate is only used for WHERE Clause
            if isinstance(used_op.op_type, TimestampType) and random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(HOUR FROM {op.str_value()}) - EXTRACT(HOUR FROM {used_op.str_value()}) < 2",
                                 BoolType())
            elif isinstance(used_op.op_type, TimestampType) and random.randint(0, 1) == 1:
                new_op = Operand(
                    f"EXTRACT(MINUTE FROM {op.str_value()}) - EXTRACT(MINUTE FROM {used_op.str_value()}) < {random.randint(5, 15)}",
                    BoolType())
            elif random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(MONTH FROM {op.str_value()}) = EXTRACT(MONTH FROM {used_op.str_value()})",
                                 BoolType())

            else:
                new_op = Operand(
                    f"EXTRACT(DAY FROM {op.str_value()}) - EXTRACT(DAY FROM {used_op.str_value()}) < {random.randint(1, 5)}",
                    BoolType())
            return new_op, BoolType()
        else:
            aggregate_flag = False
            if select_stmt['group_by_cols'] is not None:
                if len(group_by_timestamp_ops) == 0 and len(group_by_date_ops) == 0:
                    if len(timestamp_ops) < 2:
                        used_op = random.choice(timestamp_ops + date_ops)
                    else:
                        used_op = random.choice(timestamp_ops)
                    aggregate_flag = True
                else:
                    if len(group_by_timestamp_ops) < 2:
                        used_op = random.choice(group_by_timestamp_ops + group_by_date_ops)
                    else:
                        used_op = random.choice(group_by_timestamp_ops)
            else:
                if len(timestamp_ops) < 2:
                    timestamp_ops = timestamp_ops + date_ops
                used_op = random.choice(timestamp_ops)
            if (random.randint(0, 1) == 0 and isinstance(used_op.op_type, TimestampType) and
                    src_dialect in ['pg', 'oracle'] and tgt_dialect in ['pg', 'oracle']):
                new_op = Operand(f"{op.str_value()} - {used_op.str_value()}", IntType())
            elif isinstance(used_op, TimestampType) and random.randint(0, 1):
                new_op = Operand(f"EXTRACT(HOUR FROM {op.str_value()}) - EXTRACT(HOUR FROM {used_op.str_value()})",
                                 IntType())
            elif isinstance(used_op, TimestampType) and random.randint(0, 1):
                new_op = Operand(f"EXTRACT(MINUTE FROM {op.str_value()}) - EXTRACT(MINUTE FROM {used_op.str_value()})",
                                 IntType())
            elif random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(YEAR FROM {op.str_value()}) - EXTRACT(YEAR FROM {used_op.str_value()})",
                                 IntType())
            elif random.randint(0, 1) == 1:
                new_op = Operand(f"EXTRACT(MONTH FROM {op.str_value()}) - EXTRACT(MONTH FROM {used_op.str_value()})",
                                 IntType())
            else:
                new_op = Operand(f"EXTRACT(DAY FROM {op.str_value()}) - EXTRACT(DAY FROM {used_op.str_value()})",
                                 IntType())
            if aggregate_flag:
                new_op = Operand(f"AVG({new_op.str_value()})", IntType())
            return new_op, IntType()
    elif isinstance(op_type, StringGeneralType):
        if predicate_flag:
            # TODO
            return None, None
        string_ops = []
        group_by_string_ops = []
        for op1 in select_stmt['cols']:
            if isinstance(op1, Operand) and loose_match(op1.op_type, StringGeneralType()):
                string_ops.append(op1)
        if select_stmt['group_by_cols'] is not None:
            for op1 in select_stmt['group_by_cols']:
                if isinstance(op1, Operand) and loose_match(op1.op_type, StringGeneralType()):
                    group_by_string_ops.append(op1)
        aggregate_flag = False
        if select_stmt['group_by_cols'] is not None:
            if len(group_by_string_ops) == 0:
                return None, None
            else:
                used_op = random.choice(group_by_string_ops)
        else:
            if len(string_ops) == 0:
                return None, None
            used_op = random.choice(string_ops)
        if aggregate_flag:
            return None, None
        if src_dialect in ['pg', 'oracle'] and tgt_dialect in ['pg', 'oracle']:
            new_op = Operand(f"{op.str_value()} || ' ' || {used_op.str_value()}", StringGeneralType())
        elif src_dialect in ['mysql', 'oracle'] and tgt_dialect in ['mysql', 'oracle']:
            new_op = Operand(f"CONCAT(COALESCE({used_op.str_value()}, ''), {op.str_value()})", StringGeneralType())
        else:
            assert src_dialect in ['mysql', 'pg'] and tgt_dialect in ['mysql', 'pg']
            new_op = Operand(f"CONCAT(COALESCE({used_op.str_value()}, ''), ' ', {op.str_value()})", StringGeneralType())
        return new_op, StringGeneralType()
    elif isinstance(op_type, NumberType):
        int_ops = []
        group_int_ops = []
        for col_op in select_stmt['cols']:
            if isinstance(col_op, Operand) and loose_match(col_op.op_type, NumberType()):
                int_ops.append(col_op)
        if select_stmt['group_by_cols'] is not None:
            for col_op in select_stmt['group_by_cols']:
                if isinstance(col_op, Operand) and loose_match(col_op.op_type, NumberType()):
                    group_int_ops.append(col_op)
        if predicate_flag:
            return None, None
        if len(group_int_ops) + len(int_ops) == 0:
            return None, None
        aggregate_flag = False
        if select_stmt['group_by_cols'] is not None:
            if len(group_int_ops) == 0:
                used_op = random.choice(int_ops)
                aggregate_flag = True
            else:
                used_op = random.choice(group_int_ops)
        else:
            used_op = random.choice(int_ops)
        new_op = Operand(f"{op.str_value()} * {used_op.str_value()}", NumberType())
        if aggregate_flag:
            new_op = Operand(f"AVG({new_op.str_value()})", NumberType())
        return new_op, NumberType()
    else:
        if predicate_flag and not isinstance(op_type, BoolType):
            return None, None
        return op, op_type
    # return None, None


def gen_sql_used_value(value: Operand, value_type, where_flag, select_stmt):
    flag = True
    if isinstance(value_type, IntervalType):
        can_be_used_date_values = []
        group_by_date_cols = []
        for col_op in select_stmt['cols']:
            if isinstance(col_op, Operand) and loose_match(col_op.op_type, DateType()):
                can_be_used_date_values.append(col_op)
        if select_stmt['group_by_cols'] is not None:
            for col_op in select_stmt['group_by_cols']:
                if isinstance(col_op, Operand) and loose_match(col_op.op_type, DateType()):
                    group_by_date_cols.append(col_op)
            if not where_flag and len(group_by_date_cols) > 0:
                col = random.choice(group_by_date_cols)
                add_op = Operand(f'{col.str_value()} + {value.str_value()}', col.op_type)
                cur_res_type = col.op_type
            else:
                return False, None, None
        else:
            if len(can_be_used_date_values) == 0:
                return False, None, None
            col = random.choice(can_be_used_date_values)
            add_op = Operand(f'{col.str_value()} + {value.str_value()}', col.op_type)
            cur_res_type = col.op_type
    else:
        flag = True
        cur_res_type = value_type
        add_op = value
    return flag, add_op, cur_res_type


def try_gen_function_point_sql(sql: dict, point: Point,
                               backup_points: list[dict], aggressive_flag: bool,
                               execution_env: ExecutionEnv, already_build_set: CISpacelessSet) -> dict | None:
    src_dialect = point.src_dialect
    tgt_dialect = point.tgt_dialect
    try:
        print(sql)
        op_analysis_res = analysis_sql(sql[src_dialect], src_dialect)
    except ValueError as e:
        print(e)
        return None
    add_one_time = False
    have_add_to_select_flag = False
    have_add_to_select = set()
    existing_points = get_sql_points(sql)
    final_add_type = point.return_type
    for select_stmt in op_analysis_res["select_stmts"]:
        if not add_one_time:
            if is_literal(point):
                flag, added_points, ori_op = (
                    gen_pattern_value_function(point, select_stmt, backup_points,
                                               select_stmt['select_root_node'], False, execution_env, False))
                if ori_op.str_value() in already_build_set:
                    continue
                predicate_flag = random.randint(0, 1) == 0
                cur_res_type = point.return_type
                flag, add_op, cur_res_type = gen_sql_used_value(ori_op, cur_res_type, predicate_flag, select_stmt)
                if not flag:
                    continue
                augment_op, augment_type = augment_literal(add_op, cur_res_type, select_stmt,
                                                           execution_env,
                                                           predicate_flag, src_dialect, tgt_dialect)
                if augment_op is None:
                    predicate_flag = not predicate_flag
                    augment_op, augment_type = augment_literal(add_op, cur_res_type, select_stmt,
                                                               execution_env,
                                                               predicate_flag, src_dialect, tgt_dialect)
                if augment_op is None and aggressive_flag:
                    continue
                if augment_op is not None:
                    if predicate_flag:
                        assert isinstance(augment_type, BoolType)
                        add_op_to_where(augment_op, src_dialect, select_stmt['select_root_node'])
                    else:
                        final_add_type = augment_type
                        have_add_to_select_flag = True
                        have_add_to_select.add(id(select_stmt))
                        add_op_to_select(augment_op, point, src_dialect, tgt_dialect, select_stmt['select_root_node'])
                else:
                    final_add_type = cur_res_type
                    have_add_to_select_flag = True
                    have_add_to_select.add(id(select_stmt))
                    add_op_to_select(add_op, point, src_dialect, tgt_dialect, select_stmt['select_root_node'])
            else:
                if isinstance(point.return_type, BoolType):
                    if random.randint(1, 3) == 2:
                        flag, added_points, ori_op = (
                            gen_pattern_value_function(point, select_stmt, backup_points,
                                                       select_stmt['select_root_node'], False, execution_env, False))
                        if not flag or ori_op.str_value() in already_build_set:
                            continue
                        if flag:
                            if src_dialect == 'oracle' or tgt_dialect == 'oracle':
                                used_op = Operand(f'CASE WHEN {ori_op.str_value()} THEN 1 ELSE 0 END', IntType())
                            else:
                                used_op = ori_op
                            final_add_type = IntType()
                            have_add_to_select_flag = True
                            have_add_to_select.add(id(select_stmt))
                            add_op_to_select(used_op, point, src_dialect, tgt_dialect, select_stmt['select_root_node'])
                    else:
                        flag, added_points, ori_op = (
                            gen_pattern_value_function(point, select_stmt, backup_points,
                                                       select_stmt['select_root_node'], False, execution_env, True))
                        if not flag:
                            continue
                        if flag:
                            add_op_to_where(ori_op, src_dialect, select_stmt['select_root_node'])
                else:
                    flag, added_points, ori_op = (
                        gen_pattern_value_function(point, select_stmt, backup_points,
                                                   select_stmt['select_root_node'], False, execution_env, False))
                    if not flag or ori_op.str_value() in already_build_set:
                        continue
                    predicate_flag = False
                    flag, add_op, cur_res_type = gen_sql_used_value(ori_op, point.return_type,
                                                                    predicate_flag, select_stmt)
                    if not flag:
                        continue
                    final_add_type = cur_res_type
                    have_add_to_select_flag = True
                    have_add_to_select.add(id(select_stmt))
                    add_op_to_select(add_op, point, src_dialect, tgt_dialect, select_stmt['select_root_node'])
            existing_points = merge_trans_points(existing_points, added_points)
            add_one_time = add_one_time or len(added_points) > 0
            already_build_set.add(ori_op.str_value())
    if have_add_to_select_flag:
        for select_stmt in op_analysis_res["select_stmts"]:
            if id(select_stmt) not in have_add_to_select:
                add_op = gen_no_transfer_value_type(final_add_type, select_stmt)
                add_op_to_select(add_op, point, src_dialect, tgt_dialect, select_stmt['select_root_node'])
    if not add_one_time:
        return None
    return {
        src_dialect: str(op_analysis_res['root_node']),
        tgt_dialect: '',
        "points": existing_points
        # "db_id": db_id
    }


def is_slot_value(slot: ValueSlot):
    if isinstance(slot.slot_type, OptionType):
        return False
    if isinstance(slot.slot_type, IntType) and slot.slot_type.attr_container.has_literal():
        return False
    if slot.udf_func is None:
        return True
    else:
        for arg in slot.udf_func.arg_slots:
            if isinstance(arg, ValueSlot) and is_slot_value(arg):
                return True
        return False


def is_literal(point: Point):
    # judge whether a point generate only a literal value
    for ele in point.src_pattern.elements:
        if isinstance(ele, ValueSlot):
            if is_slot_value(ele):
                return False
        elif isinstance(ele, ForSlot):
            for for_slot in ele.ele_slots:
                if is_slot_value(for_slot):
                    return False
    return True


def fetch_usable_sqls(point: Point, backup_points: list[dict], db_id: str | None = None):
    # TODO: currently only use sqls with no translation point
    if db_id is not None:
        db_ids = [db_id]
    else:
        db_ids = get_db_ids()
    all_sqls = []
    src_dialect = point.src_dialect
    tgt_dialect = point.tgt_dialect
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
            all_sqls = all_sqls + sqls
    #     path = os.path.join(sql_root_path, 'points', f'{src_dialect}_{tgt_dialect}.json')
    #     if os.path.exists(path):
    #         with open(path, 'r') as file:
    #             sqls = json.load(file)
    #         for sql in sqls:
    #             flag = True
    #             for point in sql['points']:
    #                 if point not in points:
    #                     flag = False
    #                     break
    #             if flag:
    #                 all_sqls.append(sql)
    # for sql in all_sqls:
    #     if 'points' not in sql:
    #         sql['points'] = []
    random.shuffle(all_sqls)
    return all_sqls


def add_function_point(point: Point, cur_sql: dict | None, backup_points: list[dict],
                       aggressive_flag: bool, execution_env: ExecutionEnv, already_build_set: CISpacelessSet,
                       only_cur_sql_mode: bool):
    can_be_used_sqls = []
    gen_sql = None
    if cur_sql is not None and random.random() >= possibility_of_using_new_sql:
        try:
            gen_sql = try_gen_function_point_sql(cur_sql, point, backup_points,
                                                 aggressive_flag, execution_env, already_build_set)
            if only_cur_sql_mode:
                if gen_sql is not None:
                    return gen_sql
                else:
                    return None
        except Exception as e:
            print(e)
    used_sqls = fetch_usable_sqls(point, backup_points)
    if gen_sql is None:
        for sql in used_sqls:
            can_be_used_sqls.append(sql)
        while len(can_be_used_sqls) > 0:
            used_sql = random.choice(can_be_used_sqls)
            can_be_used_sqls.remove(used_sql)
            gen_sql = try_gen_function_point_sql(used_sql, point, backup_points, aggressive_flag,
                                                 execution_env, already_build_set)
            if gen_sql is not None:
                return merge_query(cur_sql, gen_sql, execution_env)
    return gen_sql


def fetch_queries_with_table(table_name: str, src_dialect, tgt_dialect):
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
                all_sqls.append(sql)
    res = []
    for sql in all_sqls:
        if 'tables' not in sql:
            continue
        if table_name in sql['tables']:
            res.append(sql)
    return res


def generate_reserved_keyword_point(point: Point, cur_sql: dict, execution_env: ExecutionEnv, only_cur_sql_mode: bool):
    word, _ = point.src_pattern.extend_pattern()
    reserved_keyword = word
    db_ids = get_db_ids()
    reserved_keyword_list = []
    for db_id in db_ids:
        with open(os.path.join(get_data_path(), db_id, 'schema.json')) as file:
            schema = json.load(file)
            for table, table_content in schema.items():
                assert isinstance(table, str)
                if table.upper() == reserved_keyword.upper():
                    fk_tables = []
                    for tbl1, tbl_cont1 in schema.items():
                        if tbl1 == table:
                            continue
                        for fk in tbl_cont1['foreign_key']:
                            if fk['ref_table'] == table and isinstance(fk['col'], str):
                                fk_tables.append({
                                    "RefTable": tbl1,
                                    "RefCol": fk['col'],
                                    "Col": fk['ref_col']
                                })
                    for fk in table_content['foreign_key']:
                        if isinstance(fk['col'], str):
                            fk_tables.append({
                                "RefTable": fk['ref_table'],
                                "RefCol": fk['ref_col'],
                                "Col": fk['col']
                            })
                    random.shuffle(fk_tables)
                    reserved_keyword_list.append({
                        "Type": "Table",
                        "TableName": table,
                        "ForeignKeyTables": fk_tables
                    })
                for col in table_content['cols']:
                    if col['col_name'].upper() == reserved_keyword.upper():
                        reserved_keyword_list.append({
                            "Type": "Column",
                            "TableName": table,
                            "ColumnName": col['col_name']
                        })
    random.shuffle(reserved_keyword_list)
    for ele in reserved_keyword_list:
        if ele['Type'] == 'Table':
            if only_cur_sql_mode:
                queries = [cur_sql]
            else:
                queries = fetch_fulfilled_sqls([], point.src_dialect, point.tgt_dialect, None)
            flag = False
            while not flag:
                query = random.choice(queries)
                res = analysis_sql(query[point.src_dialect], point.src_dialect)
                for select_stmt in res['select_stmts']:
                    root_node = select_stmt['select_root_node']
                    if point.src_dialect == 'mysql':
                        assert isinstance(root_node, TreeNode)
                        assert root_node.value == 'querySpecification' or root_node.value == 'querySpecificationNointo'
                        fromClause_node = root_node.get_child_by_value('fromClause')
                        assert fromClause_node is not None
                        table_sources_node = fromClause_node.get_child_by_value('tableSources')
                        assert isinstance(table_sources_node, TreeNode)
                        table_sources_node.add_child(TreeNode(',', point.src_dialect, True))
                        table_sources_node.add_child(TreeNode(reserved_keyword.lower(), point.src_dialect, True))
                    elif point.src_dialect == 'oracle':
                        assert isinstance(root_node, TreeNode)
                        assert root_node.value == 'query_block'
                        from_clause_node = root_node.get_child_by_value('from_clause')
                        table_ref_list_node = from_clause_node.get_child_by_value('table_ref_list')
                        table_ref_list_node.add_child(TreeNode(',', point.src_dialect, True))
                        table_ref_list_node.add_child(TreeNode(reserved_keyword.lower(), point.src_dialect, True))
                    elif point.src_dialect == 'pg':
                        assert isinstance(root_node, TreeNode)
                        assert root_node.value == 'simple_select_pramary'
                        from_clause_node = root_node.get_child_by_value('from_clause')
                        table_ref_list_node = from_clause_node.get_child_by_value('from_list')
                        table_ref_list_node.add_child(TreeNode(',', point.src_dialect, True))
                        table_ref_list_node.add_child(TreeNode(reserved_keyword.lower(), point.src_dialect, True))
                    if reserved_keyword == 'USER':
                        add_op_to_where(Operand('displayname IN (\'Harlan\', \'Jarrod Dixon\')', BaseType('')),
                                        point.src_dialect, select_stmt['select_root_node'])
                    elif reserved_keyword == 'MATCH':
                        add_op_to_where(Operand('season = \'2015/2016\'', BaseType('')),
                                        point.src_dialect, select_stmt['select_root_node'])
                    exec_flag, _ = execution_env.execute_sql(str(res['root_node']))
                    if exec_flag:
                        gen_sql = {
                            point.src_dialect: str(res['root_node']),
                            point.tgt_dialect: '',
                            "points": [{
                                "point": point.point_name,
                                "num": 1
                            }]
                        }
                        if cur_sql is not None:
                            return gen_sql
                        return merge_query(cur_sql, gen_sql, execution_env)
            # directly added to FROM clause
            # for fk_table in ele['ForeignKeyTables']:
            #     queries = fetch_queries_with_table(fk_table['RefTable'],
            #                                        point.src_dialect, point.tgt_dialect)
            #     random.shuffle(queries)
            #     for query in queries:
            #         res = analysis_sql(query, point.src_dialect)
            #         if res is None:
            #             continue
            #         for select_stmt in res['select_stmts']:
            #             root_node = select_stmt['select_root_node']
            #             if point.src_dialect == 'mysql':
            #                 assert isinstance(root_node, TreeNode)
            #                 assert root_node.value == 'querySpecification' or root_node.value == 'querySpecificationNointo'
            #                 fromClause_node = root_node.get_child_by_value('fromClause')
            #                 assert fromClause_node is not None
            #                 table_sources_node = fromClause_node.get_child_by_value('tableSources')
            #             elif point.src_dialect == 'oracle':
            #                 pass
            #             elif point.src_dialect == 'pg':
            #                 pass
        else:
            if only_cur_sql_mode:
                queries = [cur_sql]
            else:
                queries = fetch_queries_with_table(ele['TableName'], point.src_dialect, point.tgt_dialect)
            random.shuffle(queries)
            for query in queries:
                res = analysis_sql(query[point.src_dialect], point.src_dialect)
                flag = False
                added_select_stmt = None
                for select_stmt in res['select_stmts']:
                    if select_stmt['group_by_cols'] is None:
                        for col in select_stmt['cols']:
                            if isinstance(col, ColumnOp) and col.column_name.lower() == reserved_keyword.lower():
                                flag = True
                                add_op_to_select(Operand(reserved_keyword.lower(), BaseType('')),
                                                 point, point.src_dialect, point.tgt_dialect,
                                                 select_stmt['select_root_node'])
                                added_select_stmt = select_stmt
                                break
                    if flag:
                        break
                if flag:
                    for select_stmt in res['select_stmts']:
                        if select_stmt == added_select_stmt:
                            continue
                        else:
                            add_op_to_select(Operand('NULL', BaseType('')),
                                             point, point.src_dialect, point.tgt_dialect,
                                             select_stmt['select_root_node'])
                    exec_flag, _ = execution_env.execute_sql(str(res['root_node']))
                    if exec_flag:
                        gen_sql = {
                            point.src_dialect: str(res['root_node']),
                            point.tgt_dialect: '',
                            "points": [{
                                "point": point.point_name,
                                "num": 1
                            }]
                        }
                        if only_cur_sql_mode:
                            return gen_sql
                        return merge_query(cur_sql, gen_sql, execution_env)
    return None


def generate_sql_with_point(point: Point, cur_sql: None | dict, backup_points: list[dict],
                            aggressive_flag: bool, execution_env: ExecutionEnv, built_points: CISpacelessSet,
                            only_cur_sql_mode: bool):
    if isinstance(point.point_type, ClauseType):
        sql = add_pattern_point(point, cur_sql, backup_points, aggressive_flag, execution_env, only_cur_sql_mode)
    elif isinstance(point.point_type, ExpressionType) or isinstance(point.point_type, LiteralType):
        sql = add_function_point(point, cur_sql, backup_points, aggressive_flag, execution_env, built_points,
                                 only_cur_sql_mode)
    elif isinstance(point.point_type, ReservedKeywordType):
        sql = generate_reserved_keyword_point(point, cur_sql, execution_env, only_cur_sql_mode)
    else:
        raise ValueError('Other Type\'s Generation is not support yet')
    return sql

#
# point = load_point_by_name('pg', 'mysql', 'GROUP_BY_GROUPING_SETS_HAVING')
# point = parse_point(point)
# node = parse_pattern_tree(point.point_type, point.src_pattern, 'pg')
# generate_select_stmt_pattern(node, 'pg', 'mysql', {})
