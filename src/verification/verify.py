# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: verify$
# @Author: 10379
# @Time: 2025/2/21 13:04
import os.path
import subprocess
import threading
from collections import Counter
from typing import Any

from cracksql.utils.tools import print_err

from antlr_parser.Tree import TreeNode
from antlr_parser.mysql_tree import fetch_main_select_from_select_stmt_mysql
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import get_pg_main_select_node_from_select_stmt
from db_builder.normalize import rm_sql_quote_reserved
from db_builder.schema_builder import schema_build, dump_json_schema
from utils.db_connector import sql_execute, sql_dependent_execute
from utils.tools import get_proj_root_path, is_running_on_linux, get_used_reserved_keyword_list, get_all_db_name, \
    get_db_ids
import math
from numbers import Number

from collections.abc import Iterable
from collections import Counter

from verification.verify_preprocess import rep_non_deterministic_function_list, rewrite_dialect_specific_func


def make_hashable(item):
    if isinstance(item, list):
        return tuple(make_hashable(subitem) for subitem in item)
    elif isinstance(item, dict):
        return tuple(sorted((make_hashable(k), make_hashable(v)) for k, v in item.items()))
    elif isinstance(item, Iterable) and not isinstance(item, (str, bytes)):
        return tuple(make_hashable(subitem) for subitem in item)
    else:
        return item


def deep_equal(v1: Any, v2: Any, rel_tol=1e-6, abs_tol=1e-9) -> bool:
    if type(v1) != type(v2):
        return False

    if isinstance(v1, float) and isinstance(v2, float):
        return math.isclose(v1, v2, rel_tol=rel_tol, abs_tol=abs_tol)

    if isinstance(v1, (list, tuple)):
        if len(v1) != len(v2):
            return False
        return all(deep_equal(sub1, sub2, rel_tol, abs_tol) for sub1, sub2 in zip(v1, v2))

    if isinstance(v1, dict):
        if set(v1.keys()) != set(v2.keys()):
            return False
        return all(deep_equal(v1[k], v2[k], rel_tol, abs_tol) for k in v1.keys())

    if isinstance(v1, set):
        # 注意：set 中如果有浮点数，需要特殊处理
        if len(v1) != len(v2):
            return False
        # 转为列表后尝试匹配（因为 set 无序，且浮点不能精确哈希）
        unmatched = list(v2)
        for item1 in v1:
            matched = False
            for i, item2 in enumerate(unmatched):
                if deep_equal(item1, item2, rel_tol, abs_tol):
                    unmatched.pop(i)
                    matched = True
                    break
            if not matched:
                return False
        return True
    return v1 == v2


def tol_order_aware_compare(value1: list[tuple | None], value2: list[tuple | None],
                            rel_tol=1e-6, abs_tol=1e-9) -> bool:
    return deep_equal(value1, value2, rel_tol=rel_tol, abs_tol=abs_tol)


def normalize_for_sorting(item: Any, rel_tol=1e-6, abs_tol=1e-9) -> Any:
    if isinstance(item, float):
        return item
    if isinstance(item, list):
        normalized = sorted(normalize_for_sorting(x, rel_tol, abs_tol) for x in item)
        return tuple(normalized)
    if isinstance(item, tuple):
        return tuple(normalize_for_sorting(x, rel_tol, abs_tol) for x in item)
    if isinstance(item, dict):
        return tuple(
            (k, normalize_for_sorting(v, rel_tol, abs_tol))
            for k, v in sorted(item.items())
        )
    if isinstance(item, set):
        return tuple(sorted(normalize_for_sorting(x, rel_tol, abs_tol) for x in item))
    return item


def tol_aware_recursive_compare(item1: Any, item2: Any, rel_tol=1e-6, abs_tol=1e-9) -> bool:
    """Recursively compares two items, handling floats with tolerance."""
    if isinstance(item1, Number) and isinstance(item2, Number):
        return math.isclose(item1, item2, rel_tol=rel_tol, abs_tol=abs_tol)

    if isinstance(item1, (tuple, list)) and isinstance(item2, (tuple, list)):
        if len(item1) != len(item2):
            return False
        return all(
            tol_aware_recursive_compare(x, y, rel_tol, abs_tol) for x, y in zip(item1, item2)
        )
    if type(item1) != type(item2):
        return False
    return item1 == item2


def tol_order_unaware_compare(
        value1: list[tuple | None],
        value2: list[tuple | None],
        rel_tol=1e-6,
        abs_tol=1e-9
) -> bool:
    if len(value1) != len(value2):
        return False

    # Normalize and sort non-float items for comparison
    normalized1 = [normalize_for_sorting(item, rel_tol, abs_tol) for item in value1]
    normalized2 = [normalize_for_sorting(item, rel_tol, abs_tol) for item in value2]
    sorted1 = sorted(normalized1, key=lambda x: str(x))
    sorted2 = sorted(normalized2, key=lambda x: str(x))

    for item1, item2 in zip(sorted1, sorted2):
        if not tol_aware_recursive_compare(item1, item2, rel_tol, abs_tol):
            print(f"Mismatch found: {item1} vs {item2}")
            return False
    return True


def tol_order_unaware_compare_fallback(value1: list[tuple | None], value2: list[tuple | None],
                                       rel_tol=1e-6, abs_tol=1e-9) -> bool:
    """无序比较：两个列表的元素在顺序无关下是否相等（支持浮点误差）"""
    if len(value1) != len(value2):
        return False

    unmatched = value2.copy()
    for item1 in value1:
        matched = False
        for i, item2 in enumerate(unmatched):
            if deep_equal(item1, item2, rel_tol=rel_tol, abs_tol=abs_tol):
                unmatched.pop(i)
                matched = True
                break
        if not matched:
            return False
    return True


def order_unaware_compare(value1: list[tuple | None], value2: list[tuple | None]):
    return Counter(make_hashable(item) for item in value1) == Counter(make_hashable(item) for item in value2)


def order_aware_compare(value1: list[tuple | None], value2: list[tuple | None]):
    return value1 == value2


def subquery_node_has_order_by(subquery_node):
    if len(subquery_node.get_children_by_value('subquery_operation_part')) == 0:
        basic_element_node = subquery_node.get_child_by_value('subquery_basic_elements')
        assert basic_element_node is not None
        if basic_element_node.get_child_by_value('query_block') is not None:
            query_block_node = basic_element_node.get_child_by_value('query_block')
            order_by_node = query_block_node.get_child_by_value('order_by_clause')
            if order_by_node is not None:
                return True
            return False
        else:
            subquery_node = basic_element_node.get_child_by_value('subquery')
            return subquery_node_has_order_by(subquery_node)
    else:
        subquery_operation_part_nodes = subquery_node.get_children_by_value('subquery_operation_part')
        last_union_node = subquery_operation_part_nodes[-1]
        subquery_basic_elements_node = last_union_node.get_child_by_value('subquery_basic_elements')
        if subquery_basic_elements_node.get_child_by_value('subquery') is not None:
            return False
        else:
            query_block_node = subquery_basic_elements_node.get_child_by_value('query_block')
            assert query_block_node is not None
            if query_block_node.get_child_by_value('order_by_clause') is not None:
                return True
            return False


def has_outer_most_order_by(sql: str, dialect) -> bool:
    root_node, _, _, _ = parse_tree(sql, dialect)
    if root_node is None:
        print_err("No root node")
        return False
    root_node = TreeNode.make_g4_tree_by_node(root_node, dialect)
    if dialect == 'pg':
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        select_no_parens_node = get_pg_main_select_node_from_select_stmt(select_stmt_node)
        assert select_no_parens_node is not None
        order_by_node = select_no_parens_node.get_children_by_value('opt_sort_clause')
        if len(order_by_node) > 0:
            return True
        return False
    elif dialect == 'mysql':
        select_statement_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                'dmlStatement', 'selectStatement'])
        assert len(select_statement_node) == 1
        select_statement_node = select_statement_node[0]
        if select_statement_node.get_child_by_value('orderByClause') is not None:
            return True
        else:
            last_select_node = None
            only_one_flag = False
            for child in select_statement_node.children:
                if child.value in ['querySpecificationNointo', 'queryExpression', 'querySpecification',
                                   'queryExpressionNointo', 'unionStatement', 'unionParenthesis']:
                    last_select_node = child
                    if not only_one_flag:
                        only_one_flag = True
                    else:
                        only_one_flag = False
            if only_one_flag:
                main_node = fetch_main_select_from_select_stmt_mysql(last_select_node)
                if main_node.get_child_by_value('orderByClause') is not None:
                    return True
                else:
                    return False
            else:
                if (last_select_node.value == 'queryExpressionNointo'
                        or last_select_node.value == 'queryExpression' or last_select_node.value == 'unionParenthesis'):
                    return False
                elif last_select_node.value == 'unionStatement':
                    query_specification_no_into_node = last_select_node.get_child_by_value('querySpecificationNointo')
                    if query_specification_no_into_node is not None:
                        return query_specification_no_into_node.get_child_by_value('orderByClause') is not None
                    else:
                        return False
                else:
                    if last_select_node.get_child_by_value('orderByClause') is not None:
                        return True
                    else:
                        return False
    elif dialect == 'oracle':
        select_stmt_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                           'select_statement', 'select_only_statement'])
        subquery_node = select_stmt_node[0].get_child_by_value('subquery')
        return subquery_node_has_order_by(subquery_node)
    else:
        assert False


def post_process_for_reserved_keyword(sql: str, src_dialect: str, tgt_dialect: str):
    # post process for reserved keyword
    root_node, _, _, _ = parse_tree(sql, tgt_dialect)
    if root_node is None:
        return sql
    root_node = TreeNode.make_g4_tree_by_node(root_node, tgt_dialect)
    reserved_keywords = get_used_reserved_keyword_list()
    src_reserved_keyword_list = reserved_keywords[src_dialect]
    tgt_reserved_keyword_list = reserved_keywords[tgt_dialect]
    rep_reserved_keyword = []
    for keyword in src_reserved_keyword_list:
        if keyword not in tgt_reserved_keyword_list:
            rep_reserved_keyword.append(keyword)
    if tgt_dialect == 'mysql':
        rm_sql_quote_reserved(root_node, tgt_dialect, rep_reserved_keyword)
    elif tgt_dialect == 'pg':
        rm_sql_quote_reserved(root_node, tgt_dialect, rep_reserved_keyword)
    elif tgt_dialect == 'oracle':
        rm_sql_quote_reserved(root_node, tgt_dialect, rep_reserved_keyword)
    else:
        assert False
    return str(root_node)


def execution_verify(sql1: str, sql_res, res_sql: str, res_sql_res, db_id: str, db_param: dict, dialect: str,
                     order_mode):
    # verify one by one
    if sql_res is not None:
        res1 = sql_res
        flag1 = True
    else:
        flag1, res1 = sql_dependent_execute(dialect, db_id, sql1, db_param.get(dialect, {}))
    if res_sql_res is not None:
        res2 = res_sql_res
        flag2 = True
    else:
        flag2, res2 = sql_dependent_execute(dialect, db_id, res_sql, db_param.get(dialect, {}))

    if not flag1:
        return False, res1, res1, res2
    print(dialect)
    print(res_sql)
    if not flag2:
        print(res2)
    assert flag2
    if len(res1) == 0 and len(res2) == 0:
        return True, 'No result', res1, res2
    if not order_mode:
        if tol_order_unaware_compare(res1, res2):
            return True, '', res1, res2
        else:
            return False, 'inconsistent_result', res1, res2
    else:
        if tol_order_aware_compare(res1, res2):
            return True, '', res1, res2
        else:
            return False, 'inconsistent_result', res1, res2


def ori_execution_verify(sql1: str, sql_res, res_sql: str, res_sql_res, db_id: str, db_param: dict, dialect: str,
                         order_mode):
    # verify one by one
    if sql_res is not None:
        res1 = sql_res
        flag1 = True
    else:
        flag1, res1 = sql_dependent_execute(dialect, db_id, sql1, db_param.get(dialect, {}))
    if res_sql_res is not None:
        res2 = res_sql_res
        flag2 = True
    else:
        flag2, res2 = sql_dependent_execute(dialect, db_id, res_sql, db_param.get(dialect, {}))
    if not flag1:
        return False, res1, res1, res2
    assert flag2
    if len(res1) == 0 and len(res2) == 0:
        return True, 'No result', res1, res2
    if not order_mode:
        if order_unaware_compare(res1, res2):
            return True, '', res1, res2
        else:
            return False, 'inconsistent_result', res1, res2
    else:
        if res1 == res2:
            return True, '', res1, res2
        else:
            return False, 'inconsistent_result', res1, res2

lock = threading.Lock()
used_number = 0

def verify_sql_solver(sql1, sql2, schema):
    cur_path = os.path.join(get_proj_root_path(), 'src', 'verification')
    jar_path = os.path.join(cur_path, 'solver_dependency', 'sqlsolver-v1.1.0.jar')

    global lock
    global used_number
    number = None
    with lock:
        number = used_number
        used_number += 1
    sql1_path = os.path.join(cur_path, f'sql{number}_1.sql')
    sql2_path = os.path.join(cur_path, f'sql{number}_2.sql')
    schema_path = os.path.join(cur_path, f'schema_{number}.sql')
    res_path = os.path.join(cur_path, f'res_{number}.txt')
    with open(sql1_path, 'w') as file:
        file.write(sql1)

    with open(sql2_path, 'w') as file:
        file.write(sql2)

    with open(schema_path, 'w') as file:
        file.write(schema)

    if is_running_on_linux():
        load_cmd = f"export LD_LIBRARY_PATH={os.path.join(cur_path, 'solver_dependency')}:$LD_LIBRARY_PATH "
    else:
        load_cmd = f"set PATH={os.path.join(cur_path, 'solver_dependency')};%PATH%"
    cmd = load_cmd + (f"&& java -jar {jar_path} -sql1={sql1_path} -sql2={sql2_path} "
                      f"-schema={schema_path} -output={res_path}")
    cnt = 0
    while not os.path.exists(res_path):
        cnt += 1
        if cnt > 3:
            return "Error"
        try:
            result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            return None
    verify_result = None
    with open(res_path, 'r') as file:
        contents = file.readlines()
        if contents[0].strip() == 'EQ':
            verify_result = True
        elif contents[0].strip() == 'NEQ':
            verify_result = False
        elif contents[0].strip() == 'UNKNOWN':
            verify_result = None
        elif contents[0].strip() == 'TIMEOUT':
            verify_result = None
    os.remove(sql1_path)
    os.remove(sql2_path)
    os.remove(schema_path)
    os.remove(res_path)
    return verify_result


def verify_sqls_solver(sql1s, sql2s, schema):
    # verify all
    cur_path = os.path.join(get_proj_root_path(), 'src', 'verification')
    jar_path = os.path.join(cur_path, 'solver_dependency', 'sqlsolver-v1.1.0.jar')
    sql1_path = os.path.join(cur_path, 'sql1.sql')
    sql2_path = os.path.join(cur_path, 'sql2.sql')
    schema_path = os.path.join(cur_path, 'schema.sql')
    res_path = os.path.join(cur_path, 'res.txt')

    with open(sql1_path, 'w') as file:
        flag = False
        for sql in sql1s:
            if flag:
                file.write('\n')
            flag = True
            file.write(sql)

    with open(sql2_path, 'w') as file:
        flag = False
        for sql in sql2s:
            if flag:
                file.write('\n')
            flag = True
            file.write(sql)

    with open(schema_path, 'w') as file:
        file.write(schema)

    if is_running_on_linux():
        load_cmd = f"export LD_LIBRARY_PATH={os.path.join(cur_path, 'solver_dependency')}:$LD_LIBRARY_PATH \\"
    else:
        load_cmd = f"set PATH={os.path.join(cur_path, 'solver_dependency')};%PATH%"
    cmd = load_cmd + (f"&& java -jar {jar_path} -sql1={sql1_path} "
                      f"-sql2={sql2_path} -schema={schema_path} -output={res_path}")

    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    res = []
    with open(res_path, 'r') as file:
        contents = file.readlines()
        for i in range(len(sql1s)):
            verify_result = None
            if contents[0].strip() == 'EQ':
                verify_result = "EQ"
            elif contents[0].strip() == 'NEQ':
                verify_result = "NEQ"
            elif contents[0].strip() == 'UNKNOWN':
                verify_result = "UNKNOWN"
            elif contents[0].strip() == 'TIMEOUT':
                verify_result = "TIMEOUT"
            res.append({
                "sql1": sql1s[i],
                "sql2": sql2s[i],
                "res": verify_result
            })
    os.remove(sql1_path)
    os.remove(sql2_path)
    os.remove(schema_path)
    os.remove(res_path)
    return res


all_table_schema = None


def verify_sql(sql, res_sql, res_sql_res, db_id, db_param, dialect, tables: list, order_mode: bool | None = True):
    # verify one by one
    verify_res = {}
    sql = rep_non_deterministic_function_list(sql, dialect)
    res_sql = rep_non_deterministic_function_list(res_sql, dialect)
    flag, error, sql_res, res_sql_res = execution_verify(sql, None, res_sql, res_sql_res, db_id, db_param, dialect,
                                                         order_mode)
    flag1, error1, _, _ = ori_execution_verify(sql, sql_res, res_sql, res_sql_res, db_id, db_param, dialect, order_mode)
    if flag:
        verify_res['execution'] = True
        verify_res['error'] = error
        verify_res['ori_execution'] = flag1
        verify_res['ori_execution_reason'] = error1
    else:
        verify_res['execution'] = False
        verify_res['error'] = error
        verify_res['ori_execution'] = flag1
        verify_res['ori_execution_reason'] = error1
    global all_table_schema
    if all_table_schema is None:
        all_table_schema = {}
        for ori_dialect in ['mysql', 'pg', 'oracle']:
            for db_name in get_db_ids():
                schema, add_constraints, type_defs = schema_build(db_name, ori_dialect)
                dict_value = dump_json_schema(schema, add_constraints, type_defs, ori_dialect, db_name)
                for table_name, table_ddl in dict_value.items():
                    if table_name not in all_table_schema:
                        all_table_schema[table_name] = {}
                    all_table_schema[table_name][ori_dialect] = table_ddl
    if verify_res['execution']:
        rewrite_sql = rewrite_dialect_specific_func(sql, dialect)
        # rewrite_for_specific function might be wrong for syntax error
        if rewrite_sql is not None:
            sql = rewrite_sql
        rewrite_res_sql = rewrite_dialect_specific_func(res_sql, dialect)
        if rewrite_res_sql is not None:
            res_sql = rewrite_res_sql
        schema = ''
        for table in tables:
            for ddl in all_table_schema[table][dialect]:
                if schema == '':
                    schema += f'{ddl}'
                else:
                    schema += f'\n{ddl}'
        verify_res['formal'] = verify_sql_solver(sql, res_sql, schema.replace('VARCHAR2', 'VARCHAR'))
        verify_res['manual'] = None
    else:
        verify_res['formal'] = None
        verify_res['manual'] = None
    return verify_res, res_sql_res
