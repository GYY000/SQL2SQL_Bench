# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: general_tree_analysis$
# @Author: 10379
# @Time: 2025/6/18 16:53
import json
import os

from tqdm import tqdm

from antlr_parser.Tree import TreeNode
from antlr_parser.mysql_tree import fetch_all_simple_select_from_select_stmt_mysql, rename_column_mysql, \
    fetch_main_select_from_select_stmt_mysql
from antlr_parser.oracle_tree import fetch_all_simple_select_from_subquery_oracle
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import get_pg_main_select_node_from_select_stmt, fetch_all_simple_select_from_select_stmt_pg
from sql_gen.generator.ele_type.type_def import OptionType
from utils.tools import get_proj_root_path, get_table_col_name

with open(os.path.join(get_proj_root_path(), 'src',
                       'antlr_parser', 'aggr_funcs.json'), 'r') as file:
    aggregate_functions = json.load(file)


def inside_aggregate_function(dialect, tree_node: TreeNode):
    if dialect == 'mysql':
        while tree_node is not None and tree_node.value != 'selectStatement':
            if tree_node.value == 'overClause':
                return False
            if (tree_node.value == 'functionCall' or
                    tree_node.value == 'specificFunction' or tree_node.value == 'aggregateWindowedFunction'
                    or tree_node.value == 'nonAggregateWindowedFunction'):
                if tree_node.get_child_by_value('overClause') is not None:
                    return False
                string = str(tree_node).upper().strip()
                val = string[:string.find('(')].strip()
                for value in aggregate_functions[dialect]:
                    if value.lower() == val.lower():
                        return True
            tree_node = tree_node.father
        return False
    elif dialect == 'pg':
        while tree_node is not None and tree_node.value != 'simple_select_pramary':
            if tree_node.value == 'over_clause':
                return False
            if tree_node.value == 'func_expr' or (tree_node.value == 'func_application'
                                                  and tree_node.father is None):
                string = str(tree_node).upper().strip()
                val = string[:string.find('(')].strip()
                for value in aggregate_functions[dialect]:
                    if value.lower() == val.lower():
                        return True
            tree_node = tree_node.father
        return False
    elif dialect == 'oracle':
        while tree_node is not None and tree_node.value != 'query_block':
            if tree_node.value == 'keep_clause' or tree_node.value == 'over_clause':
                return True
            if (tree_node.value == 'standard_function' or tree_node.value == 'string_function'
                    or tree_node.value == 'numeric_function_wrapper' or tree_node.value == 'json_function'
                    or tree_node.value == 'other_function' or tree_node.value == 'general_element_part'):
                string = str(tree_node).upper().strip()
                val = string[:string.find('(')].strip()
                for value in aggregate_functions[dialect]:
                    if value.lower() == val.lower():
                        return True
            tree_node = tree_node.father
        return False
    else:
        assert False


def father_value_list_compare(root_node: TreeNode, father_list: list[str | list]) -> bool:
    now_father = root_node.father
    for father in father_list:
        if isinstance(father, list):
            if now_father is None:
                return False
            else:
                flag = False
                for possible_father in father:
                    if now_father.value.lower() == possible_father.lower():
                        flag = True
                        break
                if not flag:
                    return False
        else:
            if now_father is None or now_father.value.lower() != father.lower():
                return False
        now_father = now_father.father
    return True


def child_value_list_compare(root_node: TreeNode, child_list: list[str]) -> bool:
    now_child = root_node
    for child in child_list:
        if len(now_child.children) == 0:
            return False
        now_child = now_child.children[0]
        if isinstance(child, list):
            flag = False
            for possible_child in child:
                if now_child.value.lower() == possible_child.lower():
                    flag = True
                    break
            if not flag:
                return False
        else:
            if now_child.value.lower() != child.lower():
                return False
    return True


def in_oracle_column_table_name_path(root_node: TreeNode) -> bool:
    if (father_value_list_compare(root_node, ['id_expression', 'general_element_part']) and
            not child_value_list_compare(root_node,
                                         ['non_reserved_keywords_pre12c',
                                          ['NULLIF', 'SYSDATE', "ROWNUM", "TRUNC", "GROUPING", "CURRENT_TIMESTAMP",
                                           "CURRENT_DATE"]])):
        return True
    elif father_value_list_compare(root_node, ['regular_id', 'id_expression', 'identifier',
                                               'tableview_name', 'dml_table_expression_clause',
                                               'table_ref_aux_internal',
                                               'table_ref_aux', 'table_ref']) and not root_node.value == 'dual':
        return True
    elif father_value_list_compare(root_node,
                                   ['id_expression', 'identifier', 'tableview_name']) and not child_value_list_compare(
        root_node, ['dual']):
        return True
    elif father_value_list_compare(root_node, ['id_expression', 'table_element']):
        return True
    elif father_value_list_compare(root_node, ['id_expression', 'tableview_name']):
        return True
    elif father_value_list_compare(root_node, ['id_expression', 'identifier', 'table_alias']):
        return True
    elif father_value_list_compare(root_node, ['id_expression', 'identifier', 'query_name']):
        return True
    elif father_value_list_compare(root_node, ['id_expression', 'identifier', 'column_alias']):
        return True
    elif father_value_list_compare(root_node, ['qualified_name', 'relation_expr']):
        return True
    return False


def in_pg_column_table_name_path(root_node: TreeNode) -> bool:
    if father_value_list_compare(root_node, ['colid', 'columnref']):
        return True
    elif father_value_list_compare(root_node, ['collabel', 'attr_name']):
        return True
    elif father_value_list_compare(root_node, ['collabel', 'target_el']):
        return True
    elif father_value_list_compare(root_node, ['table_alias']):
        return True
    elif father_value_list_compare(root_node, ['qualified_name', 'relation_expr']):
        return True
    else:
        return False


def in_mysql_column_table_name_path(root_node: TreeNode) -> bool:
    if father_value_list_compare(root_node, ['uid', 'tableSourceItem']):
        return True
    elif (father_value_list_compare(root_node, ['uid', 'fullId'])
          and not father_value_list_compare(root_node, ['uid', 'fullId', 'functionCall'])):
        return True
    # elif father_value_list_compare(root_node, ['stringLiteral', 'constant']):
    #     return True
    elif father_value_list_compare(root_node, ['uid', 'simpleId', 'cteName']):
        return True
    elif father_value_list_compare(root_node, ['uid', 'fullColumnName']):
        if len(root_node.children) != 0 and root_node.children[0].value == 'scalarFunctionName':
            if str(root_node.children[0]).upper() in ['CURRENT_TIMESTAMP', 'LOCALTIME', 'LOCALTIMESTAMP', 'SYSDATE',
                                                      'UTC_TIMESTAMP', 'UTC_DATE', 'UTC_TIME',
                                                      'CURRENT_DATE', 'CURRENT_TIME', 'CURTIME']:
                return False
        return True
    elif father_value_list_compare(root_node, ['uid', 'cteName', 'commonTableExpression']):
        return True
    elif father_value_list_compare(root_node, ['qualified_name', 'relation_expr']):
        return True
    elif father_value_list_compare(root_node, ['uid', 'selectElement']):
        return True
    return False


def fetch_all_uppermost_subquery_in_select_stmt(select_stmt_node: TreeNode, dialect: str):
    res = []
    if dialect == 'mysql':
        if select_stmt_node.value == 'fromClause':
            if select_stmt_node.get_child_by_value('expression') is not None:
                return fetch_all_uppermost_subquery_in_select_stmt(select_stmt_node.get_child_by_value('expression'),
                                                                   dialect)
        if select_stmt_node.value == 'querySpecification' or select_stmt_node.value == 'querySpecificationNointo':
            return [select_stmt_node]
    elif dialect == 'pg':
        if select_stmt_node.value == 'from_clause':
            return []
        if select_stmt_node.value == 'simple_select_pramary':
            return [select_stmt_node]
    elif dialect == 'oracle':
        if select_stmt_node.value == 'from_clause':
            return []
        if select_stmt_node.value == 'query_block':
            return [select_stmt_node]
    else:
        assert False
    for node in select_stmt_node.children:
        res = res + fetch_all_uppermost_subquery_in_select_stmt(node, dialect)
    return res


def dfs_table_ref_node(table_ref_node: TreeNode):
    res = [table_ref_node]
    if len(table_ref_node.get_children_by_value('table_ref')) > 0:
        for child in table_ref_node.get_children_by_value('table_ref'):
            res += dfs_table_ref_node(child)
    else:
        res.append(table_ref_node)
    return res


def analyze_select_stmt_used_tables(select_stmt_node: TreeNode, cte_names: list[list], dialect):
    res = set()
    cur_layer_tables = []
    if dialect == 'mysql':
        from_clause_node = select_stmt_node.get_child_by_value('fromClause')
        if from_clause_node is not None:
            table_source_nodes = from_clause_node.get_children_by_path(['tableSources', 'tableSource'])
            table_source_items = []
            for table_source_node in table_source_nodes:
                table_source_item_node = table_source_node.get_child_by_value('tableSourceItem')
                table_source_items.append(table_source_item_node)
                assert table_source_item_node is not None
                join_part_nodes = table_source_node.get_children_by_value('joinPart')
                for join_part_node in join_part_nodes:
                    table_source_item_node = join_part_node.get_child_by_value('tableSourceItem')
                    table_source_items.append(table_source_item_node)
            for table_source_item_node in table_source_items:
                if table_source_item_node.get_child_by_value('tableSources') is not None:
                    assert False
                if table_source_item_node.get_child_by_value('dmlStatement') is not None:
                    if table_source_item_node.get_child_by_value('LATERAL') is not None:
                        cte_names.append(cur_layer_tables)
                    query_body_node = table_source_item_node.get_child_by_value('dmlStatement')
                    select_statement_node = query_body_node.get_child_by_value('selectStatement')
                    select_stmts = fetch_all_simple_select_from_select_stmt_mysql(select_statement_node)
                    for simple_select_stmt_node in select_stmts:
                        select_res = analyze_select_stmt_used_tables(simple_select_stmt_node, cte_names, dialect)
                        for tbl in select_res:
                            res.add(tbl)
                    if table_source_item_node.get_child_by_value('LATERAL') is not None:
                        cte_names.remove(cur_layer_tables)
                    if table_source_item_node.get_child_by_value('uid') is not None:
                        name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
                        cur_layer_tables.append(name)
                else:
                    assert table_source_item_node.get_child_by_value('tableName') is not None
                    name = str(table_source_item_node.get_child_by_value('tableName'))
                    name = name.strip('`')
                    if not find_in_stack(cte_names, name):
                        res.add(name)
                    if table_source_item_node.get_child_by_value('uid') is not None:
                        name = str(table_source_item_node.get_child_by_value('uid')).strip('`')
                    cur_layer_tables.append(name)
    elif dialect == 'pg':
        from_clause_node = select_stmt_node.get_child_by_value('from_clause')
        if from_clause_node is not None:
            assert isinstance(from_clause_node, TreeNode)
            table_ref_nodes = from_clause_node.get_children_by_path(['from_list', 'table_ref'])
            all_table_ref_nodes = []
            for table_ref_node in table_ref_nodes:
                all_table_ref_nodes += dfs_table_ref_node(table_ref_node)
            for table_ref_node in all_table_ref_nodes:
                if table_ref_node.get_child_by_value('relation_expr') is not None:
                    name = str(table_ref_node.get_child_by_value('relation_expr')).strip('"')
                    if not find_in_stack(cte_names, name):
                        res.add(name)
                    if table_ref_node.get_child_by_value('opt_alias_clause') is not None:
                        alias_name = table_ref_node.get_children_by_path(
                            ['opt_alias_clause', 'table_alias_clause', 'table_alias'])
                        assert len(alias_name) > 0
                        name = str(alias_name[0]).strip('"')
                    cur_layer_tables.append(name)
                elif table_ref_node.get_child_by_value('select_with_parens') is not None:
                    if table_ref_node.get_child_by_value('LATERAL') is not None:
                        cte_names.append(cur_layer_tables)
                    select_stmt_nodes = fetch_all_simple_select_from_select_stmt_pg(
                        table_ref_node.get_child_by_value('select_with_parens'))
                    for simple_select_stmt_node in select_stmt_nodes:
                        select_res = analyze_select_stmt_used_tables(simple_select_stmt_node, cte_names, dialect)
                        for tbl in select_res:
                            res.add(tbl)
                    if table_ref_node.get_child_by_value('LATERAL') is not None:
                        cte_names.remove(cur_layer_tables)
                    if table_ref_node.get_child_by_value('opt_alias_clause') is not None:
                        alias_name = table_ref_node.get_children_by_path(
                            ['opt_alias_clause', 'table_alias_clause', 'table_alias'])
                        assert len(alias_name) > 0
                        name = str(alias_name[0]).strip('"')
                        cur_layer_tables.append(name)
    elif dialect == 'oracle':
        from_clause_node = select_stmt_node.get_child_by_value('from_clause')
        if from_clause_node is not None:
            assert isinstance(from_clause_node, TreeNode)
            table_ref_nodes = from_clause_node.get_children_by_path(['table_ref_list', 'table_ref'])
            all_table_ref_aux_nodes = []
            for table_ref_node in table_ref_nodes:
                table_ref_aux_node = table_ref_node.get_child_by_value('table_ref_aux')
                assert table_ref_aux_node is not None
                all_table_ref_aux_nodes.append(table_ref_aux_node)
                for join_clause in table_ref_node.get_children_by_value('join_clause'):
                    table_ref_aux_node = join_clause.get_child_by_value('table_ref_aux')
                    assert table_ref_aux_node is not None
                    all_table_ref_aux_nodes.append(table_ref_aux_node)
            for table_ref_aux_node in all_table_ref_aux_nodes:
                table_ref_aux_internal_node = table_ref_aux_node.get_child_by_value('table_ref_aux_internal')
                assert table_ref_aux_internal_node is not None
                assert table_ref_aux_internal_node.get_child_by_value('table_ref_aux_internal') is None
                dml_table_expression_clause_node = table_ref_aux_internal_node.get_child_by_value(
                    'dml_table_expression_clause')
                if dml_table_expression_clause_node.get_child_by_value('tableview_name') is not None:
                    name = str(dml_table_expression_clause_node.get_child_by_value('tableview_name')).strip('"')
                    if not find_in_stack(cte_names, name):
                        # print(cte_names)
                        # print(name)
                        res.add(name)
                elif dml_table_expression_clause_node.get_child_by_value('select_statement') is not None:
                    subquery_node = dml_table_expression_clause_node.get_children_by_path(['select_statement',
                                                                                           'select_only_statement',
                                                                                           'subquery'])
                    assert len(subquery_node) == 1
                    subquery_node = subquery_node[0]
                    simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(subquery_node)
                    for simple_select_node in simple_select_nodes:
                        select_res = analyze_select_stmt_used_tables(simple_select_node, cte_names, dialect)
                        for tbl in select_res:
                            res.add(tbl)
                    name = None
                elif dml_table_expression_clause_node.get_child_by_value('subquery') is not None:
                    subquery_node = dml_table_expression_clause_node.get_child_by_value('subquery')
                    if dml_table_expression_clause_node.get_child_by_value('LATERAL') is not None:
                        cte_names.append(cur_layer_tables)
                    simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(subquery_node)
                    for simple_select_node in simple_select_nodes:
                        select_res = analyze_select_stmt_used_tables(simple_select_node, cte_names, dialect)
                        for tbl in select_res:
                            res.add(tbl)
                    if dml_table_expression_clause_node.get_child_by_value('LATERAL') is not None:
                        cte_names.remove(cur_layer_tables)
                    name = None
                else:
                    continue
                alias_node = table_ref_aux_node.get_child_by_value('table_alias')
                if alias_node is not None:
                    name = str(alias_node).strip('"')
                if name is not None:
                    cur_layer_tables.append(name)
    else:
        assert False
    to_dfs_select_stmt_nodes = []
    for child in select_stmt_node.children:
        to_dfs_select_stmt_nodes = to_dfs_select_stmt_nodes + fetch_all_uppermost_subquery_in_select_stmt(child,
                                                                                                          dialect)
    for select_stmt_node in to_dfs_select_stmt_nodes:
        res = res.union(analyze_select_stmt_used_tables(select_stmt_node, cte_names, dialect))
    return res


def find_in_stack(stack: list[list], value):
    for i in range(len(stack)):
        if value in stack[i]:
            return True
    return False


def add_to_stack(stack: list[list], value):
    assert len(stack) > 0
    stack[-1].append(value)


def fetch_all_table_in_sql(sql: str, dialect):
    # we do not allow using with statement in subquery
    root_node, _, _, _ = parse_tree(sql, dialect)
    if root_node is None:
        return None
    root_node = TreeNode.make_g4_tree_by_node(root_node, dialect)
    # use stack
    derived_tables = [[]]
    used_tables_set = set()
    if dialect == 'mysql':
        with_stmt_node = root_node.get_children_by_path(
            ['sqlStatements', 'sqlStatement', 'dmlStatement', 'withStatement'])
        if len(with_stmt_node) != 0:
            assert len(with_stmt_node) == 1
            with_stmt_node = with_stmt_node[0]
            common_table_expressions = with_stmt_node.get_children_by_value('commonTableExpression')
            for cte_root_node in common_table_expressions:
                cte_name = str(cte_root_node.get_child_by_value('cteName')).strip('`')
                query_body_node = cte_root_node.get_child_by_value('dmlStatement')
                select_stmt_node = query_body_node.get_child_by_value('selectStatement')
                select_stmts = fetch_all_simple_select_from_select_stmt_mysql(select_stmt_node)
                add_to_stack(derived_tables, cte_name)
                for select_stmt_node in select_stmts:
                    res = analyze_select_stmt_used_tables(select_stmt_node, derived_tables, dialect)
                    for tbl in res:
                        used_tables_set.add(tbl)
        select_statement_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                'dmlStatement', 'selectStatement'])
        assert len(select_statement_node) == 1
        select_statement_node = select_statement_node[0]
        select_stmts = fetch_all_simple_select_from_select_stmt_mysql(select_statement_node)
        for select_stmt_nodes in select_stmts:
            res = analyze_select_stmt_used_tables(select_stmt_nodes, derived_tables, dialect)
            for tbl in res:
                used_tables_set.add(tbl)
    elif dialect == 'pg':
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        select_main_node = get_pg_main_select_node_from_select_stmt(select_stmt_node)
        with_clause_node = select_main_node.get_child_by_value('with_clause')
        if with_clause_node is not None:
            cte_nodes = with_clause_node.get_children_by_path(['cte_list', 'common_table_expr'])
            for cte_node in cte_nodes:
                cte_name = str(cte_node.get_child_by_value('name')).strip('"')
                query_body_node = cte_node.get_children_by_path(['preparablestmt', 'selectstmt'])
                assert len(query_body_node) == 1
                query_body_node = query_body_node[0]
                simple_select_nodes = fetch_all_simple_select_from_select_stmt_pg(query_body_node)
                add_to_stack(derived_tables, cte_name)
                for simple_select_node in simple_select_nodes:
                    res = analyze_select_stmt_used_tables(simple_select_node, derived_tables, dialect)
                    for tbl in res:
                        used_tables_set.add(tbl)
        simple_select_nodes = fetch_all_simple_select_from_select_stmt_pg(select_stmt_node)
        for simple_select_node in simple_select_nodes:
            res = analyze_select_stmt_used_tables(simple_select_node, derived_tables, dialect)
            for tbl in res:
                used_tables_set.add(tbl)
    elif dialect == 'oracle':
        select_stmt_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                           'select_statement', 'select_only_statement'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        with_clause_node = select_stmt_node.get_child_by_value('with_clause')
        if with_clause_node is not None:
            cte_nodes = with_clause_node.get_children_by_value('with_factoring_clause')
            for cte_node in cte_nodes:
                query_factoring_clause_node = cte_node.get_child_by_value('subquery_factoring_clause')
                query_body_node = query_factoring_clause_node.get_child_by_value('subquery')
                cte_name = str(query_factoring_clause_node.get_child_by_value('query_name')).strip('"')
                simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(query_body_node)
                add_to_stack(derived_tables, cte_name)
                for simple_select_node in simple_select_nodes:
                    res = analyze_select_stmt_used_tables(simple_select_node, derived_tables, dialect)
                    for tbl in res:
                        used_tables_set.add(tbl)
        subquery_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                        'select_statement', 'select_only_statement', 'subquery'])
        if len(subquery_node) != 1:
            print('FOR UPDATE haven\'t been supported yet')
            assert False
        select_stmt_node = subquery_node[0]
        simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(select_stmt_node)
        for simple_select_node in simple_select_nodes:
            res = analyze_select_stmt_used_tables(simple_select_node, derived_tables, dialect)
            for tbl in res:
                used_tables_set.add(tbl)
    else:
        assert False
    return used_tables_set


def fetch_query_body_node(root_node: TreeNode, dialect: str) -> TreeNode:
    if dialect == 'oracle':
        main_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                    'select_statement'])
        assert len(main_node) == 1
        main_node = main_node[0]
        select_only_node = main_node.get_child_by_value('select_only_statement')
        assert select_only_node is not None
        if select_only_node.get_child_by_value('with_clause') is not None:
            select_only_node.rm_child_by_value('with_clause')
    elif dialect == 'mysql':
        main_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                    'dmlStatement', 'selectStatement'])
        assert len(main_node) == 1
        main_node = main_node[0]
    elif dialect == 'pg':
        main_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti',
                                                    'stmt', 'selectstmt', 'select_no_parens'])
        assert len(main_node) == 1
        main_node = main_node[0]
        if main_node.get_child_by_value('with_clause') is not None:
            main_node.rm_child_by_value('with_clause')
    else:
        assert False
    return main_node


def build_ctes(ctes: dict, dialect: str):
    if len(ctes['cte_list']) == 0:
        return ''
    if dialect == 'oracle':
        with_clauses = 'WITH '
        for cte in ctes['cte_list']:
            if 'search_clause' not in cte or cte['search_clause'] is None:
                search_clause = ''
            else:
                search_clause = cte['search_clause']
            if 'cycle_clause' not in cte or cte['cycle_clause'] is None:
                cycle_clause = ''
            else:
                cycle_clause = cte['cycle_clause']
            if cte['column_list'] is None:
                cte_str = (f'{get_table_col_name(cte["cte_name"], dialect)} AS ({str(cte["query"])}) '
                           f'{search_clause} {cycle_clause}')
            else:
                cols = ''
                for col in cte['column_list']:
                    if cols != '':
                        cols += ', '
                    cols += f'{get_table_col_name(col, dialect)}'
                cte_str = f'{cte["cte_name"]} ({cols}) AS ({str(cte["query"])}) {search_clause} {cycle_clause}'
            if with_clauses == 'WITH ':
                with_clauses += cte_str
            else:
                with_clauses += f',\n {cte_str}'
    else:
        with_clauses = 'WITH '
        if ctes['is_recursive']:
            with_clauses = 'WITH RECURSIVE '
        for cte in ctes['cte_list']:
            if cte['column_list'] is None:
                cte_str = f'{get_table_col_name(cte["cte_name"], dialect)} AS ({str(cte["query"])})'
            else:
                cols = ''
                for col in cte['column_list']:
                    if cols != '':
                        cols += ', '
                    cols += get_table_col_name(col, dialect)
                cte_str = f'{get_table_col_name(cte["cte_name"], dialect)} ({cols}) AS ({str(cte["query"])})'
            if with_clauses == 'WITH ' or with_clauses == 'WITH RECURSIVE ':
                with_clauses += cte_str
            else:
                with_clauses += f',\n {cte_str}'
    return with_clauses


def fetch_all_ctes(root_node: TreeNode, dialect: str) -> tuple[bool, dict]:
    cte_list = []
    is_recursive = False
    if dialect == 'mysql':
        with_stmt_node = root_node.get_children_by_path(
            ['sqlStatements', 'sqlStatement', 'dmlStatement', 'withStatement'])
        if len(with_stmt_node) == 0:
            # No cte
            return True, {
                "is_recursive": False,
                "cte_list": []
            }
        assert len(with_stmt_node) == 1
        with_stmt_node = with_stmt_node[0]
        if with_stmt_node.get_child_by_value('RECURSIVE') is not None:
            is_recursive = True
        common_table_expressions = with_stmt_node.get_children_by_value('commonTableExpression')
        for cte_root_node in common_table_expressions:
            cte_name = str(cte_root_node.get_child_by_value('cteName')).strip('`')
            query_body_node = cte_root_node.get_children_by_path(
                ['dmlStatement', 'selectStatement'])
            assert len(query_body_node) == 1
            query_body_node = query_body_node[0]
            alias_nodes = cte_root_node.get_children_by_value('cteColumnName')
            column_list = None
            if len(alias_nodes) > 0:
                # deduplicate col_name
                rename_dict = {}
                column_list = []
                for idx, col_node in enumerate(alias_nodes):
                    col_name = str(col_node).strip('`')
                    if col_name in rename_dict:
                        new_name = f"{col_name}_{rename_dict[col_name]}"
                        assert len(col_node.children) == 1
                        assert isinstance(col_node, TreeNode)
                        col_node.children[0].value = new_name
                        rename_dict[col_name] = rename_dict[col_name] + 1
                        col_node.children[0].is_terminal = True
                    else:
                        new_name = col_name
                        rename_dict[col_name] = 1
                    column_list.append(new_name)
            cte_list.append({
                'cte_name': cte_name,
                'query': query_body_node,
                'column_list': column_list,
            })
        return True, {
            "is_recursive": is_recursive,
            "cte_list": cte_list
        }
    elif dialect == 'pg':
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        select_main_node = get_pg_main_select_node_from_select_stmt(select_stmt_node)
        with_clause_node = select_main_node.get_child_by_value('with_clause')
        if with_clause_node is None:
            return True, {
                "is_recursive": False,
                "cte_list": []
            }
        if with_clause_node.get_child_by_value('RECURSIVE') is not None:
            is_recursive = True
        cte_nodes = with_clause_node.get_children_by_path(['cte_list', 'common_table_expr'])
        for cte_node in cte_nodes:
            cte_name = str(cte_node.get_child_by_value('name')).strip('"')
            query_body_node = cte_node.get_children_by_path(['preparablestmt', 'selectstmt'])
            assert len(query_body_node) == 1
            query_body_node = query_body_node[0]
            alias_nodes = cte_node.get_children_by_path(['opt_name_list', 'name_list', 'name'])
            column_list = None
            if len(alias_nodes) > 0:
                # deduplicate col_name
                rename_dict = {}
                column_list = []
                for idx, col_node in enumerate(alias_nodes):
                    col_name = str(col_node).strip('"')
                    if col_name in rename_dict:
                        new_name = f"{col_name}_{rename_dict[col_name]}"
                        assert len(col_node.children) == 1
                        assert isinstance(col_node, TreeNode)
                        col_node.children[0].value = new_name
                        rename_dict[col_name] = rename_dict[col_name] + 1
                        col_node.children[0].is_terminal = True
                    else:
                        new_name = col_name
                        rename_dict[col_name] = 1
                    column_list.append(new_name)
            cte_list.append(
                {
                    'cte_name': cte_name,
                    'query': query_body_node,
                    'column_list': column_list
                }
            )
        return True, {
            "is_recursive": is_recursive,
            "cte_list": cte_list
        }
    elif dialect == 'oracle':
        select_stmt_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                           'select_statement', 'select_only_statement'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        with_clause_node = select_stmt_node.get_child_by_value('with_clause')
        if with_clause_node is None:
            return True, {
                "is_recursive": False,
                "cte_list": []
            }
        cte_nodes = with_clause_node.get_children_by_value('with_factoring_clause')
        for cte_node in cte_nodes:
            assert cte_node.get_child_by_value('subquery_factoring_clause') is not None
            query_factoring_clause_node = cte_node.get_child_by_value('subquery_factoring_clause')
            query_body_node = query_factoring_clause_node.get_child_by_value('subquery')
            search_clause = query_factoring_clause_node.get_child_by_value('search_clause')
            cycle_clause = query_factoring_clause_node.get_child_by_value('cycle_clause')
            if search_clause is not None:
                search_clause = str(search_clause)
            if cycle_clause is not None:
                cycle_clause = str(cycle_clause)
            cte_name = str(query_factoring_clause_node.get_child_by_value('query_name')).strip('"')
            column_list = None
            if query_factoring_clause_node.get_child_by_value('paren_column_list') is not None:
                column_alias_list_node = (query_factoring_clause_node.get_child_by_value('paren_column_list').
                                          get_child_by_value('column_list'))
                rename_dict = {}
                column_list = []
                for idx, col_node in enumerate(column_alias_list_node.get_children_by_value('column_name')):
                    col_name = str(col_node).strip('"')
                    if col_name in rename_dict:
                        new_name = f"{col_name}_{rename_dict[col_name]}"
                        assert len(col_node.children) == 1
                        assert isinstance(col_node, TreeNode)
                        col_node.children[0].value = new_name
                        rename_dict[col_name] = rename_dict[col_name] + 1
                        col_node.children[0].is_terminal = True
                    else:
                        new_name = col_name
                        rename_dict[col_name] = 1
                    column_list.append(new_name)
            cte_list.append({
                'cte_name': cte_name,
                'query': query_body_node,
                'column_list': column_list,
                'search_clause': search_clause,
                'cycle_clause': cycle_clause
            })
        return True, {
            "is_recursive": is_recursive,
            "cte_list": cte_list
        }
    else:
        assert False
