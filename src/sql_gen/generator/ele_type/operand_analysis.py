# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: operand_analysis$
# @Author: 10379
# @Time: 2025/4/1 17:01
import json
import os
import re

from antlr_parser.Tree import TreeNode
from antlr_parser.general_tree_analysis import build_ctes
from antlr_parser.mysql_tree import rename_column_mysql, fetch_main_select_from_select_stmt_mysql, \
    fetch_all_simple_select_from_select_stmt_mysql, \
    analyze_mysql_table_sources, get_select_statement_node_from_root
from antlr_parser.oracle_tree import fetch_main_select_from_subquery_oracle, general_element_only_oracle, \
    rename_column_oracle, fetch_all_simple_select_from_subquery_oracle, analyze_table_refs_oracle, parse_oracle_group_by
from antlr_parser.parse_tree import parse_tree
from antlr_parser.pg_tree import get_pg_main_select_node_from_select_stmt, fetch_main_select_from_select_stmt_pg, \
    only_column_ref_pg, \
    rename_column_pg, fetch_all_simple_select_from_select_stmt_pg, analyze_pg_table_refs, parse_pg_group_by
from sql_gen.generator.ele_type.SemanticAttribute import SemanticAttribute
from sql_gen.generator.ele_type.type_conversion import type_mapping
from sql_gen.generator.ele_type.type_operation import load_col_type
from sql_gen.generator.element.Operand import ColumnOp, Operand
from utils.db_connector import get_mysql_type, get_pg_type, get_oracle_type
from utils.tools import get_table_col_name, get_data_path, get_all_db_name, get_db_ids, get_used_reserved_keyword_list


def fetch_table_types_from_db(dialect, table_name: str, alias_name: str | None):
    db_ids = get_db_ids()
    for db_id in db_ids:
        with open(os.path.join(get_data_path(), db_id, 'schema.json')) as f:
            schema = json.load(f)
        if table_name not in schema:
            continue
        table_schema = schema[table_name]
        res = []
        for col in table_schema['cols']:
            col_type, _, _ = load_col_type(col['type'], col['col_name'], dialect, db_id, table_name)
            if col_type.get_type_name(dialect) is None:
                continue
            if 'semantic' in col:
                semantic_attribute = SemanticAttribute(col['semantic'])
            else:
                semantic_attribute = SemanticAttribute(None)
            if alias_name is None:
                res.append(ColumnOp(dialect, col['col_name'], table_name, col_type, semantic_attribute))
            else:
                res.append(ColumnOp(dialect, col['col_name'], alias_name, col_type, semantic_attribute))
        return res


def analysis_ctes(root_node: TreeNode, dialect: str) -> tuple[bool, dict]:
    res = []
    name_dict = {}
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
            query_body_node = cte_root_node.get_child_by_value('dmlStatement')
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
            with_clauses = build_ctes(
                {
                    "is_recursive": is_recursive,
                    "cte_list": res + [
                        {
                            'cte_name': cte_name,
                            'query': str(query_body_node),
                            'column_list': column_list,
                            'cte_name_type_pairs': []
                        }
                    ]
                }, dialect
            )
            get_type_query = f"{with_clauses}\n SELECT * FROM {get_table_col_name(cte_name, dialect)} LIMIT 1"
            flag, cte_types = get_mysql_type(get_all_db_name(dialect), get_type_query, False)
            if not flag:
                raise ValueError(cte_types[0])
            select_statement_node = cte_root_node.get_children_by_path(
                ['dmlStatement', 'selectStatement'])
            assert len(select_statement_node) == 1
            select_main_node = fetch_main_select_from_select_stmt_mysql(select_statement_node[0])
            select_elements = select_main_node.get_children_by_path(['selectElements', 'selectElement'])
            assert len(select_elements) == len(cte_types)
            if column_list is not None:
                assert len(column_list) == len(cte_types)
                # just checking
                col_names = set()
                for i in range(len(column_list)):
                    assert column_list[i].strip('`') == cte_types[i]['col']
                    assert cte_types[i]['col'] not in col_names
                    col_names.add(cte_types[i]['col'])
            else:
                for i in range(len(cte_types)):
                    j = i + 1
                    if (select_elements[i].get_child_by_value('functionCall') is not None or
                            select_elements[i].get_child_by_value('expression') is not None):
                        if select_elements[i].get_child_by_value('uid') is None:
                            cte_types[i]['col'] = rename_column_mysql(select_elements[i], name_dict)
                    if cte_types[i]['col'] not in name_dict:
                        name_dict[cte_types[i]['col']] = 1
                    while j < len(cte_types):
                        if cte_types[i]['col'] == cte_types[j]['col']:
                            cte_types[j]['col'] = rename_column_mysql(select_elements[j], name_dict,
                                                                      cte_types[i]['col'])
                        j += 1
            res.append({
                'cte_name': cte_name,
                'query': str(query_body_node),
                'column_list': column_list,
                'cte_name_type_pairs': cte_types
            })
        return True, {
            "is_recursive": is_recursive,
            "cte_list": res
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
            with_clauses = build_ctes(
                {
                    "is_recursive": is_recursive,
                    "cte_list": res + [
                        {
                            'cte_name': cte_name,
                            'query': str(query_body_node),
                            'column_list': column_list,
                            'cte_name_type_pairs': []
                        }
                    ]
                }, dialect
            )
            get_type_query = f"{with_clauses}\n SELECT * FROM {get_table_col_name(cte_name, dialect)} LIMIT 1"
            flag, cte_types = get_pg_type(get_all_db_name(dialect), get_type_query, False)
            if not flag:
                raise ValueError(cte_types[0])
            select_main_node = fetch_main_select_from_select_stmt_pg(query_body_node)
            target_list_node = select_main_node.get_child_by_value('target_list')
            if target_list_node is None:
                target_list_node = select_main_node.get_children_by_path(['opt_target_list', 'target_list'])
                assert len(target_list_node) == 1
                target_list_node = target_list_node[0]
            select_elements = target_list_node.get_children_by_value('target_el')
            assert len(select_elements) == len(cte_types)
            if column_list is not None:
                assert len(column_list) == len(cte_types)
                col_names = set()
                for i in range(len(column_list)):
                    assert column_list[i].strip('"') == cte_types[i]['col']
                    assert cte_types[i]['col'] not in col_names
                    col_names.add(cte_types[i]['col'])
            else:
                for i in range(len(select_elements)):
                    j = i + 1
                    if select_elements[i].get_child_by_value('*') is not None:
                        print('* is not Support yet')
                        assert False
                    else:
                        a_expr_node = select_elements[i].get_child_by_value('a_expr')
                        assert a_expr_node is not None
                        if not only_column_ref_pg(a_expr_node):
                            if select_elements[i].get_child_by_value('AS') is None:
                                cte_types[i]['col'] = rename_column_pg(select_elements[i], name_dict)
                        if cte_types[i]['col'] not in name_dict:
                            name_dict[cte_types[i]['col']] = 1
                        while j < len(cte_types):
                            if cte_types[i]['col'] == cte_types[j]['col']:
                                cte_types[j]['col'] = rename_column_pg(select_elements[j], name_dict,
                                                                       cte_types[i]['col'])
                            j += 1
            res.append({
                'cte_name': cte_name,
                'query': str(query_body_node),
                'column_list': column_list,
                'cte_name_type_pairs': cte_types
            })
        return True, {
            "is_recursive": is_recursive,
            "cte_list": res
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
            with_clauses = build_ctes(
                {
                    "is_recursive": is_recursive,
                    "cte_list": res + [
                        {
                            'cte_name': cte_name,
                            'query': str(query_body_node),
                            'column_list': column_list,
                            'search_clause': search_clause,
                            'cycle_clause': cycle_clause,
                            'cte_name_type_pairs': []
                        }
                    ]
                }, dialect
            )
            get_type_query = f"{with_clauses}\n SELECT * FROM {get_table_col_name(cte_name, dialect)} WHERE ROWNUM = 1"
            flag, cte_types = get_oracle_type(get_all_db_name(dialect), get_type_query, False)
            if not flag:
                raise ValueError(cte_types[0])
            select_main_node = fetch_main_select_from_subquery_oracle(query_body_node)
            selected_list_nodes = select_main_node.get_child_by_value('selected_list')
            assert selected_list_nodes is not None
            if selected_list_nodes.get_child_by_value('*') is not None:
                print('* is not Support yet')
                assert False
            select_elements = selected_list_nodes.get_children_by_value('select_list_elements')
            assert len(select_elements) == len(cte_types)
            if column_list is not None:
                assert len(column_list) == len(cte_types)
                col_names = set()
                for i in range(len(column_list)):
                    assert column_list[i].strip('"') == cte_types[i]['col']
                    assert cte_types[i]['col'] not in col_names
                    col_names.add(cte_types[i]['col'])
            else:
                for i in range(len(select_elements)):
                    j = i + 1
                    expr_node = select_elements[i].get_child_by_value('expression')
                    assert expr_node is not None
                    if not general_element_only_oracle(expr_node):
                        if select_elements[i].get_child_by_value('column_alias') is None:
                            cte_types[i]['col'] = rename_column_oracle(select_elements[i], name_dict)
                    if cte_types[i]['col'] not in name_dict:
                        name_dict[cte_types[i]['col']] = 1
                    while j < len(cte_types):
                        if cte_types[i]['col'] == cte_types[j]['col']:
                            cte_types[j]['col'] = rename_column_oracle(select_elements[j], name_dict,
                                                                       cte_types[i]['col'])
                        j = j + 1
            res.append({
                'cte_name': cte_name,
                'query': str(query_body_node),
                'column_list': column_list,
                'search_clause': search_clause,
                'cycle_clause': cycle_clause,
                'cte_name_type_pairs': cte_types
            })
        return True, {
            "is_recursive": is_recursive,
            "cte_list": res
        }
    else:
        assert False


def build_from_elem(elem_dict, dialect: str):
    if elem_dict['type'] == 'subquery':
        assert elem_dict['name'] is not None
        return (f"{'LATERAL' if elem_dict['lateral'] else ''} ({str(elem_dict['sub_query_node'])}) "
                f"AS {get_table_col_name(elem_dict['name'], dialect)}")
    elif elem_dict['type'] == 'table':
        assert elem_dict['name'] is not None
        if elem_dict['column_names'] is None:
            if elem_dict['alias'] is not None:
                return (f"{get_table_col_name(elem_dict['name'], dialect)} "
                        f"{get_table_col_name(elem_dict['alias'], dialect)}")
            return f"{get_table_col_name(elem_dict['name'], dialect)}"
        else:
            cols = ''
            for col in elem_dict['column_names']:
                if cols != '':
                    cols += ', '
                cols += f'{get_table_col_name(col, dialect)}'
            if elem_dict['alias'] is not None:
                return (f"{get_table_col_name(elem_dict['name'], dialect)} ({cols}) "
                        f"AS {get_table_col_name(elem_dict['alias'], dialect)}")
            return f"{get_table_col_name(elem_dict['name'], dialect)} ({cols})"
    elif elem_dict['type'] == 'other':
        return f"{str(elem_dict['content'])}"
    else:
        assert False


def analysis_res_cols_sql(db_name, root_node: TreeNode, dialect: str) -> tuple[bool, list]:
    # analysis tables and columns could be used for select, where having
    # return rename flag and column name list
    res = []
    name_dict = {}
    rename_flag = False
    if dialect == 'mysql':
        select_statement_node = get_select_statement_node_from_root(root_node)
        select_main_node = fetch_main_select_from_select_stmt_mysql(select_statement_node[0])
        # if len(select_main_node.get_children_by_path(['selectElements', '*'])) > 0:
        #     flag, all_cols = analysis_usable_cols_sql(db_name, sql, dialect)
        #     for col in all_cols:
        #         assert isinstance(col, ColumnOp)
        #         if col.value in name_dict:
        #             rename_flag = True
        #             col_name = f"{col.table_name}_{col.value}"
        #             if col_name in name_dict:
        #                 col_name = f"{col.table_name}_{col.value}_{name_dict[col_name]}"
        #                 name_dict[col_name] = name_dict[col_name] + 1
        #             else:
        #                 name_dict[col_name] = 1
        #         else:
        #             name_dict[col.value] = 1
        #             col_name = col.value
        #         res.append(col_name)
        select_elements = select_main_node.get_children_by_path(['selectElements', 'selectElement'])
        for select_element_node in select_elements:
            # TODO: haven't process value like tbl.*
            if select_element_node.get_child_by_value('functionCall') is not None or \
                    select_element_node.get_child_by_value('expression') is not None:
                if select_element_node.get_child_by_value('uid') is None:
                    col_name = rename_column_mysql(select_element_node, name_dict)
                else:
                    col_name = str(select_element_node.get_child_by_value('uid')).strip('`')
                    if col_name in name_dict:
                        col_name = rename_column_mysql(select_element_node, name_dict, col_name)
                    else:
                        name_dict[col_name] = 1
                res.append(col_name)
            else:
                if select_element_node.get_child_by_value('uid') is not None:
                    col_name = str(select_element_node.get_child_by_value('uid')).strip('`')
                else:
                    col_name = str(select_element_node.get_child_by_value('fullColumnName')).strip('`')
                if col_name in name_dict:
                    col_name = rename_column_mysql(select_element_node, name_dict, col_name)
                else:
                    name_dict[col_name] = 1
                res.append(col_name)
        return rename_flag, res


def analysis_usable_cols_sql(simple_select_node: TreeNode, dialect: str, ctes: dict) -> tuple[bool, list, str]:
    name_dict = {}
    if dialect == 'mysql':
        assert (simple_select_node.value == 'querySpecificationNointo'
                or simple_select_node.value == 'querySpecification')
        from_clause_node = simple_select_node.get_child_by_value('fromClause')
        if from_clause_node is None:
            return True, [], str(simple_select_node)
        table_sources_node = from_clause_node.get_child_by_value('tableSources')
        table_elements = analyze_mysql_table_sources(table_sources_node, dialect, name_dict)
        with_clauses = build_ctes(ctes, dialect)
        from_elems = ''
        ele_cnt = 0
        final_cols = []
        for i in range(len(table_elements)):
            if from_elems != '':
                from_elems += ', '
            if table_elements[i]['type'] == 'table':
                table_name = table_elements[i]['name']
                alias = table_elements[i]['alias']
                if table_name not in [cte["cte_name"] for cte in ctes['cte_list']]:
                    column_ops = fetch_table_types_from_db(dialect, table_name, alias)
                    ele_cnt += len(column_ops)
                    final_cols += column_ops
                    from_elems += build_from_elem(table_elements[i], dialect)
                    continue
            from_elems += build_from_elem(table_elements[i], dialect)
            sql = f"{with_clauses}\nSELECT * FROM {from_elems}"
            flag, types = get_mysql_type(get_all_db_name(dialect), sql, False)
            if not flag:
                print(sql)
                print(types)
                assert False
            j = ele_cnt
            newly_added_cols = []
            while j < len(types):
                owner_name = table_elements[i]['name']
                if 'alias' in table_elements[i] and table_elements[i]['alias'] is not None:
                    owner_name = table_elements[i]['alias']
                col = ColumnOp(dialect, types[j]['col'], owner_name,
                               type_mapping(dialect, types[j]['type']))
                j += 1
                newly_added_cols.append(col)
            final_cols = final_cols + newly_added_cols
            ele_cnt = j
        return True, final_cols, str(simple_select_node)
    elif dialect == 'pg':
        assert simple_select_node.value == 'simple_select_pramary'
        from_clause_node = simple_select_node.get_child_by_value('from_clause')
        if from_clause_node is None:
            return True, [], str(simple_select_node)
        table_refs = from_clause_node.get_children_by_path(['from_list', 'non_ansi_join', 'table_ref'])
        if len(table_refs) == 0:
            table_refs = from_clause_node.get_children_by_path(['from_list', 'table_ref'])
        assert len(table_refs) != 0
        table_elements = analyze_pg_table_refs(table_refs)
        with_clauses = build_ctes(ctes, dialect)
        from_elems = ''
        ele_cnt = 0
        final_cols = []
        for i in range(len(table_elements)):
            if from_elems != '':
                from_elems += ', '
            if table_elements[i]['type'] == 'table':
                table_name = table_elements[i]['name']
                alias = table_elements[i]['alias']
                if table_name not in [cte["cte_name"] for cte in ctes['cte_list']]:
                    column_ops = fetch_table_types_from_db(dialect, table_name, alias)
                    ele_cnt += len(column_ops)
                    final_cols += column_ops
                    from_elems += build_from_elem(table_elements[i], dialect)
                    continue
            from_elems += build_from_elem(table_elements[i], dialect)
            sql = f"{with_clauses}\nSELECT * FROM {from_elems}"
            flag, types = get_pg_type(get_all_db_name(dialect), sql, False)
            if not flag:
                print(sql)
                print(types)
                assert False
            j = ele_cnt
            newly_added_cols = []
            while j < len(types):
                owner_name = table_elements[i]['name']
                if 'alias' in table_elements[i] and table_elements[i]['alias'] is not None:
                    owner_name = table_elements[i]['alias']
                col = ColumnOp(dialect, types[j]['col'], owner_name,
                               type_mapping(dialect, types[j]['type']))
                j += 1
                newly_added_cols.append(col)
            final_cols = final_cols + newly_added_cols
            ele_cnt = j
        return True, final_cols, str(simple_select_node)
    elif dialect == 'oracle':
        assert simple_select_node.value == 'query_block'
        from_clause_node = simple_select_node.get_child_by_value('from_clause')
        if from_clause_node is None:
            return True, [], str(simple_select_node)
        table_ref_list_node = from_clause_node.get_child_by_value('table_ref_list')
        table_refs = table_ref_list_node.get_children_by_value('table_ref')
        if len(table_refs) == 1 and str(table_refs[0]).lower() == 'dual':
            return True, [], str(simple_select_node)
        table_elements = analyze_table_refs_oracle(table_refs)
        with_clauses = build_ctes(ctes, dialect)
        from_elems = ''
        ele_cnt = 0
        final_cols = []
        if len(ctes['cte_list']) > 0:
            ctes_list_str = ','.join(ctes['cte_list'][i]['cte_name'] for i in range(len(ctes['cte_list'])))
            mask_where_clause = f"WHERE EXISTS (SELECT 1 FROM {ctes_list_str} WHERE ROWNUM = 0);"
        else:
            mask_where_clause = ''
        for i in range(len(table_elements)):
            if from_elems != '':
                from_elems += ', '
            if table_elements[i]['type'] == 'table':
                table_name = table_elements[i]['name']
                alias = table_elements[i]['alias']
                if table_name not in [cte["cte_name"] for cte in ctes['cte_list']]:
                    column_ops = fetch_table_types_from_db(dialect, table_name, alias)
                    ele_cnt += len(column_ops)
                    final_cols += column_ops
                    from_elems += build_from_elem(table_elements[i], dialect)
                    continue
            from_elems += build_from_elem(table_elements[i], dialect)
            sql = f"{with_clauses}\nSELECT * FROM {from_elems} {mask_where_clause}"
            flag, types = get_oracle_type(get_all_db_name(dialect), sql, False)
            if not flag:
                print(sql)
                print(types)
                assert False
            j = ele_cnt
            newly_added_cols = []
            while j < len(types):
                owner_name = table_elements[i]['name']
                if 'alias' in table_elements[i] and table_elements[i]['alias'] is not None:
                    owner_name = table_elements[i]['alias']
                col = ColumnOp(dialect, types[j]['col'], owner_name,
                               type_mapping(dialect, types[j]['type']))
                j += 1
                newly_added_cols.append(col)
            final_cols = final_cols + newly_added_cols
            ele_cnt = j
        return True, final_cols, str(simple_select_node)
    else:
        assert False


def analysis_group_by_simple_select(simple_select_node: TreeNode, dialect: str,
                                    ctes: dict) -> tuple[bool, list | None]:
    if dialect == 'mysql':
        select_list = []
        group_by_node = simple_select_node.get_child_by_value('groupByClause')
        if group_by_node is None:
            return True, None
        items = group_by_node.get_children_by_value('groupByItem')
        for expression_node in items:
            assert isinstance(expression_node, TreeNode)
            node_expr = expression_node.get_child_by_value('expression')
            assert node_expr is not None
            select_list.append(node_expr)
        clone_node = simple_select_node.clone()
        clone_node.rm_child_by_value('groupByClause')
        clone_node.rm_child_by_value('havingClause')
        clone_node.rm_child_by_value('orderByClause')
        select_elements_node = clone_node.get_child_by_value('selectElements')
        assert isinstance(select_elements_node, TreeNode)
        new_str = ''
        for node in select_list:
            if new_str != '':
                new_str = new_str + ', '
            new_str = new_str + str(node)
        select_elements_node.value = ' ' + new_str + ' '
        select_elements_node.is_terminal = True
        get_type_sql = f"{build_ctes(ctes, dialect)}\n {str(clone_node)}"
        flag, res = get_mysql_type(get_all_db_name(dialect), get_type_sql, False)
        if not flag:
            print(get_type_sql)
            assert False
        group_by_cols = []
        assert len(select_list) == len(res)
        for idx, col in enumerate(res):
            group_by_cols.append(Operand(str(select_list[idx]), type_mapping(dialect, col['type'])))
        return True, group_by_cols
    elif dialect == 'pg':
        group_by_node = simple_select_node.get_child_by_value('group_clause')
        if group_by_node is None:
            return True, None
        items = group_by_node.get_child_by_value('group_by_list')
        expr_list = parse_pg_group_by(items)
        clone_node = simple_select_node.clone()
        select_elements_node = clone_node.get_child_by_value('opt_target_list')
        if select_elements_node is None:
            select_elements_node = clone_node.get_child_by_value('target_list')
        assert isinstance(select_elements_node, TreeNode)
        new_str = ''
        for node in expr_list:
            if new_str != '':
                new_str = new_str + ', '
            new_str = new_str + str(node)
        select_elements_node.value = ' ' + new_str + ' '
        select_elements_node.is_terminal = True
        clone_node.rm_child_by_value('group_clause')
        clone_node.rm_child_by_value('having_clause')
        flag, res = get_pg_type(get_all_db_name(dialect), f"{build_ctes(ctes, dialect)}\n {str(clone_node)}", False)
        if not flag:
            print(str(clone_node))
            assert False
        group_by_cols = []
        assert len(expr_list) == len(res)
        for idx, col in enumerate(res):
            group_by_cols.append(Operand(str(expr_list[idx]), type_mapping(dialect, col['type'])))
        return True, group_by_cols
    elif dialect == 'oracle':
        group_by_node = simple_select_node.get_child_by_value('group_by_clause')
        if group_by_node is None:
            return True, None
        items = group_by_node.get_children_by_value('group_by_elements')
        expr_list = parse_oracle_group_by(items)
        clone_node = simple_select_node.clone()
        select_elements_node = clone_node.get_child_by_value('selected_list')
        assert isinstance(select_elements_node, TreeNode)
        new_str = ''
        for node in expr_list:
            if new_str != '':
                new_str = new_str + ', '
            new_str = new_str + str(node)
        select_elements_node.value = ' ' + new_str + ' '
        select_elements_node.is_terminal = True
        clone_node.rm_child_by_value('group_by_clause')
        clone_node.rm_child_by_value('order_by_clause')
        flag, res = get_oracle_type(get_all_db_name(dialect), f"{build_ctes(ctes, dialect)}\n {str(clone_node)}", False)
        if not flag:
            print(f"{build_ctes(ctes, dialect)}\n {str(clone_node)}")
            assert False
        group_by_cols = []
        assert len(expr_list) == len(res)
        for idx, col in enumerate(res):
            group_by_cols.append(Operand(str(expr_list[idx]), type_mapping(dialect, col['type'])))
        return True, group_by_cols
    else:
        assert False


def rep_select_stmt_with_cte(select_stmt_nodes: list[TreeNode], src_dialect, ctes):
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
        select_stmt_nodes[key] = value


def analysis_sql(sql, dialect: str, filter_quote_flag: bool = True):
    root_node, _, _, _ = parse_tree(sql, dialect)
    if root_node is None:
        print(sql)
        print('parse error')
        return None
    root_node = TreeNode.make_g4_tree_by_node(root_node, dialect)
    flag, ctes = analysis_ctes(root_node, dialect)
    res = []
    select_stmts = []
    if not flag:
        return None
    res = None
    if dialect == 'mysql':
        select_statement_node = root_node.get_children_by_path(['sqlStatements', 'sqlStatement',
                                                                'dmlStatement', 'selectStatement'])
        assert len(select_statement_node) == 1
        select_statement_node = select_statement_node[0]
        if select_statement_node.get_child_by_value('lateralStatement') is not None:
            print(sql)
            print('lateralStatement not supported')
            return False, [], ''
        simple_select_nodes = fetch_all_simple_select_from_select_stmt_mysql(select_statement_node)
        rep_select_stmt_with_cte(simple_select_nodes, dialect, ctes)
        for simple_select_node in simple_select_nodes:
            flag_select, cols, rewrite_sql = analysis_usable_cols_sql(simple_select_node, dialect, ctes)
            flag_group_by, group_by_cols = analysis_group_by_simple_select(simple_select_node, dialect, ctes)
            select_stmts.append({
                "select_root_node": simple_select_node,
                "type": 'UNION',
                "cols": cols,
                "group_by_cols": group_by_cols
            })
        res = {
            "cte": ctes,
            "select_stmts": select_stmts,
            "root_node": root_node
        }
    elif dialect == 'pg':
        select_stmt_node = root_node.get_children_by_path(['stmtblock', 'stmtmulti', 'stmt', 'selectstmt'])
        assert len(select_stmt_node) == 1
        select_stmt_node = select_stmt_node[0]
        select_statement_node = get_pg_main_select_node_from_select_stmt(select_stmt_node)
        assert select_statement_node is not None
        simple_select_nodes = fetch_all_simple_select_from_select_stmt_pg(select_stmt_node)
        rep_select_stmt_with_cte(simple_select_nodes, dialect, ctes)
        for simple_select_node in simple_select_nodes:
            flag_select, cols, rewrite_sql = analysis_usable_cols_sql(simple_select_node, dialect, ctes)
            flag_group_by, group_by_cols = analysis_group_by_simple_select(simple_select_node, dialect, ctes)
            select_stmts.append({
                "select_root_node": simple_select_node,
                "type": 'UNION',  # Haven't been used yet
                "cols": cols,
                "group_by_cols": group_by_cols
            })
        res = {
            "cte": ctes,
            "select_stmts": select_stmts,
            "root_node": root_node
        }
    elif dialect == 'oracle':
        subquery_node = root_node.get_children_by_path(['unit_statement', 'data_manipulation_language_statements',
                                                        'select_statement', 'select_only_statement', 'subquery'])
        if len(subquery_node) != 1:
            print('FOR UPDATE haven\'t been supported yet')
            assert False
        select_stmt_node = subquery_node[0]
        simple_select_nodes = fetch_all_simple_select_from_subquery_oracle(select_stmt_node)
        rep_select_stmt_with_cte(simple_select_nodes, dialect, ctes)
        for simple_select_node in simple_select_nodes:
            flag_select, cols, rewrite_sql = analysis_usable_cols_sql(simple_select_node, dialect, ctes)
            flag_group_by, group_by_cols = analysis_group_by_simple_select(simple_select_node, dialect, ctes)
            select_stmts.append({
                "select_root_node": simple_select_node,
                "type": 'UNION',  # Haven't been used yet
                "cols": cols,
                "group_by_cols": group_by_cols
            })
        res = {
            "cte": ctes,
            "select_stmts": select_stmts,
            "root_node": root_node
        }
    else:
        assert False
    assert res is not None
    if filter_quote_flag:
        for select_stmt in res['select_stmts']:
            rm_col_list = []
            for col in select_stmt['cols']:
                assert isinstance(col, ColumnOp)
                used_mysql_column_name = get_table_col_name(col.column_name, 'mysql')
                used_pg_column_name = get_table_col_name(col.column_name, 'pg')
                used_oracle_column_name = get_table_col_name(col.column_name, 'oracle')
                used_mysql_tbl_name = get_table_col_name(col.table_name, 'mysql')
                used_pg_tbl_name = get_table_col_name(col.table_name, 'pg')
                used_oracle_tbl_name = get_table_col_name(col.table_name, 'oracle')
                if not (used_mysql_column_name == used_pg_column_name
                        and used_mysql_column_name == used_oracle_column_name):
                    rm_col_list.append(col)
                elif not (used_mysql_tbl_name == used_pg_tbl_name and used_oracle_tbl_name == used_mysql_tbl_name):
                    rm_col_list.append(col)
            for col in rm_col_list:
                select_stmt['cols'].remove(col)
    return res
