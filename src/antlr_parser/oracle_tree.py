# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: oracle_tree$
# @Author: 10379
# @Time: 2025/4/2 20:35
from antlr_parser.Tree import TreeNode
from utils.tools import get_table_col_name


# used for analyze oracle tree structure

def rename_sql_oracle(sql: str):
    pass


def fetch_main_select_from_subquery_oracle(subquery_node: TreeNode):
    subquery_basic_elements_node = subquery_node.get_child_by_value('subquery_basic_elements')
    assert subquery_basic_elements_node is not None
    query_block_node = subquery_basic_elements_node.get_child_by_value('query_block')
    if query_block_node is None:
        assert subquery_basic_elements_node.get_child_by_value('subquery') is not None
        return fetch_main_select_from_subquery_oracle(subquery_basic_elements_node.get_child_by_value('subquery'))
    else:
        return query_block_node


def general_element_only_oracle(expression_node: TreeNode):
    node = expression_node
    while node.value != 'general_element':
        if len(node.children) != 1:
            return False
    if node.value == 'general_element':
        return True
    else:
        return False


def rename_column_oracle(select_element_node: TreeNode, name_dict: dict, extend_name=None):
    if extend_name is None:
        extend_name = 'col'
    idx = name_dict.get(extend_name.upper(), 1)
    name_dict[extend_name.upper()] = idx + 1
    if select_element_node.get_child_by_value('column_alias') is not None:
        uid_node = select_element_node.get_child_by_value('identifier')
        if uid_node is None:
            uid_node = select_element_node.get_child_by_value('quoted_string')
        new_name_node = TreeNode(f"{extend_name.lower()}_{idx}", 'oracle', True)
        assert isinstance(uid_node, TreeNode)
        uid_node.children = [new_name_node]
    else:
        column_alias_node = TreeNode('column_alias', 'oracle', False)
        column_alias_node.add_child(TreeNode(f"{extend_name.lower()}_{idx}", 'oracle', True))
        select_element_node.add_child(column_alias_node)
    return f"{extend_name}_{idx}"


def fetch_all_simple_select_from_subquery_oracle(subquery_node: TreeNode):
    subquery_basic_elements_node = subquery_node.get_child_by_value('subquery_basic_elements')
    assert subquery_basic_elements_node is not None
    res = fetch_through_subquery_basic_elements(subquery_basic_elements_node)
    for part_node in subquery_node.get_children_by_value('subquery_operation_part'):
        res = res + fetch_through_subquery_basic_elements(part_node.get_child_by_value('subquery_basic_elements'))
    return res


def fetch_through_subquery_basic_elements(subquery_basic_elements_node: TreeNode):
    res = []
    query_block_node = subquery_basic_elements_node.get_child_by_value('query_block')
    if query_block_node is None:
        assert subquery_basic_elements_node.get_child_by_value('subquery') is not None
        res = res + fetch_all_simple_select_from_subquery_oracle(subquery_basic_elements_node.
                                                                 get_child_by_value('subquery'))
    else:
        res = [query_block_node]
    return res


def analyze_table_refs_oracle(table_refs: list):
    res = []
    name_dict = {}
    for table_ref in table_refs:
        assert table_ref.get_child_by_value('pivot_clause') is None and table_ref.get_child_by_value(
            'unpivot_clause') is None
        res.append(analyze_table_ref_aux(table_ref.get_child_by_value('table_ref_aux'), {}))
        for join_clause_node in table_ref.get_children_by_value('join_clause'):
            res.append(analyze_table_ref_aux(join_clause_node.get_child_by_value('table_ref_aux'), name_dict))
    return res


def analyze_table_ref_aux(table_ref_aux_node: TreeNode, name_dict: dict):
    table_ref_aux_internal_node = table_ref_aux_node.get_child_by_value('table_ref_aux_internal')
    rename_flag = True
    extension_name = None
    if table_ref_aux_node.get_child_by_value('table_alias') is not None:
        extension_name = str(table_ref_aux_node.get_child_by_value('table_alias')).strip('"')
        if extension_name.upper() not in name_dict:
            name_dict[extension_name.upper()] = 1
            rename_flag = False
    else:
        if table_ref_aux_internal_node.get_child_by_value('dml_table_expression_clause') is not None:
            dml_table_node = table_ref_aux_internal_node.get_child_by_value('dml_table_expression_clause')
            if dml_table_node.get_child_by_value('tableview_name') is not None:
                tableview_name_node = dml_table_node.get_child_by_value('tableview_name')
                if tableview_name_node.get_child_by_value('identifier') is not None:
                    identifier_node = tableview_name_node.get_child_by_value('identifier')
                    if identifier_node.get_child_by_value('id_expression') is not None:
                        extension_name = str(identifier_node.get_child_by_value('id_expression')).strip('"')
                        if extension_name.upper() not in name_dict:
                            name_dict[extension_name.upper()] = 1
                            rename_flag = False
    if rename_flag:
        if extension_name is None:
            extension_name = 'table'
        if extension_name.upper() not in name_dict:
            name_dict[extension_name.upper()] = 1
        else:
            name_dict[extension_name.upper()] = name_dict[extension_name.upper()] + 1
            extension_name = f"{extension_name}_{name_dict[extension_name.upper()] - 1}"
        if table_ref_aux_node.get_child_by_value('table_alias') is not None:
            new_node = TreeNode(f"{get_table_col_name(extension_name, 'oracle').lower()}", 'oracle', True)
            table_ref_aux_node.get_child_by_value('table_alias').children = [new_node]
        else:
            new_node = TreeNode('table_alias', 'oracle', False)
            new_node.add_child(TreeNode(f"{get_table_col_name(extension_name, 'oracle').lower()}", 'oracle', True))
            table_ref_aux_node.add_child(new_node)
    return {
        "type": "other",
        "name": extension_name,
        "content": table_ref_aux_node
    }


def parse_oracle_group_by(group_by_elements_nodes: list):
    all_expressions = []
    for group_by_element_node in group_by_elements_nodes:
        all_expressions = all_expressions + get_all_expression_nodes(group_by_element_node)
    return all_expressions


def get_all_expression_nodes(node: TreeNode):
    if node.value == 'expression':
        return [node]
    else:
        res = []
        for child in node.children:
            res = res + get_all_expression_nodes(child)
        return res


def fetch_operands_from_expression_node_oracle(expression_node: TreeNode):
    return dfs_node(expression_node)


def dfs_node(node: TreeNode):
    res = []
    if node.value == 'atom':
        if node.get_child_by_value('constant') is not None or node.get_child_by_value('general_element') is not None:
            return [node]
        else:
            for child in node.children:
                res = res + dfs_node(child)
    if node.value == 'subquery':
        return [node]
