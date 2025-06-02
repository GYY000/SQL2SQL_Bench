# -*- coding: utf-8 -*-
# @Project: LLM4DB
# @Module: normalize$
# @Author: 10379
# @Time: 2024/10/6 22:19
from typing import List

from antlr_parser.parse_tree import parse_tree
from antlr_parser.Tree import TreeNode
from utils.tools import get_used_reserved_keyword_list


def in_oracle_column_table_name_path(root_node) -> bool:
    if (father_value_list_compare(root_node, ['id_expression', 'general_element_part']) and
            not child_value_list_compare(root_node,
                                         ['non_reserved_keywords_pre12c', ['NULLIF', 'SYSDATE', "ROWNUM", "TRUNC"]])):
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
    elif father_value_list_compare(root_node, ['id_expression', 'identifier', 'table_alias']):
        return True
    elif father_value_list_compare(root_node, ['id_expression', 'identifier', 'query_name']):
        return True
    elif father_value_list_compare(root_node, ['id_expression', 'identifier', 'column_alias']):
        return True
    elif father_value_list_compare(root_node, ['qualified_name', 'relation_expr']):
        return True
    return False


def in_pg_column_table_name_path(root_node) -> bool:
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


def in_mysql_column_table_name_path(root_node) -> bool:
    if father_value_list_compare(root_node, ['uid', 'tableSourceItem']):
        return True
    elif father_value_list_compare(root_node, ['uid', 'fullId']):
        return True
    elif father_value_list_compare(root_node, ['stringLiteral', 'constant']):
        return True
    elif father_value_list_compare(root_node, ['uid', 'simpleId', 'cteName']):
        return True
    elif (father_value_list_compare(root_node, ['uid', 'fullColumnName']) and
          not (len(root_node.children) != 0 and root_node.children[0].value == 'scalarFunctionName')):
        return True
    elif father_value_list_compare(root_node, ['uid', 'cteName', 'commonTableExpression']):
        return True
    elif father_value_list_compare(root_node, ['qualified_name', 'relation_expr']):
        return True
    elif father_value_list_compare(root_node, ['uid', 'selectElement']):
        return True
    return False


def remove_as_mysql(root_node: TreeNode):
    remove_children = []
    if root_node.value in ['tableSourceItem', 'commonTableExpression', 'selectElement']:
        for child in root_node.children:
            if child.is_terminal and child.value == 'AS':
                remove_children.append(child)
        for child in remove_children:
            root_node.children.remove(child)
    for child in root_node.children:
        if not child.is_terminal:
            remove_as_mysql(child)


def remove_as_pg(root_node: TreeNode):
    remove_children = []
    if root_node.value in ['target_el', 'table_alias_clause']:
        for child in root_node.children:
            if child.is_terminal and child.value == 'AS':
                remove_children.append(child)
        for child in remove_children:
            root_node.children.remove(child)
    for child in root_node.children:
        if not child.is_terminal:
            remove_as_pg(child)


def add_quote_mysql(root_node: TreeNode, quote_type: str, lower_case_flag: bool):
    if in_mysql_column_table_name_path(root_node):
        add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    elif father_value_list_compare(root_node, ['dottedId', 'fullColumnName']):
        if root_node.is_terminal and not root_node.value == '.':
            assert root_node.value[0] == '.'
            src_str = root_node.value[1:]
            res = add_quote(src_str, quote_type, lower_case_flag)
            root_node.value = '.' + res
        elif root_node.is_terminal and root_node.value == '.':
            pass
        else:
            assert root_node.value == 'uid'
            add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    else:
        for child in root_node.children:
            add_quote_mysql(child, quote_type, lower_case_flag)


def remove_quote_mysql(root_node: TreeNode):
    if in_mysql_column_table_name_path(root_node):
        rm_quote_to_bot_node(root_node, '`')
    elif father_value_list_compare(root_node, ['dottedId', 'fullColumnName']):
        if root_node.is_terminal and not root_node.value == '.':
            assert root_node.value[0] == '.'
            src_str = root_node.value[1:]
            res = rm_quote(src_str, '`')
            root_node.value = '.' + res
        elif root_node.is_terminal and root_node.value == '.':
            pass
        else:
            assert root_node.value == 'uid'
            rm_quote_to_bot_node(root_node, '`')
    else:
        for child in root_node.children:
            remove_quote_mysql(child)


def child_value_list_compare(root_node: TreeNode, child_list: List[str]) -> bool:
    now_child = root_node
    for child in child_list:
        if len(now_child.children) == 0:
            return False
        now_child = now_child.children[0]
        if isinstance(child, list):
            if now_child.value not in child:
                return False
        else:
            if now_child.value != child:
                return False
    return True


def add_quote_oracle(root_node: TreeNode, quote_type: str, lower_case_flag: bool):
    if in_oracle_column_table_name_path(root_node):
        add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    else:
        for child in root_node.children:
            add_quote_oracle(child, quote_type, lower_case_flag)


def remove_quote_oracle(root_node: TreeNode):
    if in_oracle_column_table_name_path(root_node):
        rm_quote_to_bot_node(root_node, '"')
    else:
        for child in root_node.children:
            remove_quote_oracle(child)


def father_value_list_compare(root_node: TreeNode, father_list: List[str]) -> bool:
    now_father = root_node.father
    for father in father_list:
        if isinstance(father, list):
            if now_father is None or now_father.value not in father:
                return False
        else:
            if now_father is None or now_father.value != father:
                return False
        now_father = now_father.father
    return True


def add_quote_pg(root_node: TreeNode, quote_type, lower_case_flag: bool):
    if in_pg_column_table_name_path(root_node):
        add_quote_to_bot_node(root_node, quote_type, lower_case_flag)
    else:
        for child in root_node.children:
            add_quote_pg(child, quote_type, lower_case_flag)


def remove_quote_pg(root_node: TreeNode):
    if in_pg_column_table_name_path(root_node):
        rm_quote_to_bot_node(root_node, '"')
    else:
        for child in root_node.children:
            remove_quote_pg(child)


def rm_quote_to_bot_node(root_node: TreeNode, quote_type: str):
    while not root_node.is_terminal:
        root_node = root_node.children[0]
    root_node.value = rm_quote(root_node.value, quote_type)


def rm_quote(src_str: str, quote_type: str) -> str:
    if src_str.startswith(quote_type):
        assert len(src_str) > 0
        src_str = src_str[1:]
    if src_str.endswith(quote_type):
        src_str = src_str[:-1]
    return src_str


def add_quote(src_str: str, quote_type: str, lower_case_flag: bool) -> str:
    if src_str.startswith('\''):
        return src_str
    if quote_type == '\"':
        reverse_quote = '`'
    else:
        reverse_quote = '\"'
    res = ''
    if src_str.startswith(reverse_quote):
        assert len(src_str) > 0
        src_str = src_str[1:]
    if src_str.endswith(reverse_quote):
        src_str = src_str[:-1]

    if not src_str.startswith(quote_type):
        res = res + quote_type
    res = res + src_str
    if not src_str.endswith(quote_type):
        res = res + quote_type
    if not lower_case_flag:
        return res
    else:
        return res.lower()


def add_quote_to_bot_node(root_node: TreeNode, quote_type: str, lower_flag: bool):
    while not root_node.is_terminal:
        root_node = root_node.children[0]
    root_node.value = add_quote(root_node.value, quote_type, lower_flag)


def normalize(root_node: TreeNode, src_dialect: str, tgt_dialect: str):
    if tgt_dialect == 'oracle':
        if src_dialect == 'mysql':
            remove_as_mysql(root_node)
            add_quote_mysql(root_node, '\"', True)
        elif src_dialect == 'pg':
            remove_as_pg(root_node)
            add_quote_pg(root_node, '\"', True)
        elif src_dialect == 'oracle':
            add_quote_oracle(root_node, '\"', True)
    elif tgt_dialect == 'mysql':
        if src_dialect == 'oracle':
            add_quote_oracle(root_node, '`', True)
        elif src_dialect == 'pg':
            add_quote_pg(root_node, '`', True)
        elif src_dialect == 'mysql':
            add_quote_mysql(root_node, '`', True)
    elif tgt_dialect == 'pg':
        if src_dialect == 'oracle':
            add_quote_oracle(root_node, '\"', True)
        elif src_dialect == 'mysql':
            add_quote_mysql(root_node, '\"', True)
        elif src_dialect == 'pg':
            add_quote_pg(root_node, '\"', True)


def normalize_sql(sql: str, src_dialect: str, tgt_dialect: str):
    tree_node, _, _, _ = parse_tree(sql, src_dialect)
    if tree_node is not None:
        node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
        normalize(node, src_dialect, tgt_dialect)
        return node
    else:
        return None


def remove_sql_quote(sql, dialect):
    tree_node, _, _, _ = parse_tree(sql, dialect)
    if tree_node is not None:
        node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        if dialect == 'mysql':
            remove_quote_mysql(node)
        elif dialect == 'pg':
            remove_quote_pg(node)
        elif dialect == 'oracle':
            remove_quote_oracle(node)
        return str(node)
    else:
        return None


def remove_for_oracle(sql, dialect):
    tree_node, _, _, _ = parse_tree(sql, dialect)
    if tree_node is not None:
        node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        if dialect == 'mysql':
            remove_as_mysql(node)
            remove_quote_mysql(node)
        elif dialect == 'pg':
            remove_quote_pg(node)
            remove_as_pg(node)
        return str(node)
    else:
        return None


def normalize_specific_sql(sql, dialect):
    return str(normalize_sql(sql, dialect, dialect))


def rep_quote(src_str: str, reserved_tgt_dialect: List[str], quote_type: str) -> str:
    if src_str.startswith('\''):
        return src_str
    real_name = src_str.strip().strip('`').strip('"')
    flag = True
    for char in real_name:
        if not (char.isalnum() or char == '_'):
            flag = False
    if real_name.upper() in reserved_tgt_dialect or not flag:
        used_name = quote_type + real_name + quote_type
    else:
        used_name = real_name
    return used_name.lower()


def rep_quote_to_bot_node(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    while not root_node.is_terminal:
        root_node = root_node.children[0]
    root_node.value = rep_quote(root_node.value, reserved_tgt_dialect, quote_type)


def rep_quote_mysql(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_mysql_column_table_name_path(root_node):
        rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    elif father_value_list_compare(root_node, ['dottedId', 'fullColumnName']):
        if root_node.is_terminal and not root_node.value == '.':
            assert root_node.value[0] == '.'
            src_str = root_node.value[1:]
            res = rep_quote(src_str, reserved_tgt_dialect, quote_type)
            root_node.value = '.' + res
        elif root_node.is_terminal and root_node.value == '.':
            pass
        else:
            assert root_node.value == 'uid'
            rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rep_quote_mysql(child, reserved_tgt_dialect, quote_type)


def rep_quote_pg(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_pg_column_table_name_path(root_node):
        rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rep_quote_pg(child, reserved_tgt_dialect, quote_type)


def rep_quote_oracle(root_node: TreeNode, reserved_tgt_dialect: List[str], quote_type: str):
    if in_oracle_column_table_name_path(root_node):
        rep_quote_to_bot_node(root_node, reserved_tgt_dialect, quote_type)
    else:
        for child in root_node.children:
            rep_quote_oracle(child, reserved_tgt_dialect, quote_type)


def rep_reserved_keyword_quote(sql: str | None, tree_node: TreeNode | None, src_dialect, tgt_dialect):
    assert not (sql is None and tree_node is None)
    if tree_node is None:
        tree_node, _, _, _ = parse_tree(sql, src_dialect)
        if tree_node is None:
            raise ValueError("Parse tree is None.")
        tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)

    reserved_keywords = get_used_reserved_keyword_list()
    tgt_reserved_keyword_list = reserved_keywords[tgt_dialect]
    if tgt_dialect == 'oracle':
        quote_type = '"'
    elif tgt_dialect == 'mysql':
        quote_type = '`'
    elif tgt_dialect == 'pg':
        quote_type = '"'
    else:
        assert False
    if src_dialect == 'mysql':
        rep_quote_mysql(tree_node, tgt_reserved_keyword_list, quote_type)
    elif src_dialect == 'pg':
        rep_quote_pg(tree_node, tgt_reserved_keyword_list, quote_type)
    elif src_dialect == 'oracle':
        rep_quote_oracle(tree_node, tgt_reserved_keyword_list, quote_type)
    else:
        assert False
    return str(tree_node)
