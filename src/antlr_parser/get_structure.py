# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: get_structure$
# @Author: 10379
# @Time: 2025/1/16 22:18
from antlr_parser.Tree import TreeNode


def get_pg_select_primary(node: TreeNode):
    while node.value != 'select_no_parens' and node.value != 'select_with_parens':
        node = node.children[0]
    while True:
        while node.value == 'select_with_parens':
            tmp_node = node.get_child_by_value('select_no_parens')
            if tmp_node is None:
                node = node.get_child_by_value('select_with_parens')
                assert node is not None
            else:
                node = tmp_node
        assert isinstance(node, TreeNode)
        select_clause_node = node.get_child_by_value('select_clause')
        assert isinstance(select_clause_node, TreeNode)
        if len(select_clause_node.get_children_by_value('simple_select_intersect')) != 1:
            # TODO: deal with UNION
            return None
        else:
            simple_select_intersect_node = select_clause_node.get_child_by_value('simple_select_intersect')
            if len(simple_select_intersect_node.get_children_by_value('simple_select_pramary')) != 1:
                # TODO: deal with intersect
                return None
            else:
                simple_select_primary_node = simple_select_intersect_node.get_child_by_value(
                    'simple_select_pramary')
                assert isinstance(simple_select_primary_node, TreeNode)
                if simple_select_primary_node.get_child_by_value('select_with_parens') is not None:
                    node = simple_select_primary_node.get_child_by_value('select_with_parens')
                else:
                    node = simple_select_primary_node
                    break
    return node