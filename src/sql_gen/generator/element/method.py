# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: method$
# @Author: 10379
# @Time: 2025/5/10 12:24
from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_function_tree, parse_tree
from sql_gen.generator.element.Pattern import Pattern, Slot


def parse_pattern_tree(point_type, pattern: Pattern, dialect) -> TreeNode:
    extended_pattern, slot_map = pattern.extend_pattern()
    name_to_slot = {}
    for slot, name in slot_map.items():
        name_to_slot[name] = slot
    if point_type == 'PATTERN':
        tree_node, _, _, _ = parse_tree(extended_pattern, dialect)
        if tree_node is None:
            raise ValueError(f"Failed to parse the pattern {pattern}")
        tree_node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        rep_value_with_slot(tree_node, name_to_slot)
    else:
        tree_node, _, _, _ = parse_function_tree(extended_pattern, dialect)
        if tree_node is None:
            raise ValueError(f"Failed to parse the pattern {extended_pattern}")
        tree_node = TreeNode.make_g4_tree_by_node(tree_node, dialect)
        rep_value_with_slot(tree_node, name_to_slot)
    return tree_node


def rep_value_with_slot(root_node: TreeNode, slot_map: dict[str, Slot]):
    if root_node.value in slot_map:
        used_value = root_node.value
        while root_node.father is not None:
            father_node = root_node.father
            assert isinstance(root_node, TreeNode)
            if len(father_node.children) == 1:
                root_node = father_node
            else:
                break
        assert root_node.father is not None
        root_node.slot = slot_map[used_value]
        root_node.is_terminal = True
    else:
        for child in root_node.children:
            rep_value_with_slot(child, slot_map)


