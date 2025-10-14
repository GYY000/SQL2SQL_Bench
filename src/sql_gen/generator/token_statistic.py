# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: token_statistic$
# @Author: 10379
# @Time: 2025/7/14 13:21
import json
import os.path

from tqdm import tqdm

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from utils.tools import get_data_path, get_proj_root_path


def is_string_node(tree_node: TreeNode):
    return (tree_node.terminal_node_name == 'START_NATIONAL_STRING_LITERAL' or
            tree_node.terminal_node_name == 'STRING_LITERAL' or tree_node.value == 'anysconst'
            or tree_node.terminal_node_name == 'CHAR_STRING' or
            tree_node.terminal_node_name == 'NATIONAL_CHAR_STRING_LIT')


def stat_tokens(tree_node: TreeNode):
    stat_cnt = 0
    if is_string_node(tree_node):
        return len(str(tree_node).split())
    else:
        if tree_node.is_terminal:
            return 1
        else:
            for child in tree_node.children:
                stat_cnt += stat_tokens(child)
    return stat_cnt


def stat_begin_node_end_node(cur_node: TreeNode, begin_node: TreeNode,
                             end_node: TreeNode, flag: bool):
    if cur_node == end_node:
        cnt = stat_tokens(cur_node)
        return cnt, False
    elif cur_node == begin_node:
        cnt = stat_tokens(cur_node)
        return cnt, True
    else:
        if is_string_node(cur_node):
            if flag:
                return len(str(cur_node).split()), flag
            else:
                return 0, flag
        elif cur_node.is_terminal:
            if flag:
                return len(str(cur_node).split()), flag
            else:
                return 0, flag
        else:
            cnt = 0
            for child in cur_node.children:
                cnt1, flag = stat_begin_node_end_node(child, begin_node, end_node, flag)
                cnt += cnt1
            return cnt, flag


def stat_sqls(db_id):
    db_id_path = os.path.join(get_proj_root_path(), 'SQL', db_id)
    dialect = ['mysql', 'oracle', 'pg']
    for src_dialect in dialect:
        for tgt_dialect in dialect:
            if src_dialect == tgt_dialect:
                continue
            with open(os.path.join(db_id_path, 'no_points', f'{src_dialect}_{tgt_dialect}.json'), 'r') as file:
                data = json.load(file)
            for sql in tqdm(data):
                if 'tokens' in sql:
                    continue
                src_sql = sql[src_dialect]
                tree_node, _, _, _ = parse_tree(src_sql, src_dialect)
                if tree_node is None:
                    print(sql)
                    tokens = None
                else:
                    tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
                    tokens = stat_tokens(tree_node)
                sql['tokens'] = tokens
            with open(os.path.join(db_id_path, 'no_points', f'{src_dialect}_{tgt_dialect}.json'), 'w') as file:
                json.dump(data, file, indent=4)
