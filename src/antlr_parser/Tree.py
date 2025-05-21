import sys

from antlr4.tree.Tree import TerminalNodeImpl

from antlr_parser.parse_tree import get_parser, parse_tree
from utils.tools import self_split, remove_all_space

parser_map = {}


class TreeNode:
    def __init__(self, value: str, dialect: str, is_terminal: bool, father=None,
                 father_child_index=None, children=None, model_get=False):
        """
        初始化多叉树节点
        :param value:该节点对应的ANTLR文法中的表示
        :param father_child_index: 在父节点中的child数组中的index
        :param father: 父节点
        :param children: 子节点列表，默认为空列表
        """
        self.value = value.strip()
        self.children = children if children is not None else []
        self.child_link = {}
        self.father = father
        self.father_child_index = father_child_index
        self.link = None
        self.father_link = {}
        self.dialect = dialect
        self.is_terminal = is_terminal
        self.model_get = model_get
        self.slot = None
        self.slot_times = None
        self.for_slot_ancestor_id = None
        self.for_slot_ancestor = None
        self.for_loop_slot = []
        self.for_loop_sub_trees = []

    def to_tree_rep(self):
        if len(self.children) != 0:
            res = '(' + self.get_value_rep()
            for child in self.children:
                res = res + " " + str(child.to_tree_rep())
            res = res + ")"
        else:
            return self.get_value_rep()
        return res

    def get_value_rep(self):
        if self.slot is not None:
            return f"{self.value}({str(self.slot)})"
        return self.value

    def __str__(self):
        if self.get_value_rep() == '<EOF>':
            return ''
        res = ''
        flag = False
        flag_paren = True
        if self.is_terminal:
            res = self.get_value_rep()
            return res
        if self.dialect == 'mysql':
            if self.get_value_rep() in ['comparisonOperator', 'logicalOperator', 'bitOperator', 'multOperator',
                                        'jsonOperator']:
                for child in self.children:
                    sub_str = str(child)
                    res = res + sub_str.strip()
                return res
            if (self.get_value_rep() == 'functionCall' and len(self.children) != 0
                    and (self.children[0].get_value_rep() == 'scalarFunctionName' or self.children[
                        0].get_value_rep() == 'fullId')):
                flag_paren = False
            elif (self.get_value_rep() == 'specificFunction' or self.get_value_rep() == 'passwordFunctionClause' or
                  self.get_value_rep() == 'aggregateWindowedFunction' or self.get_value_rep() == 'nonAggregateWindowedFunction'
                  or self.get_value_rep() == 'dataType'):
                flag_paren = False
            if not flag_paren:
                for child in self.children:
                    if child.is_terminal and child.get_value_rep() == '(':
                        res = res + child.get_value_rep()
                        flag = True
                    else:
                        sub_str = str(child)
                        if sub_str.startswith('.') or sub_str.startswith('(') or res.endswith('.'):
                            flag = False
                        if sub_str != '':
                            if flag:
                                res = res + " " + sub_str.strip()
                            else:
                                res = res + sub_str
                                flag = True
                return res
        elif self.dialect == 'pg':
            if ((self.get_value_rep() == 'func_application' or self.get_value_rep() == 'func_expr_common_subexpr')
                    and len(self.children) != 0):
                flag_paren = False
            if self.father is not None and self.father.get_value_rep() == 'simpletypename':
                flag_paren = False
            if not flag_paren:
                for child in self.children:
                    if child.is_terminal and child.get_value_rep() == '(':
                        res = res + child.get_value_rep()
                        flag = True
                    else:
                        sub_str = str(child)
                        if sub_str.startswith('.') or sub_str.startswith('(') or res.endswith('.'):
                            flag = False
                        if sub_str != '':
                            if flag:
                                res = res + " " + sub_str.strip()
                            else:
                                res = res + sub_str
                                flag = True
                return res
        elif self.dialect == 'oracle':
            if self.get_value_rep() in ['relational_operator']:
                for child in self.children:
                    sub_str = str(child)
                    res = res + sub_str.strip()
                return res
            if (self.get_value_rep() == 'string_function' or self.get_value_rep() == 'json_function'
                    or self.get_value_rep() == 'other_function' or self.get_value_rep() == 'numeric_function' or self.get_value_rep() == 'datatype'):
                flag_paren = False
            if not flag_paren:
                for child in self.children:
                    if child.is_terminal and child.get_value_rep() == '(':
                        res = res + child.get_value_rep()
                        flag = True
                    else:
                        sub_str = str(child)
                        if sub_str.startswith('.') or sub_str.startswith('(') or res.endswith('.'):
                            flag = False
                        if sub_str != '':
                            if flag:
                                res = res + " " + sub_str.strip()
                            else:
                                res = res + sub_str
                                flag = True
                return res
            for child in self.children:
                sub_str = str(child)
                if sub_str.startswith('.'):
                    flag = False
                if sub_str != '':
                    if (flag and not res.endswith('.')
                            and not child.get_value_rep() == 'function_argument'
                            and not child.get_value_rep() == 'function_argument_analytic'
                            and not child.get_value_rep() == 'function_argument_modeling'):
                        res = res + " " + sub_str.strip()
                    else:
                        res = res + sub_str
                        flag = True
            return res
        for child in self.children:
            sub_str = str(child)
            if sub_str.startswith('.'):
                flag = False
            if sub_str != '':
                if flag and not res.endswith('.'):
                    res = res + " " + sub_str.strip()
                else:
                    res = res + sub_str
                    flag = True
        return res

    def __repr__(self):
        return self.__str__()

    def add_child(self, node):
        self.children.append(node)
        node.father = self

    def replace_child(self, ori_child, new_child):
        i = 0
        while i < len(self.children):
            if self.children[i] == ori_child:
                self.children[i] = new_child
                new_child.father = self
                # new_child.father_child_index = ori_child.father_child_index
                return
            i = i + 1
        assert False

    @staticmethod
    def make_g4_tree_by_node(antlr_node, dialect: str):
        if dialect in parser_map:
            parser = parser_map[dialect]
        else:
            parser = get_parser(dialect)
            parser_map[dialect] = parser
        if isinstance(antlr_node, TerminalNodeImpl):
            if antlr_node.getText() == '<EOF>':
                return None
            return TreeNode(antlr_node.getText(), dialect, True)
        else:
            if antlr_node.children is not None:
                node = TreeNode(parser.ruleNames[antlr_node.getRuleIndex()], dialect, False)
                for child in antlr_node.children:
                    child_node = TreeNode.make_g4_tree_by_node(child, dialect)
                    if child_node is not None:
                        node.add_child(child_node)
            else:
                return None
        return node

    @staticmethod
    def make_g4_tree(str_tree: str, dialect: str):
        # assume 括号成对出现
        node_stack = []
        used_words = self_split(str_tree)
        i = 0
        while i < len(used_words):
            if used_words[i][0] == '(':
                if len(used_words[i]) == 1:
                    cur_node = TreeNode(used_words[i], dialect, True, node_stack[len(node_stack) - 1],
                                        len(node_stack[len(node_stack) - 1].children))
                    node_stack[len(node_stack) - 1].add_child(cur_node)
                else:
                    node_name = used_words[i][1:]
                    if len(node_stack) == 0:
                        res = TreeNode(node_name, dialect, False)
                        node_stack.append(res)
                    else:
                        cur_node = TreeNode(node_name, dialect, False, node_stack[len(node_stack) - 1],
                                            len(node_stack[len(node_stack) - 1].children))
                        node_stack[len(node_stack) - 1].add_child(cur_node)
                        node_stack.append(cur_node)
            elif used_words[i][len(used_words[i]) - 1] == ')':
                if used_words[i][0] == ')':
                    cur_node = TreeNode(used_words[i][0], dialect, True, node_stack[len(node_stack) - 1],
                                        len(node_stack[len(node_stack) - 1].children))
                    node_stack[len(node_stack) - 1].add_child(cur_node)
                    cnt = len(used_words[i]) - 1
                else:
                    cnt = 0
                    final_index = len(used_words[i]) - 1
                    while used_words[i][final_index] == ')' and final_index > -1:
                        final_index = final_index - 1
                        cnt = cnt + 1
                    if final_index != -1:
                        final_index = final_index + 1
                        node_name = used_words[i][0: final_index]
                        cur_node = TreeNode(node_name, dialect, True, node_stack[len(node_stack) - 1],
                                            len(node_stack[len(node_stack) - 1].children))
                        node_stack[len(node_stack) - 1].add_child(cur_node)
                for j in range(cnt):
                    node_stack.pop()
            else:
                cur_node = TreeNode(used_words[i], dialect, True, node_stack[len(node_stack) - 1],
                                    len(node_stack[len(node_stack) - 1].children))
                node_stack[len(node_stack) - 1].add_child(cur_node)
            i = i + 1
        if len(node_stack) != 0:
            print("error when parse to antlr Tree", file=sys.stderr)
        TreeNode.clean_node(res, res.dialect)
        return res

    @staticmethod
    def clean_node(root_node, dialect: str):
        for child in root_node.children:
            TreeNode.clean_node(child, dialect)
        i = len(root_node.children) - 1
        while i >= 0:
            if len(root_node.children[i].children) == 0 and not root_node.children[i].is_terminal:
                root_node.children.pop(i)
            i = i - 1

    @staticmethod
    def locate_node_exec(root_node, column: int, ori_sql: str, now_str: str):
        cur_str = now_str
        if len(root_node.children) != 0:
            for child in root_node.children:
                node, cur_str = TreeNode.locate_node_exec(child, column, ori_sql, cur_str)
                if node is not None:
                    return node, cur_str
            return None, cur_str
        elif not root_node.is_terminal:
            return None, now_str
        else:
            i = len(now_str)
            while ori_sql[i] == ' ':
                i = i + 1
            j, k = 0, 0
            while j < len(root_node.get_value_rep()):
                while (root_node.get_value_rep()[j] == ' '
                       or root_node.get_value_rep()[j] == '\n' or root_node.get_value_rep()[j] == '\t'):
                    j = j + 1
                while (ori_sql[i + k] == ' '
                       or ori_sql[i + k] == '\n' or ori_sql[i + k] == '\t'):
                    k = k + 1
                assert root_node.get_value_rep()[j] == ori_sql[i + k]
                j = j + 1
                k = k + 1
            if i <= column < i + j:
                return root_node, ori_sql[:i + j]
            else:
                return None, ori_sql[:i + j]

    def get_children_by_value(self, value: str):
        res = []
        for child in self.children:
            if child.get_value_rep() == value:
                res.append(child)
        return res

    def get_child_by_value(self, value: str):
        temp = self.get_children_by_value(value)
        if len(temp) > 1:
            raise ValueError("Has more than one children")
        elif len(temp) == 0:
            return None
        return temp[0]

    @staticmethod
    def locate_node(root_node, column: int, ori_sql: str):
        # print("column", column)
        # print("ori_sql", ori_sql)
        node_str = str(root_node)
        node_res = ''
        for split_piece in node_str.split():
            node_res = node_res + split_piece
        ori_res = ''
        for split_piece in ori_sql.split():
            ori_res = ori_res + split_piece
        assert ori_res == node_res
        return TreeNode.locate_node_exec(root_node, column, ori_sql, '')

    def get_children_by_path(self, path: list):
        if len(path) == 0:
            return [self]
        else:
            res = []
            for child in self.children:
                if child.get_value_rep() == path[0]:
                    res = res + child.get_children_by_path(path[1:])
            return res

    def clone(self):
        new_node = TreeNode(self.value, self.dialect, self.is_terminal)
        new_node.model_get = self.model_get
        for child in self.children:
            new_node.add_child(child.clone())
        return new_node

    def rm_child(self, node):
        for child in self.children:
            if child == node:
                self.children.remove(child)
                break

    def rm_child_by_value(self, value: str):
        for child in self.children:
            if child.get_value_rep() == value:
                self.children.remove(child)
                break

    def insert_after_node(self, insert_node, after_node_value: str, times: int = 1):
        for child in self.children:
            if child.get_value_rep() == after_node_value:
                times = times - 1
                if times > 0:
                    continue
                child_index = self.children.index(child)
                self.children.insert(child_index + 1, insert_node)
                insert_node.father = self
                return
        raise ValueError("Cannot find the after node")

    def find_all_nodes_of_values(self, values: list):
        res = []
        for child in self.children:
            if child.get_value_rep() in values:
                res.append(child)
        return res

    def get_node_until(self, value: str):
        if self.get_value_rep() == value:
            return [self]
        res = []
        for child in self.children:
            res = res + child.get_node_until(value)
        return res


def try_fetch_nodes_by_route(root_node: TreeNode, path: list):
    if root_node.get_value_rep() == path[0]:
        if len(path) == 1:
            return [root_node]
        else:
            res = []
            for child in root_node.children:
                res = res + try_fetch_nodes_by_route(child, path[1:])
            return res
    else:
        return []
