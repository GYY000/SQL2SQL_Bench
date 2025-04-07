# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: pg_tree$
# @Author: 10379
# @Time: 2025/4/2 20:35
from antlr_parser.Tree import TreeNode


# used for analyze pg tree structure

def get_pg_main_select_node_from_select_stmt(select_stmt_node: TreeNode):
    select_root_node = select_stmt_node
    while select_root_node.get_child_by_value('select_no_parens') is None:
        assert select_root_node.get_child_by_value('select_with_parens') is not None
        select_root_node = select_root_node.get_child_by_value('select_with_parens')
    assert select_root_node.get_child_by_value('select_no_parens') is not None
    select_root_node = select_root_node.get_child_by_value('select_no_parens')
    return select_root_node


def fetch_main_select_from_select_stmt_pg(select_stmt_node: TreeNode):
    "can be both selectstmt node or select_with_parens or selct_no_parens"
    while True:
        if select_stmt_node.value != 'select_no_parens':
            while select_stmt_node.get_child_by_value('select_no_parens') is None:
                assert select_stmt_node.get_child_by_value('select_with_parens') is not None
                select_stmt_node = select_stmt_node.get_child_by_value('select_with_parens')
            assert select_stmt_node.get_child_by_value('select_no_parens') is not None
            select_stmt_node = select_stmt_node.get_child_by_value('select_no_parens')
        select_clause_node = select_stmt_node.get_child_by_value('select_clause')
        assert isinstance(select_clause_node, TreeNode)
        simple_select_intersect_node = select_clause_node.get_child_by_value('simple_select_intersect')
        simple_select_primary_node = simple_select_intersect_node.get_child_by_value('simple_select_pramary')
        assert isinstance(simple_select_primary_node, TreeNode)
        if simple_select_primary_node.get_child_by_value('select_with_parens') is not None:
            select_stmt_node = simple_select_primary_node.get_child_by_value('select_with_parens')
        else:
            if simple_select_primary_node.get_child_by_value('values_clause') is not None:
                print('Value Clause haven\'t been implemented')
                assert False
            elif simple_select_primary_node.get_child_by_value('TABLE') is not None:
                print('Table Clause haven\'t been implemented')
                assert False
            node = simple_select_primary_node
            break
    return node


def fetch_all_simple_select_from_select_stmt_pg(select_stmt_node: TreeNode):
    if select_stmt_node.value != 'select_no_parens':
        while select_stmt_node.get_child_by_value('select_no_parens') is None:
            assert select_stmt_node.get_child_by_value('select_with_parens') is not None
            select_stmt_node = select_stmt_node.get_child_by_value('select_with_parens')
            assert select_stmt_node.get_child_by_value('select_no_parens') is not None
            select_stmt_node = select_stmt_node.get_child_by_value('select_no_parens')
    select_clause_node = select_stmt_node.get_child_by_value('select_clause')
    return dfs_select_clause(select_clause_node)


def dfs_select_clause(select_clause_node: TreeNode) -> list:
    res = []
    for child in select_clause_node.get_children_by_value('simple_select_intersect'):
        res = res + dfs_simple_select_intersect(child)
    return res


def dfs_simple_select_intersect(simple_select_intersect_node: TreeNode) -> list:
    res = []
    for child in simple_select_intersect_node.get_children_by_value('simple_select_pramary'):
        res = res + dfs_simple_select_primary(child)
    return res


def dfs_simple_select_primary(simple_select_primary_node: TreeNode) -> list:
    if simple_select_primary_node.get_child_by_value('select_with_parens') is not None:
        node = simple_select_primary_node
        while node.get_child_by_value('select_no_parens') is None:
            assert node.get_child_by_value('select_with_parens') is not None
            node = node.get_child_by_value('select_with_parens')
        select_clause_node = node.get_children_by_path(['select_no_parens', 'select_clause'])
        assert len(select_clause_node) == 1
        select_clause_node = select_clause_node[0]
        return dfs_select_clause(select_clause_node)
    else:
        if simple_select_primary_node.get_child_by_value('values_clause') is not None:
            print('Value Clause haven\'t been implemented')
            assert False
        elif simple_select_primary_node.get_child_by_value('TABLE') is not None:
            print('Table Clause haven\'t been implemented')
            assert False
        return [simple_select_primary_node]


def only_column_ref_pg(a_expr_node: TreeNode):
    while a_expr_node.value != 'column_ref':
        if len(a_expr_node.children) != 1:
            return False
        a_expr_node = a_expr_node.children[0]
    return True


def rename_column_pg(target_el_node: TreeNode, name_dict: dict, extend_name=None):
    if extend_name is None:
        extend_name = 'col'
    idx = name_dict.get(extend_name, 1)
    name_dict[extend_name] = idx + 1
    if target_el_node.get_child_by_value('collable') is not None:
        uid_node = target_el_node.get_child_by_value('collable')
        new_name_node = TreeNode(f"\"{extend_name}_{idx}\"", 'mysql', True)
        assert isinstance(uid_node, TreeNode)
        uid_node.children = [new_name_node]
    elif target_el_node.get_child_by_value('identifier') is not None:
        uid_node = target_el_node.get_child_by_value('identifier')
        new_name_node = TreeNode(f"\"{extend_name}_{idx}\"", 'mysql', True)
        assert isinstance(uid_node, TreeNode)
        uid_node.children = [new_name_node]
    else:
        target_el_node.add_child(TreeNode('AS', 'mysql', True))
        target_el_node.add_child(TreeNode(f"\"{extend_name}_{idx}\"", 'mysql', True))
    return f"{extend_name}_{idx}"


def analyze_pg_table_refs(table_refs: list):
    res = []
    name_dict = {}
    for table_ref in table_refs:
        res = res + analyze_table_ref(table_ref, name_dict)
    return res


def analyze_table_ref(table_ref: TreeNode, name_dict: dict):
    """
    {
            "type": "subquery",
            "name": table_name,
            "column_names": column_names,
            "sub_query_node": table_source_item_node.get_child_by_value('selectStatement'),
            "lateral": lateral_flag,  # if it's a lateral subquery
            "rename_flag": rename_flag
    }
    {
            "type": "table",
            "name": table_name,
            'alias': final_name,
            "column_names": column_names
    }
    table_ref
    : (
        relation_expr opt_alias_clause tablesample_clause?
        | func_table func_alias_clause
        | xmltable opt_alias_clause
        | select_with_parens opt_alias_clause
        | LATERAL_P (
            xmltable opt_alias_clause
            | func_table func_alias_clause
            | select_with_parens opt_alias_clause
        )
        | OPEN_PAREN table_ref (
            CROSS JOIN table_ref
            | NATURAL join_type? JOIN table_ref
            | join_type? JOIN table_ref join_qual
        )? CLOSE_PAREN opt_alias_clause
    ) (
        CROSS JOIN table_ref
        | NATURAL join_type? JOIN table_ref
        | join_type? JOIN table_ref join_qual
    )*
    ;
    """
    addition_table_refs = []
    final_column_names = None
    table_name = None
    if table_ref.get_child_by_value('opt_alias_clause') is not None:
        namelist_node = (table_ref.get_child_by_value('opt_alias_clause').
                         get_child_by_value('table_alias_clause').get_child_by_value('namelist'))
        table_name = (table_ref.get_child_by_value('opt_alias_clause').
                      get_child_by_value('table_alias_clause').get_child_by_value('table_alias'))
        table_name = str(table_name).strip('"')
        if namelist_node is not None:
            final_column_names = [str(child).strip('"') for child
                                  in namelist_node.get_children_by_value('name')]
    if table_ref.get_child_by_value('(') is not None:
        flag = False
        for child in table_ref.children:
            if child.value == ')':
                flag = True
            if flag and child.value == 'table_ref':
                addition_table_refs.append(child)
    else:
        addition_table_refs = table_ref.get_children_by_value('table_ref')
    if table_ref.get_child_by_value('LATERAL') is not None:
        lateral_flag = True
    else:
        lateral_flag = False
    if table_ref.get_child_by_value('(') is not None:
        table_refs = []
        newly_build_node = TreeNode('temporary', 'pg', False)
        for child in table_ref.children:
            if child.value == ')':
                newly_build_node.add_child(child)
                break
            newly_build_node.add_child(child)
            if child.value == 'table_ref':
                table_refs.append(child)
        if table_name is None:
            table_name = rename_table(table_ref, table_ref.get_child_by_value(')'), name_dict)
        if final_column_names is None:
            final_column_names = []
            sub_ref_name_dict = {}
            for table_ref in table_refs:
                for ele in analyze_table_ref(table_ref, sub_ref_name_dict):
                    final_column_names = final_column_names + ele['column_names']
        res = [{
            "type": "subquery",
            "name": table_name,
            "column_names": final_column_names,
            "sub_query_node": newly_build_node,
            "lateral": lateral_flag,  # if it's a lateral subquery
            "rename_flag": False
        }]
    elif table_ref.get_child_by_value('relation_expr') is not None:
        table_name_node = (table_ref.get_child_by_value('relation_expr').
                           get_child_by_value('qualified_name'))
        if table_name_node.get_child_by_value('indirection') is None:
            ori_table_name = table_name_node.get_child_by_value('colid')
            assert ori_table_name is not None
            ori_table_name = str(ori_table_name).strip('"')
        else:
            indirection_node = table_name_node.get_child_by_value('indirection')
            indirection_els = indirection_node.get_children_by_value('indirection_el')
            used_name = indirection_els[-1]
            ori_table_name = str(used_name).strip('"')
        if table_name is None:
            table_name = ori_table_name
        if table_name in name_dict:
            table_name = rename_table(table_ref, table_ref.get_child_by_value('relation_expr'), name_dict)
        else:
            name_dict[table_name] = 1
        if table_name is not None:
            pass
        return [
            {
                "type": "table",
                "name": ori_table_name,
                'alias': table_name,
                "column_names": None
            }
        ]
    elif table_ref.get_child_by_value('xmltable') is not None:
        if table_name is None:
            table_name = rename_table(table_ref, table_ref.get_child_by_value('xmltable'),
                                      name_dict, 'xml_table')
        xmltable_column_list_node = (table_ref.get_child_by_value('xmltable').
                                     get_child_by_value('xmltable_column_list'))
        assert xmltable_column_list_node is not None
        xmltable_column_els = (xmltable_column_list_node.
                               get_children_by_value('xmltable_column_el'))
        if final_column_names is None:
            final_column_names = []
            for xml_table_column_el in xmltable_column_els:
                column_name = xml_table_column_el.get_child_by_value('colid')
                assert column_name is not None
                final_column_names.append(str(column_name).strip('"'))
        res = [{
            "type": "subquery",
            "name": table_name,
            "column_names": final_column_names,
            "sub_query_node": table_ref.get_child_by_value('xmltable'),
            "lateral": lateral_flag,  # if it's a lateral subquery
            "rename_flag": False
        }]
    elif table_ref.get_child_by_value('select_with_parens') is not None:
        if table_name is None:
            table_name = rename_table(table_ref, table_ref.get_child_by_value('select_with_parens'), name_dict)
        if final_column_names is None:
            final_column_names = rename_sql_pg(table_ref.get_child_by_value('select_with_parens'))
        res = [{
            "type": "subquery",
            "name": table_name,
            "column_names": final_column_names,
            "sub_query_node": table_ref.get_child_by_value('select_with_parens'),
            "lateral": lateral_flag,  # if it's a lateral subquery
            "rename_flag": False
        }]
    else:
        print("Haven't support functable yet")
        assert False
    for addi_table_ref in addition_table_refs:
        res = res + analyze_table_ref(addi_table_ref, {})
    return res


def rename_table(father_node: TreeNode, son_node: TreeNode | None, name_dict, value: str = None):
    if value is None:
        value = 'table'
    if value in name_dict:
        value = value + '_' + str(name_dict[value])
        name_dict[value] += 1
    else:
        name_dict[value] = 1
    if father_node.get_child_by_value('opt_alias_clause') is None:
        assert son_node is not None
        table_alias_node = TreeNode('table_alias_clause', 'pg', False)
        table_alias_node.add_child(TreeNode('AS', 'pg', True))
        table_alias_node.add_child(TreeNode(f"\"{value}\"", 'pg', True))
        opt_alias_node = TreeNode('opt_alias_clause', 'pg', False)
        opt_alias_node.add_child(table_alias_node)
        father_node.insert_after_node(opt_alias_node, son_node.value)
        return value
    else:
        table_name_node = (father_node.get_child_by_value('opt_alias_clause').
                           get_child_by_value('table_alias_clause').get_child_by_value('table_alias'))
        table_name_node.children = [TreeNode(f"\"{value}\"", 'pg', True)]
        return value



def rename_sql_pg(select_stmt_node: TreeNode):
    name_dict = {}
    res = []
    select_main_node = fetch_main_select_from_select_stmt_pg(select_stmt_node)
    target_list_node = select_main_node.get_child_by_value('target_list')
    if target_list_node is not None:
        target_list_node = select_main_node.get_children_by_path(['opt_target_list', 'target_list'])
        assert target_list_node is not None
    select_elements = target_list_node.get_children_by_value('target_el')
    for i in range(len(select_elements)):
        if select_elements[i].get_child_by_value('*') is not None:
            print('* is not Support yet')
            assert False
        a_expr_node = select_elements[i].get_child_by_value('a_expr')
        assert a_expr_node is not None
        if select_elements[i].get_child_by_value('AS') is not None:
            col_name = select_elements[i].get_child_by_value('collabel')
            if col_name is None:
                col_name = select_elements[i].get_child_by_value('identifier')
            col_name = str(col_name).strip('"')
            if col_name in dict:
                col_name = rename_column_pg(select_elements[i], name_dict, col_name)
            else:
                name_dict[col_name] = 1
        else:
            if not only_column_ref_pg(a_expr_node):
                col_name = rename_column_pg(select_elements[i], name_dict)
            else:
                column_ref_node = a_expr_node.get_node_until('columnref')
                if column_ref_node.get_child_by_value('indirection') is None:
                    col_name = column_ref_node.get_child_by_value('colid')
                    assert col_name is not None
                    col_name = str(col_name).strip('"')
                    if col_name in dict:
                        col_name = rename_column_pg(select_elements[i], name_dict, col_name)
                    else:
                        name_dict[col_name] = 1
                else:
                    indirection_node = column_ref_node.get_child_by_value('indirection')
                    indirection_els = indirection_node.get_children_by_value('indirection_el')
                    used_name = indirection_els[-1]
                    if used_name.get_child_by_value('*') is not None:
                        print('* is not Support yet')
                        assert False
                    else:
                        col_name = used_name.get_child_by_value('attr_name')
                        if col_name is None:
                            # array
                            col_name = rename_column_pg(select_elements[i], name_dict, extend_name='array')
                        else:
                            col_name = str(col_name).strip('"')
                            if col_name in dict:
                                col_name = rename_column_pg(select_elements[i], name_dict, col_name)
                            else:
                                name_dict[col_name] = 1
        res.append(col_name)
    return res

def parse_pg_group_by(group_list_node: TreeNode) -> list:
    group_by_item_nodes = group_list_node.get_children_by_value('group_by_list')
    if len(group_by_item_nodes) > 1:
        # No Node has cube or grouping sets or roll up
        res = []
        for children in group_by_item_nodes:
            res.append(str(children))
        return res
    elif len(group_by_item_nodes) == 1:
        group_by_item = group_by_item_nodes[0]
        assert isinstance(group_by_item, TreeNode)
        if group_by_item.get_child_by_value('empty_grouping_set') is not None:
            return []
        elif group_by_item.get_child_by_value('a_expr') is not None:
            return [str(group_by_item.get_child_by_value('a_expr'))]
        elif group_by_item.get_child_by_value('cube_clause') is not None:
            cube_clause_node = group_by_item.get_child_by_value('cube_clause')
            expr_list_node = cube_clause_node.get_child_by_value('expr_list')
            res = []
            for expr in expr_list_node.get_children_by_value('a_expr'):
                res.append(str(expr))
            return res
        elif group_by_item.get_child_by_value('rollup_clause') is not None:
            rollup_clause_node = group_by_item.get_child_by_value('rollup_clause')
            expr_list_node = rollup_clause_node.get_child_by_value('expr_list')
            res = []
            for expr in expr_list_node.get_children_by_value('a_expr'):
                res.append(str(expr))
            return res
        elif group_by_item.get_child_by_value('grouping_sets_clause') is not None:
            sets_clause_node = group_by_item.get_child_by_value('grouping_sets_clause')
            group_by_list_node = sets_clause_node.get_child_by_value('group_by_list')
            res = []
            for item_node in group_by_list_node.get_children_by_value('group_by_item'):
                assert item_node.get_child_by_value('group_expr_list') is not None
                expr_list = group_by_item.get_child_by_value('group_expr_list').get_child_by_value('expr_list')
                for a_expr in expr_list.get_child_by_value('a_expr'):
                    res.append(a_expr)
            return res
        else:
            assert False
    else:
        assert False
