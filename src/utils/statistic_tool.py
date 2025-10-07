from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from sql_gen.utils import QueryPair
from sql_preprocess.try_rewrite_sql import tgt_dialect


def fetch_all_nodes_of_name(tree_node: TreeNode, name: str) -> list[TreeNode]:
    res = []
    if tree_node.value == name:
        res.append(tree_node)
    for child in tree_node.children:
        res.extend(fetch_all_nodes_of_name(child, name))
    return res


def stat_pg_table_ref(table_ref_node: TreeNode):
    relation_expr_node = table_ref_node.get_children_by_value('relation_expr')
    func_table_node = table_ref_node.get_children_by_value('func_table')
    xmltable_node = table_ref_node.get_children_by_value('xmltable')
    select_with_parens_node = table_ref_node.get_children_by_value('select_with_parens')
    table_ref_nodes = table_ref_node.get_children_by_value('table_ref')
    res = len(relation_expr_node) + len(func_table_node) + len(xmltable_node) + len(select_with_parens_node)
    for table_ref_node in table_ref_nodes:
        res += stat_pg_table_ref(table_ref_node)
    return res


def stat_oracle_table_ref(table_ref_node: TreeNode):
    table_ref_aux = table_ref_node.get_children_by_value('table_ref_aux')
    join_clause = table_ref_node.get_children_by_value('join_clause')
    res = 0
    for table_ref_aux_node in table_ref_aux:
        res += stat_oracle_table_ref_aux(table_ref_aux_node)
    for join_clause_node in join_clause:
        table_ref_aux_nodes = join_clause_node.get_children_by_value('table_ref_aux')
        for table_ref_aux_node in table_ref_aux_nodes:
            res += stat_oracle_table_ref_aux(table_ref_aux_node)
    return res


def stat_oracle_table_ref_aux(table_ref_aux_node: TreeNode):
    table_ref_aux_internal_nodes = table_ref_aux_node.get_children_by_value('table_ref_aux_internal')
    res = 0
    for table_ref_aux_internal_node in table_ref_aux_internal_nodes:
        res += stat_oracle_table_ref_aux_internal(table_ref_aux_internal_node)
    return res


def stat_oracle_table_ref_aux_internal(table_ref_aux_internal_node: TreeNode):
    dml_table_expression_clause_nodes = table_ref_aux_internal_node.get_children_by_value(
        'dml_table_expression_clause')
    table_ref_nodes = table_ref_aux_internal_node.get_children_by_value('table_ref')
    res = len(dml_table_expression_clause_nodes)
    for table_ref_node in table_ref_nodes:
        res += stat_oracle_table_ref(table_ref_node)
    return res


def stat_joins(sql, dialect: str):
    if isinstance(sql, str):
        tree_node, _, _, _ = parse_tree(sql, dialect)
        if tree_node is None:
            return None
        else:
            tree_node = TreeNode.make_g4_tree_by_node(tree_node)
    else:
        tree_node = sql
    if dialect == "mysql":
        fromClause_nodes = fetch_all_nodes_of_name(tree_node, "fromClause")
        res = 0
        for node in fromClause_nodes:
            tableSources_node = node.get_child_by_value('tableSources')
            assert isinstance(tableSources_node, TreeNode)
            tableSource_nodes = tableSources_node.get_children_by_value('tableSource')
            for tableSource in tableSource_nodes:
                table_source_item_node = tableSource.get_child_by_value('tableSourceItem')
                join_nodes = tableSource.get_children_by_value('joinPart')
                res += (len(join_nodes) + 1) if table_source_item_node is not None else len(join_nodes)
        return res
    elif dialect == "oracle":
        join_nodes = fetch_all_nodes_of_name(tree_node, "table_ref_list")
        res = 0
        for node in join_nodes:
            tableSources_node = node.get_children_by_value('table_ref')
            for tableSource in tableSources_node:
                res += stat_oracle_table_ref(tableSource)
        return res
    elif dialect == "pg":
        from_list_nodes = fetch_all_nodes_of_name(tree_node, "from_list")
        res = 0
        for node in from_list_nodes:
            table_ref_nodes = node.get_children_by_value('table_ref')
            for table_ref_node in table_ref_nodes:
                res += stat_pg_table_ref(table_ref_node)
        return res
    else:
        assert False


def point_list_equal(points1, points2):
    if len(points1) != len(points2):
        return False
    for point1 in points1:
        flag = False
        for point2 in points2:
            if point1['point'] == point2['point'] and point1['num'] == point2['num']:
                flag = True
                break
        if not flag:
            return False
    return True


def stat_point_comb(query_pairs: list[QueryPair]):
    unique_pointlist_list = {}
    dialects = ['mysql', 'oracle', 'pg']
    for src_dialect in dialects:
        for tgt_dialect in dialects:
            if src_dialect == tgt_dialect:
                continue
            unique_pointlist_list[f"{src_dialect}->{tgt_dialect}"] = []
    for query_pair in query_pairs:
        src_dialect = query_pair.src_dialect
        tgt_dialect = query_pair.tgt_dialect
        points = query_pair.points
        key = f"{src_dialect}->{tgt_dialect}"
        flag = False
        for unique_pointlist in unique_pointlist_list[key]:
            if point_list_equal(unique_pointlist, points):
                flag = True
                break
        if not flag:
            unique_pointlist_list[key].append(points)
    sum = 0
    for key, unique_pointlist in unique_pointlist_list.items():
        print(f"{key}: {len(unique_pointlist)}")
        sum += len(unique_pointlist)
    print(f"total: {sum}")
    return unique_pointlist_list
