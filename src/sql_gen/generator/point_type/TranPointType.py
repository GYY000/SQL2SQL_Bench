# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: PointType$
# @Author: 10379
# @Time: 2025/6/14 20:12
import random
from abc import ABC, abstractmethod


class TranPointType(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def parsing_rule_name(self, dialect):
        pass

    def get_generate_full_pattern(self, src_pattern):
        return None


class LiteralType(TranPointType):
    def parsing_rule_name(self, dialect):
        if dialect == 'pg':
            return 'aexprconst'
        elif dialect == 'mysql':
            return 'expressionAtom'
        elif dialect == 'oracle':
            return 'constant'


class ExpressionType(TranPointType):
    def parsing_rule_name(self, dialect):
        if dialect == 'pg':
            return 'a_expr'
        elif dialect == 'mysql':
            return 'expression'
        elif dialect == 'oracle':
            return 'expression'


class ClauseType(TranPointType):
    def parsing_rule_name(self, dialect):
        if dialect == 'pg':
            return 'root'
        elif dialect == 'mysql':
            return 'root'
        elif dialect == 'oracle':
            return 'sql_script'


class OrderByClauseType(TranPointType):
    def get_full_pattern(self, src_pattern):
        if random.randint(1, 2) == 1:
            return (f"SELECT <col_list: LIST[ANY_VALUE]> FROM <tbl_list: LIST[TABLE]> "
                    f"WHERE <cond: BOOL> {src_pattern}")
        else:
            return (f"SELECT <col_list: LIST[ANY_VALUE]> FROM <tbl_list: LIST[TABLE]> "
                    f"WHERE <cond: BOOL> GROUP BY <col_list: LIST[ANY_VALUE]> {src_pattern}")

    def parsing_rule_name(self, dialect):
        if dialect == 'pg':
            return 'sort_clause'
        elif dialect == 'mysql':
            return 'orderByClause'
        elif dialect == 'oracle':
            return 'order_by_clause'


class ReservedKeywordType(TranPointType):
    def parsing_rule_name(self, dialect):
        # won't be used
        assert False


class TableType(TranPointType):
    def get_full_pattern(self, src_pattern):
        if random.randint(1, 2) == 1:
            return f"SELECT <col_list: LIST[ANY_VALUE]> FROM {src_pattern}, <tbl_list: LIST[TABLE]> WHERE <cond: BOOL>"
        else:
            return (f"SELECT <col_list: LIST[ANY_VALUE]> FROM {src_pattern}, "
                    f"<tbl_list: LIST[TABLE]> WHERE <cond: BOOL> GROUP BY <col_list: LIST[ANY_VALUE]>")

    def parsing_rule_name(self, dialect):
        assert False


def gen_point_type(point_type: str):
    if point_type == 'LITERAL':
        return LiteralType()
    elif point_type == 'AGGREGATE_FUNCTION' or point_type == 'FUNCTION' or point_type == 'EXPRESSION':
        return ExpressionType()
    elif point_type == 'PATTERN':
        return ClauseType()
    elif point_type == 'ORDER_BY_CLAUSE':
        return OrderByClauseType()
    elif point_type == 'RESERVED_KEYWORD' or point_type == 'QUOTE':
        return ReservedKeywordType()
    elif point_type == 'TABLE':
        return TableType()
    else:
        raise ValueError(f"Unknown point type: {point_type}")
