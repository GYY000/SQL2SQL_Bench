# -*- coding: utf-8 -*-
# @Project: sql2sqlBench
# @Module: get_type$
# @Author: 10379
# @Time: 2024/12/6 12:59
from typing import List

from utils.db_connector import *
from utils.tools import dialect_judge


def get_type(obj: str, dialect: str, db_name,is_table: bool) -> tuple[bool, list]:
    type = dialect_judge(dialect)
    match type:
        case 'mysql':
            return get_mysql_type(obj, db_name, is_table)
        case 'postgres':
            return get_pg_type(obj, db_name, is_table)
        case 'oracle':
            return get_oracle_type(obj, db_name, is_table)
        case _:
            assert False


def get_usable_cols(sql: str, dialect: str):
    type = dialect_judge(dialect)
    match type:
        case 'mysql':
            return get_mysql_usable_cols(sql)
        case 'postgres':
            return get_pg_usable_cols(sql)
        case 'oracle':
            return get_oracle_usable_cols(sql)
        case _:
            assert False

def get_mysql_usable_cols(sql):

    return None

def get_pg_usable_cols(sql):
    return None


def get_oracle_usable_cols(sql):
    return None