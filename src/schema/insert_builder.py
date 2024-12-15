# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: insert_builder$
# @Author: 10379
# @Time: 2024/12/9 20:15
from typing import List, Dict

from utils.tools import dialect_judge


def build_insert(src_dialect: str, data: List[Dict], schema: Dict) -> str:
    try:
        dialect = dialect_judge(src_dialect)
    except Exception as e:
        raise e
    match dialect:
        case 'mysql':
            return mysql_insert(data, schema)
        case 'postgres':
            return pg_insert(data, schema)
        case 'oracle':
            return oracle_insert(data, schema)
        case _:
            assert False


def mysql_insert(data: List[Dict], schema: Dict) -> str:
    pass


def pg_insert(data: List[Dict], schema: Dict) -> str:
    pass


def oracle_insert(data: List[Dict], schema: Dict) -> str:
    pass
