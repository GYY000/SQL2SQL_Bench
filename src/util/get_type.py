# -*- coding: utf-8 -*-
# @Project: sql2sqlBench
# @Module: get_type$
# @Author: 10379
# @Time: 2024/12/6 12:59
from typing import List

from db_connector import *


def get_type(obj: str, dialect: str, db_name,is_table: bool) -> tuple[bool, list]:
    pg_synonyms = ['pg', 'postgres', 'postgreSQL', 'PostgreSQL']
    oracle_synonyms = ['oracle', 'Oracle']
    mysql_synonyms = ['mysql', 'MySQL']
    if dialect in pg_synonyms:
        return get_pg_type(obj, db_name, is_table)
    elif dialect in oracle_synonyms:
        return get_oracle_type(obj, db_name, is_table)
    elif dialect in mysql_synonyms:
        return get_mysql_type(obj, db_name, is_table)
    else:
        print(f"Your dialect have to be one of")
        raise ValueError
