# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: tools$
# @Author: 10379
# @Time: 2024/12/9 20:18
import os


def dialect_judge(dialect: str):
    oracle_synonyms = ['oracle']
    pg_synonyms = ['pg', 'postgres', 'postgreSQL']
    mysql_synonyms = ['mysql']
    if dialect.lower() in mysql_synonyms:
        return 'mysql'
    elif dialect.lower() in pg_synonyms:
        return 'postgres'
    elif dialect.lower() in oracle_synonyms:
        return 'oracle'
    else:
        raise ValueError(f"Dialect must be one of {oracle_synonyms + pg_synonyms + mysql_synonyms}")


def get_proj_root_path():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
