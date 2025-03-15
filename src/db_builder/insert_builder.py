# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: insert_builder$
# @Author: 10379
# @Time: 2025/2/18 21:52
from utils.tools import extract_parameters


def naive_extract_values_from_insert(ins_sql: str, dialect: str):
    "INSERT INTO orders (order_id, order_tms, customer_id, store_id, order_status) VALUES( )"
    tbl_name_begin_pos = ins_sql.find('INSERT INTO') + len('INSERT INTO')
    cut_begin = ins_sql[tbl_name_begin_pos:].strip()
    i = 0
    while cut_begin[i] != ' ':
        i = i + 1
        tbl_name = cut_begin[:i].strip()
    pos_values = ins_sql.find('VALUES')
    if ins_sql[:pos_values].find('(') != -1:
        cols = ins_sql[ins_sql.find('('): ins_sql.find(')') + 1]
        cols = extract_parameters(cols)
    else:
        cols = None
    values = ins_sql[ins_sql.find('VALUES') + len('VALUES'): ins_sql.rfind(')') + 1]
    return tbl_name.strip(), cols, extract_parameters(values.strip())
