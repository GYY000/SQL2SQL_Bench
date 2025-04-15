# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: date_udf$
# @Author: 10379
# @Time: 2025/3/14 18:54
from sql_gen.generator.element.Operand import Operand
from utils.tools import date_format_trans


def date_format_udf(format_str: Operand):
    return date_format_trans(format_str.value)


def gen_pg_interval_units():
    pass


def gen_pg_interval_values():
    pass


def gen_pg_iso_interval_units():
    pass


def gen_pg_iso_values():
    pass


def gen_mysql_interval():
    pass


def gen_iso_pt_literal():
    pass


def gen_iso_literal():
    pass

def gen_oracle_interval():
    pass


def gen_oracle_interval_literal():
    pass


def gen_mysql_single_interval_unit():
    pass


def gen_mysql_to_interval_unit():
    pass


def eliminate_escape():
    pass


def gen_format_interval_value():
    pass


def gen_pg_interval_literal():
    pass