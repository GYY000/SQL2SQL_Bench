# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: date_udf$
# @Author: 10379
# @Time: 2025/3/14 18:54
import random

from sql_gen.generator.ele_type.type_def import StringGeneralType
from sql_gen.generator.element.Operand import Operand


def date_format_udf(format_str: Operand):
    return ((format_str.value.replace('YYYY', '%Y').
             replace('yyyy', '%Y').replace('MM', '%m').
             replace('DD', '%d')).replace('dd', '%d').replace('MONTH', '%M').
            replace('MON', '%b').replace('HH24', '%H').replace('MI', '%i').
            replace('SS', '%S').replace('FF', '%f').replace('Dy', '%a').
            replace('AM', '%p').replace('PM', '%p').replace('HH12', '%I').replace('HH', '%I').
            replace('RR', '%y').replace('US', '%f'))


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


def ds_iso_format():
    pass



