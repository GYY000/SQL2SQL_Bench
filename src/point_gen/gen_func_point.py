# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: gen_func_point.py$
# @Author: 10379
# @Time: 2024/12/29 11:41
from model.model_init import init_model
from point_gen.prompt import sys_prompt, user_prompt, sys_prompt_bat, user_prompt_bat


def gen_func_point(func_name, func_desc, src_dialect, tgt_dialect):
    system_prompt = sys_prompt_bat.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect)
    prompt = user_prompt_bat.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect, function_name=func_name,
                                    description=func_desc)

    model = init_model('gpt-4o')
    res = model.trans_func([], system_prompt, prompt)
    print(res)


desc = """
DAYOFWEEK( date ) Returns the weekday index for date ( 1 = Sunday, 2 = Monday, …, 7 = Saturday). These index values correspond to the ODBC standard. Returns NULL if date is NULL . mysql> SELECT DAYOFWEEK('2007-02-03'); -> 7
"""
gen_func_point('DAYOFWEEK', desc, 'MySQL', 'PostgreSQL')
