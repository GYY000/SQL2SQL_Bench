# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: udf_entry$
# @Author: 10379
# @Time: 2025/4/14 12:30
from str_udf import *
from date_udf import *


def udf_entry(func_name: str, *args, **kwargs):
    if func_name == 'reg_escape':
        return reg_escape(*args, **kwargs)
    elif func_name == 'rtrim_reg':
        return rtrim_reg(*args, **kwargs)
    elif func_name == 'ltrim_reg':
        return ltrim_reg(*args, **kwargs)

    elif func_name == 'date_format_udf':
        return date_format_udf(*args, **kwargs)
