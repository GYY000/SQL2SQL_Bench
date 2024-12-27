# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: ValueType$
# @Author: 10379
# @Time: 2024/12/26 21:15
from generator.Type.Type import Type


class ValueType(Type):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Value'
