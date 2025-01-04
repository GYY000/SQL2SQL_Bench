# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Operand$
# @Author: 10379
# @Time: 2024/12/25 0:16
from generator.element.Type import Type


class Operand:
    def __init__(self, value: str, op_type: Type):
        self.value = value
        self.op_type = op_type

    def __str__(self):
        return f"value: {self.value} type: {self.op_type}"

    def __repr__(self):
        return f"value: {self.value} type: {self.op_type}"
