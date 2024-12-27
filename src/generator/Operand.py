# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Operand$
# @Author: 10379
# @Time: 2024/12/25 0:16
class Operand:
    def __init__(self, value: str, type: str):
        self.value = value
        self.type = type

    def __str__(self):
        return f"value: {self.value} type: {self.type}"
