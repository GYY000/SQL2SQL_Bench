# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Operand$
# @Author: 10379
# @Time: 2024/12/25 0:16
from sql_gen.generator.ele_type.type_def import BaseType


class Operand:
    def __init__(self, value: str, base_type: BaseType):
        self.value = value
        self.op_type = base_type

    def str_value(self, dialect: str):
        return self.value

    def __str__(self):
        return f"value: {self.value} type: {self.op_type}"

    def __repr__(self):
        return f"value: {self.value} type: {self.op_type}"


class ColumnOp(Operand):
    def __init__(self, column_name: str, table_name: str, base_type: BaseType):
        super().__init__(column_name, base_type)
        self.table_name = table_name

    def str_value(self, dialect: str):
        if dialect == 'mysql':
            return f"`{self.table_name}`.`{self.value}`"
        elif dialect == 'oracle' or dialect == 'pg':
            return f"\"{self.table_name}\".\"{self.value}\""
        else:
            print(f"{dialect} is not supported yet")
            assert False

    def __str__(self):
        return f"value: {self.table_name}.{self.value} type: {self.op_type}"

    def __repr__(self):
        return f"value: {self.table_name}.{self.value} type: {self.op_type}"
