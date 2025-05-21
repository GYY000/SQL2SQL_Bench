# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Operand$
# @Author: 10379
# @Time: 2024/12/25 0:16
from antlr_parser.Tree import TreeNode
from sql_gen.generator.ele_type.type_def import BaseType


class Operand:
    def __init__(self, value: TreeNode | str, base_type: BaseType):
        self.value = value
        self.op_type = base_type

    def str_value(self):
        return str(self.value)

    def __str__(self):
        return f"value: {self.value} type: {self.op_type}"

    def __repr__(self):
        return f"value: {self.value} type: {self.op_type}"


class ColumnOp(Operand):
    def __init__(self, dialect: str, column_name: str, table_name: str, base_type: BaseType):
        super().__init__(column_name, base_type)
        self.table_name = table_name
        self.dialect = dialect
        self.column_name = column_name
        if dialect == 'mysql':
            self.value = f"`{table_name}`.`{self.column_name}`"
        elif dialect == 'oracle' or dialect == 'pg':
            self.value = f"\"{self.table_name}\".\"{self.column_name}\""
        else:
            print(f"{dialect} is not supported yet")
            assert False

    def str_value(self):
        return self.value

    def __str__(self):
        return f"value: {self.table_name}.{self.value} type: {self.op_type}"

    def __repr__(self):
        return f"value: {self.table_name}.{self.value} type: {self.op_type}"


class TreeNodeOperand(Operand):
    def __init__(self, value: TreeNode, base_type: BaseType):
        super().__init__(value, base_type)
        self.value = value
        self.op_type = base_type
