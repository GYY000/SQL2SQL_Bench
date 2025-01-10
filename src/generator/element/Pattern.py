# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Pattern$
# @Author: 10379
# @Time: 2024/12/25 0:13
from typing import List, Dict
from abc import ABC, abstractmethod

from generator.element.Type import Type, MySQLType, PostgresType, OracleType


class Slot(ABC):
    def __init__(self):
        pass

    def __str__(self):
        return 'slot'


class ValueSlot(Slot):
    def __init__(self, name: str, src_type: Type = None, tgt_type: Type = None):
        super().__init__()
        self.src_type = src_type
        self.tgt_type = tgt_type
        self.name = name

    def fulfill(self, cols, tgt_dialect: str):
        pass


class Pattern:
    def __init__(self):
        self.elements = []
        # self.for_slots = []

    def add_keyword(self, keyword: str):
        self.elements.append(keyword)

    def add_slot(self, slot: Slot):
        self.elements.append(slot)
        # if isinstance(slot, ForSlot):
        #     self.elements.append(slot)
        #     self.for_slots.append(slot)
        #     temp_slots = []
        #     for for_slot in slot.ele_slots:
        #         if isinstance(for_slot, ValueSlot):
        #             temp_slots.append(self.set_or_get_value_slot(for_slot))
        #         else:
        #             temp_slots.append(for_slot)
        #     slot.slots = temp_slots
        #     return slot
        # elif isinstance(slot, UdfFunction):
        #     self.elements.append(slot)
        #     temp_slots = []
        #     for func_slot in slot.arg_slots:
        #         if isinstance(func_slot, ValueSlot):
        #             temp_slots.append(self.set_or_get_value_slot(func_slot))
        #         else:
        #             temp_slots.append(func_slot)
        #     slot.slots = temp_slots
        #     return slot
        # elif isinstance(slot, ValueSlot):
        #     slot = self.set_or_get_value_slot(slot)
        #     self.elements.append(slot)
        #     return slot


class ForSlot(Slot):
    def __init__(self, pattern: Pattern, sub_ele_slots: List[ValueSlot], ele_slots: List[Slot]):
        super().__init__()
        self.slots = []
        self.pattern = pattern
        self.sub_ele_slots = sub_ele_slots
        self.ele_slots = ele_slots

    def __str__(self):
        # TODO:
        return "For loop"


class UdfFunction():
    def __init__(self, func_name: str, arg_slots: List[Slot], func_def: str = None):
        self.arg_slots = arg_slots
        self.func_def = func_def
        self.func_name = func_name

    def __str__(self):
        params = ""
        for slot in self.arg_slots:
            if params != '':
                params = params + ", "
            params = params + str(slot)
        return f"{self.func_name}({params.strip()})"


# class MySQLValueSlot(ValueSlot):
#     def __init__(self, name: str, value_type: MySQLType):
#         super().__init__(name, value_type)
#
#     def fulfill(self, cols, tgt_dialect: str):
#         """
#             ANY_VALUE = 0
#             INT = 1
#             BOOL = 2
#             FLOAT = 3
#             DATE = 4
#             TIME = 5
#             TIMESTAMP = 6
#             TEXT = 7
#             JSON = 8
#             POINT = 9
#             NULL = 10
#             YEAR = 11
#         """
#         if self.value_type == MySQLType.ANY_VALUE:
#             pass
#         elif self.value_type == MySQLType.INT:
#             pass
#         elif self.value_type == MySQLType.BOOL:
#             pass
#         elif self.value_type == MySQLType.FLOAT:
#             pass
#         elif self.value_type == MySQLType.DATE:
#             pass
#         elif self.value_type == MySQLType.TIME:
#             pass
#         elif self.value_type == MySQLType.TIMESTAMP:
#             pass
#         elif self.value_type == MySQLType.TEXT:
#             pass
#         elif self.value_type == MySQLType.JSON:
#             pass
#         elif self.value_type == MySQLType.POINT:
#             pass
#         elif self.value_type == MySQLType.NULL:
#             pass
#         elif self.value_type == MySQLType.YEAR:
#             pass
#         else:
#             raise ValueError
#
#
# class PostgresValueSlot(ValueSlot):
#     def __init__(self, name: str, value_type: PostgresType):
#         super().__init__(name, value_type)
#
#     def fulfill(self, cols, tgt_dialect: str):
#         pass
#
#
# class OracleValueSlot(ValueSlot):
#     def __init__(self, name: str, value_type: OracleType):
#         super().__init__(name, value_type)
#
#     def fulfill(self, cols, tgt_dialect: str):
#         pass
