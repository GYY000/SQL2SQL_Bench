# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Pattern$
# @Author: 10379
# @Time: 2024/12/25 0:13
from typing import List, Dict
from abc import ABC, abstractmethod

from generator.element.Type import Type, MySQLType, PostgresType, OracleType, ListType, get_dialect_by_type, type_match


class Slot(ABC):
    def __init__(self):
        pass

    def __str__(self):
        return 'slot'


class ValueSlot(Slot):
    def __init__(self, name: str, src_type: Type = None, tgt_type: Type = None):
        super().__init__()
        if tgt_type is None:
            raise ValueError('target_type can\'t be None')
        self.src_type = src_type
        self.tgt_type = tgt_type
        self.name = name
        self.src_value = None
        self.tgt_value = None

    def fulfill(self, src_flag):
        """
        :param src_flag: whether to get the value of source dialect or the target dialect
        :return: the string value of this operand
        """
        assert self.is_fulfilled()
        if src_flag:
            return self.src_value
        else:
            return self.tgt_value

    def prefill(self, col_pairs: List):
        if self.src_type is None:
            return False
        else:
            # deal with List
            src_dialect = get_dialect_by_type(self.src_type)
            tgt_dialect = get_dialect_by_type(self.tgt_type)
            for col in col_pairs:
                if type_match(col[src_dialect].op_type, self.src_type) and type_match(col[tgt_dialect].op_type,
                                                                                      self.tgt_type):
                    self.src_value = col[src_dialect].value
                    self.tgt_value = col[tgt_dialect].value
            if self.src_value is not None:
                return True
            else:
                return False

    def is_fulfilled(self):
        return self.src_value is not None


class Pattern:
    def __init__(self):
        self.elements = []
        # self.for_slots = []

    def add_keyword(self, keyword: str):
        self.elements.append(keyword)

    def add_slot(self, slot: Slot):
        self.elements.append(slot)

    def fulfill_pattern(self, src_flag: bool):
        res = ''
        for ele in self.elements:
            if isinstance(ele, Slot):
                if isinstance(ele, ValueSlot):
                    if not ele.is_fulfilled():
                        raise ValueError(f"Slot {ele.name} haven't been fulfilled "
                                         f"before construction please check the define order of the slots")

                elif isinstance(ele, ForSlot):
                    res = res + "\n" + ele.fulfill(src_flag)
            else:
                if res != '':
                    res = res + ''
                res = res + ele
        return res


class ForSlot(Slot):
    def __init__(self, pattern: Pattern, sub_ele_slots: List[ValueSlot], ele_slots: List[ValueSlot]):
        super().__init__()
        if len(sub_ele_slots) != len(ele_slots):
            raise ValueError("The number of sub_elements in "
                             "for list is not equal to the number of elements in the for list")
        if len(sub_ele_slots) != len(ele_slots):
            raise ValueError("The number of elements in for list is zero")
        self.slots = []
        self.pattern = pattern
        self.sub_ele_slots = sub_ele_slots
        self.ele_slots = ele_slots

    def __str__(self):
        sub_elements = ''
        for ele in self.sub_ele_slots:
            if sub_elements != '':
                sub_elements = sub_elements + ', '
            sub_elements = sub_elements + ele.name
        elements = ''
        for ele in self.ele_slots:
            if elements != '':
                elements = elements + ', '
            elements = elements + ele.name
        split_lines = str(self.pattern).splitlines()
        patterns = ''
        for line in split_lines:
            patterns = patterns + "\t\t" + line + '\n'
        return f"{{\n\tFor {sub_elements} in {elements}:\n{patterns}}}"

    def fulfill(self, src_flag: bool):
        for slot in self.ele_slots:
            assert isinstance(slot, ValueSlot)
            if not slot.is_fulfilled():
                raise ValueError(f"Slot {slot.name} haven't been fulfilled "
                                 f"before construction please check the define order of the slots")
        i = 0
        res = ''
        if src_flag:
            for i in range(len(self.ele_slots[0].src_value)):
                for j in range(len(self.ele_slots)):
                    assert isinstance(self.ele_slots[j].src_type, ListType)
                    self.sub_ele_slots[j].src_value = self.ele_slots[j].src_value[i]
                if res != '':
                    res = res + "\n" + self.pattern.fulfill_pattern(src_flag)
                else:
                    res = res + self.pattern.fulfill_pattern(src_flag)
        else:
            for i in range(len(self.ele_slots[0].tgt_value)):
                for j in range(len(self.ele_slots)):
                    assert isinstance(self.ele_slots[j].tgt_type, ListType)
                    self.sub_ele_slots[j].tgt_value = self.ele_slots[j].tgt_value[i]
                if res != '':
                    res = res + "\n" + self.pattern.fulfill_pattern(src_flag)
                else:
                    res = res + self.pattern.fulfill_pattern(src_flag)
        return res


class UdfFunction:
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
