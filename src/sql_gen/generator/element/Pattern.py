# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Pattern$
# @Author: 10379
# @Time: 2024/12/25 0:13
from typing import List
from abc import ABC, abstractmethod

from sql_gen.generator.ele_type.type_def import ListType, BaseType, QueryType, TableType, OptionType, AliasType, \
    AnyValueType
from sql_gen.generator.element.Operand import Operand


class Slot(ABC):
    def __init__(self):
        pass

    def __str__(self):
        return 'slot'

    @abstractmethod
    def extend(self, used_id) -> str:
        pass


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


class ValueSlot(Slot):
    def __init__(self, name: str, slot_type: BaseType = None, udf_func: UdfFunction = None):
        super().__init__()
        self.slot_type = slot_type
        self.name = name
        self.value = None
        self.udf_func = udf_func

    def __str__(self):
        if self.udf_func is not None:
            return f"<{self.name}: @{str(self.udf_func)}>"
        else:
            return f"<{self.name}: {self.slot_type}>"

    def fill_value(self, op: Operand):
        if self.value is not None:
            assert isinstance(self.value, Operand)
            assert self.value.value == op.value
        else:
            self.value = op

    def is_fulfilled(self):
        return self.value is not None

    def extend(self, used_id) -> str:
        assert not isinstance(self.slot_type, QueryType)
        assert not isinstance(self.slot_type, TableType)
        # ALIAS TABLE QUERY LIST OPTION ANY_VALUE
        if isinstance(self.slot_type, ListType):
            return f"[{self.name}_{used_id}]"
        elif isinstance(self.slot_type, QueryType):
            assert False
        elif isinstance(self.slot_type, TableType):
            assert False
        elif isinstance(self.slot_type, AnyValueType):
            return f"[{self.name}_{used_id}]"
        elif isinstance(self.slot_type, OptionType):
            return f"{self.name}_{used_id}"
        elif isinstance(self.slot_type, AliasType):
            return f"{self.name}_{used_id}"
        else:
            return f"element{used_id}"


class StringLiteralSlot(Slot):
    def __init__(self, literal):
        super().__init__()
        self.literal = literal

    def __str__(self):
        return f"'{self.literal}'"


class NumberLiteralSlot(Slot):
    def __init__(self, num):
        super().__init__()
        self.num = num

    def __str__(self):
        return str(self.num)


class Pattern:
    def __init__(self):
        self.elements = []
        # self.for_slots = []

    def add_keyword(self, keyword: str):
        self.elements.append(keyword)

    def add_slot(self, slot: Slot):
        self.elements.append(slot)

    def fulfill_pattern(self, alias_id_map):
        res = ''
        for ele in self.elements:
            if isinstance(ele, Slot):
                if isinstance(ele, ValueSlot):
                    if not ele.is_fulfilled():
                        if isinstance(ele.slot_type, AliasType):
                            ele.value = Operand(f'ALIAS_{alias_id_map["ALIAS"] + 1}', AliasType())
                            alias_id_map['ALIAS'] = alias_id_map['ALIAS'] + 1
                        else:
                            raise ValueError(f"Slot {ele.name} haven't been fulfilled "
                                             f"before construction please check the define order of the slots")
                    else:
                        res = res + " " + ele.value.str_value()
                elif isinstance(ele, ForSlot):
                    res = res + "\n" + ele.fulfill()
            else:
                if res != '':
                    res = res + ' '
                res = res + ele
        return res

    def extend_pattern(self):
        res = ''
        slot_map = {}
        used_id = 0
        for ele in self.elements:
            if isinstance(ele, str):
                res = res + ele
            else:
                assert isinstance(ele, Slot)
                if ele in slot_map:
                    res = res + " " + slot_map[ele]
                else:
                    used_id = used_id + 1
                    extended_ele = ele.extend(used_id)
                    slot_map[ele] = extended_ele
                res = res + " " + str(slot_map[ele])
        return res, slot_map


class ForSlot(Slot):
    def __init__(self, pattern: Pattern, sub_ele_slots: List[ValueSlot], ele_slots: List[ValueSlot], strip_str):
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
        self.strip_str = strip_str

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
        return f"{{\n\tFor {sub_elements} in {elements} ADD '{self.strip_str}':\n{patterns}}}"

    def fulfill(self):
        for slot in self.ele_slots:
            assert isinstance(slot, ValueSlot)
            if not slot.is_fulfilled():
                raise ValueError(f"Slot {slot.name} haven't been fulfilled "
                                 f"before construction please check the define order of the slots")
        i = 0
        res = ''
        for i in range(len(self.ele_slots[0].value)):
            for j in range(len(self.ele_slots)):
                assert isinstance(self.ele_slots[j].slot_type, ListType)
                self.sub_ele_slots[j].value = self.ele_slots[j].value[i]
            if res != '':
                res = res + "\n" + self.pattern.fulfill_pattern()
            else:
                res = res + self.pattern.fulfill_pattern()
        return res


def extend(self):
    pass
