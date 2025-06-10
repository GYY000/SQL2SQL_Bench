# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Pattern$
# @Author: 10379
# @Time: 2024/12/25 0:13
from typing import List
from abc import ABC, abstractmethod

from antlr_parser.Tree import TreeNode
from sql_gen.generator.ele_type.type_def import ListType, BaseType, QueryType, TableType, OptionType, AliasType, \
    AnyValueType, StringGeneralType, NumberType
from sql_gen.generator.element.Operand import Operand
from sql_gen.generator.udfs.UdfFunction import getReturnType, execute, fulfill_cond
from utils.tools import get_no_space_len


class Slot(ABC):
    def __init__(self):
        pass

    def __str__(self):
        return 'slot'

    @abstractmethod
    def __repr__(self):
        return self.__str__()

    @abstractmethod
    def extend(self):
        pass


class UdfFunction:
    def __init__(self, func_name: str, arg_slots: List[Slot]):
        self.arg_slots = arg_slots
        self.func_name = func_name

    def get_return_type(self):
        return getReturnType(self.func_name)

    def execute(self, slot_value_map: dict):
        args = []
        for slot in self.arg_slots:
            if isinstance(slot, ValueSlot):
                args.append(slot.get_value(slot_value_map))
            else:
                assert isinstance(slot, StringLiteralSlot) or isinstance(slot, NumberLiteralSlot)
                args.append(slot.get_op())

        return execute(self.func_name, *args)

    def fulfill_cond(self, value: TreeNode):
        return fulfill_cond(self.func_name, value)

    def __str__(self):
        params = ""
        for slot in self.arg_slots:
            if params != '':
                params = params + ", "
            params = params + str(slot)
        return f"{self.func_name}({params.strip()})"

    def is_fulfilled(self, slot_value_map: dict):
        for slot in self.arg_slots:
            if isinstance(slot, ValueSlot):
                if slot not in slot_value_map:
                    return False
        return True


class ValueSlot(Slot):
    def __init__(self, name: str, slot_type: BaseType = None, udf_func: UdfFunction = None):
        super().__init__()
        self.slot_type = slot_type
        self.name = name
        self.udf_func = udf_func

    def __str__(self):
        if self.udf_func is not None:
            return f"<{self.name}: @{str(self.udf_func)}>"
        else:
            return f"<{self.name}: {self.get_type()}>"

    def __repr__(self):
        return self.__str__()

    def get_type(self):
        if self.udf_func is not None:
            return self.udf_func.get_return_type()
        else:
            return self.slot_type

    def is_fulfilled(self, slot_value_map: dict):
        if self in slot_value_map:
            return True
        else:
            if self.udf_func is not None:
                return self.udf_func.is_fulfilled(slot_value_map)
        return False

    def extend(self):
        # ALIAS TABLE QUERY LIST OPTION ANY_VALUE
        return self.get_type().gen_demo_value()

    def get_value(self, slot_value_map: dict):
        if not self.is_fulfilled(slot_value_map):
            raise ValueError(
                f"Slot {self.name} haven't been fulfilled before construction "
                f"please check the define order of the slots")
        if self in slot_value_map:
            return slot_value_map[self]
        else:
            return self.udf_func.execute(slot_value_map)


class StringLiteralSlot(Slot):
    def __init__(self, literal):
        super().__init__()
        self.literal = literal

    def __str__(self):
        return f"'{self.literal}'"

    def __repr__(self):
        return self.__str__()

    def extend(self):
        # won't be used
        assert False

    def get_op(self):
        return Operand(self.literal, StringGeneralType())


class NumberLiteralSlot(Slot):
    def __init__(self, num):
        super().__init__()
        self.num = num

    def __str__(self):
        return str(self.num)

    def __repr__(self):
        return self.__str__()

    def extend(self):
        # won't be used
        assert False

    def get_op(self):
        return Operand(self.num, NumberType())


class Pattern:
    def __init__(self):
        self.elements = []
        # self.for_slots = []

    def add_keyword(self, keyword: str):
        self.elements.append(keyword)

    def add_slot(self, slot: Slot):
        self.elements.append(slot)

    def fulfill_pattern(self, alias_id_map, slot_value_map):
        res = ''
        inside_quote = False
        for ele in self.elements:
            if isinstance(ele, Slot):
                if isinstance(ele, ValueSlot):
                    if not ele.is_fulfilled(slot_value_map):
                        if isinstance(ele.get_type(), AliasType):
                            ele.value = Operand(f'ALIAS_{alias_id_map["ALIAS"] + 1}', AliasType())
                            alias_id_map['ALIAS'] = alias_id_map['ALIAS'] + 1
                        else:
                            raise ValueError(f"Slot {ele.name} haven't been fulfilled "
                                             f"before construction please check the define order of the slots")
                    else:
                        if not inside_quote:
                            res = res + " "
                        res = res + ele.get_value(slot_value_map).str_value()
                elif isinstance(ele, ForSlot):
                    res = res + "\n" + ele.fulfill(slot_value_map)
            else:
                if res != '' and not inside_quote:
                    res = res + ' '
                i = 0
                while i < len(ele):
                    char = ele[i]
                    if char == '\'':
                        inside_quote = not inside_quote
                    if char == '\\':
                        i = i + 1
                    i = i + 1
                res = res + ele
        return res

    def extend_pattern(self):
        res = ''
        inside_quote = False
        slot_list = []
        for ele in self.elements:
            if isinstance(ele, str):
                i = 0
                while i < len(ele):
                    char = ele[i]
                    if char == '\'':
                        inside_quote = not inside_quote
                    if char == '\\':
                        i = i + 1
                    i = i + 1
                res = res + ele
            elif isinstance(ele, ForSlot):
                ori_len = get_no_space_len(res)
                extended_ele, loop_slot_list0, loop_slot_list1 = ele.extend()
                slot_list.append({
                    "slot": ele,
                    "info": {
                        "pos": [ori_len, ori_len + get_no_space_len(extended_ele['first_str']),
                                ori_len + get_no_space_len(extended_ele['first_str']) + get_no_space_len(
                                    extended_ele['second_str']),
                                ori_len + get_no_space_len(extended_ele['first_str']) + get_no_space_len(
                                    extended_ele['second_str']) +
                                get_no_space_len(extended_ele['second_str']) - 1]
                    },
                    "slot_list": [loop_slot_list0, loop_slot_list1]
                })
                res = (res + " " + extended_ele['first_str'] + extended_ele['second_str'] +
                       extended_ele['second_str'])
            else:
                ori_len = get_no_space_len(res)
                assert isinstance(ele, ValueSlot)
                extended_ele = ele.extend()
                slot_list.append({
                    "slot": ele,
                    "info": {
                        "pos": [ori_len, ori_len + get_no_space_len(extended_ele) - 1]
                    }
                })
                if not inside_quote:
                    res = res + " " + str(extended_ele)
                else:
                    res = res + extended_ele
        return res, slot_list


class ForSlot(Slot):
    def __init__(self, pattern: Pattern, sub_ele_slots: List[ValueSlot], ele_slots: List[ValueSlot], strip_str):
        super().__init__()
        if len(sub_ele_slots) != len(ele_slots):
            raise ValueError("The number of sub_elements in "
                             "for list is not equal to the number of elements in the for list")
        if len(sub_ele_slots) != len(ele_slots):
            raise ValueError("The number of elements in for list is zero")
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

    def __repr__(self):
        return self.__str__()

    def fulfill(self, slot_value_map):
        for slot in self.ele_slots:
            assert isinstance(slot, ValueSlot)
            if not slot.is_fulfilled(slot_value_map):
                raise ValueError(f"Slot {slot.name} haven't been fulfilled "
                                 f"before construction please check the define order of the slots")
        i = 0
        res = ''
        sub_slot_value_map = {}
        for slot, value in slot_value_map.items():
            if slot not in self.sub_ele_slots:
                sub_slot_value_map[slot] = value
        for i in range(len(slot_value_map[self.ele_slots[0]].value)):
            for j in range(len(self.ele_slots)):
                assert isinstance(self.ele_slots[j].get_type(), ListType)
                sub_slot_value_map[self.sub_ele_slots[j]] = (
                    slot_value_map[self.ele_slots[j]].value)[i]
            if res != '':
                res = res + self.strip_str + " " + self.pattern.fulfill_pattern({}, sub_slot_value_map)
            else:
                res = res + self.pattern.fulfill_pattern({}, sub_slot_value_map)
        return res

    def extend(self) -> tuple:
        res0, slot_list1 = self.pattern.extend_pattern()
        res1, slot_list2 = self.pattern.extend_pattern()
        res1 = self.strip_str + res1
        add_dis = get_no_space_len(self.strip_str)
        add_pos_for_slot_list(slot_list2, add_dis)
        return {
            "first_str": res0,
            "second_str": res1,
        }, slot_list1, slot_list2


def add_pos_for_slot_list(slot_list, dis):
    for slot_info in slot_list:
        for i in range(len(slot_info['info']['pos'])):
            slot_info['info']['pos'][i] = slot_info['info']['pos'][i] + dis
        if 'slot_list' in slot_info:
            add_pos_for_slot_list(slot_info['slot_list'][0], dis)
            add_pos_for_slot_list(slot_info['slot_list'][1], dis)
