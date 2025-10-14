# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Pattern$
# @Author: 10379
# @Time: 2024/12/25 0:13
import traceback
from typing import List
from abc import ABC, abstractmethod

from antlr_parser.Tree import TreeNode
from sql_gen.generator.ele_type.type_def import ListType, BaseType, QueryType, TableType, OptionType, AliasType, \
    AnyValueType, StringGeneralType, NumberType
from sql_gen.generator.element.Operand import Operand
from sql_gen.generator.udfs.UdfFunction import getReturnType, execute, fulfill_cond, function_registered
from utils.ExecutionEnv import ExecutionEnv
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
        if not function_registered(func_name):
            raise ValueError(f"Function {func_name} not registered")
        self.arg_slots = arg_slots
        self.func_name = func_name

    def get_return_type(self):
        return getReturnType(self.func_name)

    def execute(self, slot_value_map: dict, select_stmt_node: TreeNode, execute_env: ExecutionEnv,
                dialect: str | None, source_flag: bool):
        args = []
        for slot in self.arg_slots:
            if isinstance(slot, ValueSlot):
                args.append(slot.get_value(slot_value_map, select_stmt_node, execute_env, dialect, source_flag))
            else:
                assert (isinstance(slot, StringLiteralSlot) or isinstance(slot, NumberLiteralSlot)
                        or isinstance(slot, ListLiteralSlot))
                args.append(slot.get_op())
        return execute(self.func_name, dialect, execute_env, select_stmt_node, *args)

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


def get_value_final_rep(value):
    assert isinstance(value, Operand) or isinstance(value, list)
    if isinstance(value, Operand):
        return value.str_value()
    else:
        return ','.join([get_value_final_rep(ele) for ele in value])


def get_tgt_value(src_value_type: BaseType, value: Operand | list, source_flag: bool):
    assert isinstance(value, Operand) or isinstance(value, list)
    if isinstance(src_value_type, OptionType):
        if source_flag:
            assert value.str_value() in src_value_type.map_dict
            return value
        else:
            if value.str_value() not in list(src_value_type.map_dict.values()):
                assert value.str_value() in src_value_type.map_dict
                value = src_value_type.map_dict[value.str_value()]
                return Operand(value, BaseType(''))
            else:
                return value
    elif isinstance(src_value_type, ListType):
        res = []
        for val in value:
            res.append(get_tgt_value(src_value_type.element_type, val, source_flag))
        return res
    return value


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
        if self.get_type() is None:
            print(self.name)
        return self.get_type().gen_demo_value()

    def get_value(self, slot_value_map: dict, select_stmt_node: TreeNode, execution_env: ExecutionEnv,
                  dialect: str | None, source_flag: bool):
        if not self.is_fulfilled(slot_value_map):
            raise ValueError(
                f"Slot {self.name} haven't been fulfilled before construction "
                f"please check the define order of the slots")
        if self in slot_value_map:
            used_value = get_tgt_value(self.get_type(), slot_value_map[self], source_flag)
            return used_value
        else:
            assert self.udf_func is not None
            # only execute once, for some udf_func contain random component
            slot_value_map[self] = self.udf_func.execute(slot_value_map, select_stmt_node, execution_env, dialect,
                                                         source_flag)
            # if self.func_res is None:
            #     self.func_res = self.udf_func.execute(slot_value_map, select_stmt_node, execution_env, dialect,
            #                                           source_flag)
            return slot_value_map[self]

    def generate_value(self, usable_cols: list, root_node: TreeNode, slot_value_map: dict,
                       length: int | None, src_dialect: str, execution_env: ExecutionEnv):
        assert self.udf_func is None
        new_value = self.slot_type.generate_value(usable_cols, root_node, length,
                                                  src_dialect, execution_env)
        return new_value


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


class ListLiteralSlot(Slot):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return '[]'

    def __repr__(self):
        return self.__str__()

    def extend(self):
        # won't be used
        assert False

    def get_op(self):
        return []


class Pattern:
    def __init__(self):
        self.elements = []
        # self.for_slots = []

    def add_keyword(self, keyword: str):
        self.elements.append(keyword)

    def add_slot(self, slot: Slot):
        self.elements.append(slot)

    def fulfill_pattern(self, alias_id_map, slot_value_map, select_stmt_node: TreeNode | None,
                        execution_env: ExecutionEnv | None, dialect: str | None, source_flag: bool):
        res = ''
        slot_length_map = {}
        for ele in self.elements:
            if isinstance(ele, ForSlot):
                length = None
                for slot in ele.ele_slots:
                    if slot in slot_value_map:
                        length = len(slot_value_map[slot])
                if length is not None:
                    for slot in ele.ele_slots:
                        slot_length_map[id(slot)] = length
        for ele in self.elements:
            if isinstance(ele, Slot):
                if isinstance(ele, ValueSlot):
                    if not ele.is_fulfilled(slot_value_map):
                        if isinstance(ele.get_type(), AliasType):
                            if 'ALIAS' not in alias_id_map:
                                alias_id_map['ALIAS'] = 0
                            used_str = f'ALIAS_{alias_id_map["ALIAS"] + 1}'
                            slot_value_map[ele] = Operand(used_str, AliasType())
                            alias_id_map['ALIAS'] = alias_id_map['ALIAS'] + 1
                            res = res + ' ' + used_str
                        else:
                            traceback.print_stack()
                            raise ValueError(f"Slot {ele.name} haven't been fulfilled "
                                             f"before construction please check the define order of the slots")
                    else:
                        res = res + get_value_final_rep(
                            ele.get_value(slot_value_map, select_stmt_node, execution_env, dialect, source_flag))
                elif isinstance(ele, ForSlot):
                    res = res + "\n" + ele.fulfill(slot_value_map, select_stmt_node,
                                                   execution_env, dialect, alias_id_map, source_flag, slot_length_map)
            else:
                res = res + ele
        return res

    def extend_pattern(self):
        res = ''
        slot_list = []
        quote_mark = False
        for ele in self.elements:
            if isinstance(ele, str):
                res = res + ele
                i = 0
                while i < len(ele):
                    if ele[i] == "'":
                        quote_mark = not quote_mark
                    if ele[i] == '\\':
                        i = i + 1
                    i = i + 1
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
                res = (res + extended_ele['first_str'] + extended_ele['second_str'] +
                       extended_ele['second_str'])
            else:
                ori_len = get_no_space_len(res)
                assert isinstance(ele, ValueSlot)
                extended_ele = ele.extend()
                used_len = 0
                i = 0
                while i < len(extended_ele):
                    if not quote_mark and extended_ele[i] == " ":
                        i = i + 1
                    else:
                        used_len = used_len + 1
                        if extended_ele[i] == "'":
                            quote_mark = not quote_mark
                        if extended_ele[i] == '\\':
                            used_len += 1
                            i = i + 1
                        i = i + 1
                slot_list.append({
                    "slot": ele,
                    "info": {
                        "pos": [ori_len, ori_len + used_len - 1]
                    }
                })
                res = res + extended_ele
        return res, slot_list


def rm_func_res(pattern: Pattern, slot: ValueSlot):
    for ele in pattern.elements:
        if isinstance(ele, ValueSlot):
            if ele.udf_func is not None:
                for args in ele.udf_func.arg_slots:
                    if args == slot:
                        ele.func_res = None
                        break
        elif isinstance(ele, ForSlot):
            rm_func_res(ele.pattern, slot)
            for sub_ele in ele.ele_slots:
                if sub_ele.udf_func is not None:
                    for args in sub_ele.udf_func.arg_slots:
                        if args == slot:
                            sub_ele.func_res = None
                            break


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

    def fulfill(self, slot_value_map, select_stmt_node, execution_env: ExecutionEnv,
                dialect: str, alias_id_map: dict, source_flag: bool, slot_length_map):
        slot_to_value = {}
        length = None
        for slot in self.ele_slots:
            assert isinstance(slot, ValueSlot)
            if not slot.is_fulfilled(slot_value_map):
                if isinstance(slot.get_type(), ListType) and isinstance(slot.get_type().element_type, AliasType):
                    if length is None:
                        assert id(slot) in slot_length_map
                        length = slot_length_map[id(slot)]
                        # for slot1 in self.ele_slots:
                        #     if slot1.is_fulfilled(slot_value_map):
                        #         value = slot1.get_value(slot_value_map, select_stmt_node, execution_env, dialect, source_flag)
                        #         length = len(value)
                        #         slot_to_value[slot1] = value
                    assert length is not None
                    res = []
                    for i in range(length):
                        if 'ALIAS' not in alias_id_map:
                            alias_id_map['ALIAS'] = 0
                        res.append(Operand(f'ALIAS_{alias_id_map["ALIAS"] + 1}', AliasType()))
                        alias_id_map['ALIAS'] = alias_id_map['ALIAS'] + 1
                    slot_to_value[slot] = res
                    slot_value_map[slot] = res
                else:
                    raise ValueError(f"Slot {slot.name} haven't been fulfilled "
                                     f"before construction please check the define order of the slots")
        for slot in self.ele_slots:
            if slot in slot_to_value:
                continue
            slot_to_value[slot] = slot.get_value(slot_value_map, select_stmt_node, execution_env, dialect, source_flag)
        res = ''
        sub_slot_value_map = {}
        for slot, value in slot_value_map.items():
            if slot not in self.sub_ele_slots:
                sub_slot_value_map[slot] = value
        for i in range(len(slot_to_value[self.ele_slots[0]])):
            for j in range(len(self.ele_slots)):
                assert isinstance(self.ele_slots[j].get_type(), ListType)
                sub_slot_value_map[self.sub_ele_slots[j]] = (
                    slot_to_value[self.ele_slots[j]])[i]
            if res != '':
                res = res + self.strip_str + self.pattern.fulfill_pattern({}, sub_slot_value_map,
                                                                          select_stmt_node, execution_env, dialect,
                                                                          source_flag)
            else:
                res = res + self.pattern.fulfill_pattern({}, sub_slot_value_map, select_stmt_node,
                                                         execution_env, dialect, source_flag)
            for slot in self.sub_ele_slots:
                rm_func_res(self.pattern, slot)
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
        # if 'slot_list' in slot_info:
        #     add_pos_for_slot_list(slot_info['slot_list'][0], dis)
        #     add_pos_for_slot_list(slot_info['slot_list'][1], dis)
