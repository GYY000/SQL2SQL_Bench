# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: point_parser$
# @Author: 10379
# @Time: 2024/12/25 12:40
import json
from typing import List, Dict

from sql_gen.generator.ele_type.Attribute import AttributeContainer
from sql_gen.generator.ele_type.type_def import OptionType, ListType, BaseType
from sql_gen.generator.ele_type.type_operation import gen_type_through_str
from sql_gen.generator.element.Pattern import Pattern, ForSlot, UdfFunction, ValueSlot, StringLiteralSlot, \
    NumberLiteralSlot
from sql_gen.generator.element.Point import Point
from utils.tools import get_proj_root_path


def get_slot_by_name(name: str, value_slot_defs: List[List[ValueSlot]]):
    i = len(value_slot_defs) - 1
    while i >= 0:
        for j in range(len(value_slot_defs[i])):
            if name == value_slot_defs[i][j].name:
                return value_slot_defs[i][j]
        i = i - 1
    raise ValueError(f"can't find var def of {name}")


def check_dup_name(name: str, value_slot_defs: List[List[ValueSlot]]):
    slot_layer = value_slot_defs[len(value_slot_defs) - 1]
    for i in range(len(slot_layer)):
        if name == slot_layer[i].name:
            return True
    return False


def find_same_layer_bracket(string: str, pos: int, char: str):
    if char == '(':
        rev_char = ')'
    elif char == '[':
        rev_char = ']'
    elif char == '{':
        rev_char = '}'
    else:
        assert False
    layer = 0
    while pos < len(string):
        if string[pos] == char:
            layer = layer + 1
        elif string[pos] == rev_char:
            layer = layer - 1
        if layer == 0:
            return pos
        pos = pos + 1
    return -1


def load_str(str_to_read: str, index_begin):
    while str_to_read[index_begin].isspace():
        index_begin = index_begin + 1
    assert str_to_read[index_begin] == '\'' or str_to_read[index_begin] == '\"'
    quote_begin = str_to_read[index_begin]
    index_begin = index_begin + 1
    res = ''
    while str_to_read[index_begin] != quote_begin:
        if str_to_read[index_begin] == '\\':
            index_begin = index_begin + 1
            if str_to_read[index_begin] == 'n':
                res = res + '\n'
            elif str_to_read[index_begin] == 't':
                res = res + '\t'
            elif str_to_read[index_begin] == 'r':
                res = res + '\r'
            elif str_to_read[index_begin] == 'b':
                res = res + '\b'
            elif str_to_read[index_begin] == 'f':
                res = res + '\f'
            elif str_to_read[index_begin] in ['\\', '\'', '\"']:
                res = res + str_to_read[index_begin]
            elif str_to_read[index_begin] == 'u':
                res = res + chr(int(str_to_read[index_begin + 1:index_begin + 5], 16))
                index_begin = index_begin + 4
        else:
            res = res + str_to_read[index_begin]
        index_begin = index_begin + 1
    return res, index_begin + 1


def parse_point(point: Dict) -> Point:
    src_dialect = point['Dialect']['Src']
    tgt_dialect = point['Dialect']['Tgt']
    for key, value in point.items():
        assert key in ['Dialect', 'Desc', 'Return', 'SrcPattern', 'TgtPattern', 'Type', 'Condition']
    # print(point)
    src_pattern = point['SrcPattern']
    tgt_pattern = point['TgtPattern']
    point_type = point['Type']
    return_type = None
    predicate = None
    if point_type == 'FUNCTION' or point_type == 'AGGREGATE_FUNCTION':
        assert 'Return' in point
        return_type = point['Return']
    if 'Condition' in point:
        predicate = point['Condition']
    slot_defs = [[]]
    src_pattern, _ = parse_pattern(src_pattern, 0, src_dialect, slot_defs)
    tgt_pattern, _ = parse_pattern(tgt_pattern, 0, tgt_dialect, slot_defs)

    return Point(src_pattern, tgt_pattern, slot_defs[0], point_type, return_type, predicate)


def parse_pattern(pattern_str: str, index_begin: int, dialect: str, slot_defs) -> tuple[Pattern, int]:
    i = index_begin
    pattern = Pattern()
    cur_str = ''
    while i < len(pattern_str):
        token = pattern_str[i]
        if token == '\\':
            i = i + 1
            cur_str = cur_str + pattern_str[i]
            i = i + 1
        elif token == '<':
            if cur_str != '':
                pattern.add_keyword(cur_str)
                cur_str = ''
            slot, i = parse_slot(pattern_str, i, dialect, slot_defs)
            pattern.add_slot(slot)
        elif token == '{':
            if cur_str != '':
                pattern.add_keyword(cur_str)
                cur_str = ''
            for_slot, i = parse_for_loop(pattern_str, i, dialect, slot_defs)
            pattern.add_slot(for_slot)
        else:
            cur_str = cur_str + token
            i = i + 1
    if cur_str != '':
        pattern.add_keyword(cur_str)
    return pattern, i


def parse_for_loop(pattern_str, index_begin: int, dialect: str, slot_defs: List) -> tuple[ForSlot, int]:
    while pattern_str[index_begin].isspace():
        index_begin = index_begin + 1
    assert pattern_str[index_begin] == '{'
    i = index_begin + 1
    assert pattern_str[i:].startswith('for')
    i = i + 3
    while pattern_str[i].isspace():
        i = i + 1
    sub_value_slots = []
    while not pattern_str[i:].startswith('in'):
        assert pattern_str[i] == '<'
        i = i + 1
        name = ''
        while i < len(pattern_str) and pattern_str[i] != '>':
            name = name + pattern_str[i]
            i = i + 1
        assert pattern_str[i] == '>'
        i = i + 1
        while pattern_str[i].isspace():
            i = i + 1
        if pattern_str[i] == ',':
            i = i + 1
        while pattern_str[i].isspace():
            i = i + 1
        sub_value_slots.append(ValueSlot(name))
    i = i + 2
    while pattern_str[i].isspace():
        i = i + 1
    slots = []
    while not pattern_str[i:].startswith('ADD') and not pattern_str[i:].startswith(':'):
        assert pattern_str[i] == '<'
        slot, i = parse_slot(pattern_str, i, dialect, slot_defs)
        while pattern_str[i].isspace():
            i = i + 1
        assert pattern_str[i] == ',' or pattern_str[i:].startswith('ADD') or pattern_str[i:].startswith(':')
        slots.append(slot)
        if pattern_str[i] == ',':
            i = i + 1
        while pattern_str[i].isspace():
            i = i + 1

    strip_str = ''
    if pattern_str[i:].startswith('ADD'):
        strip_str, i = load_str(pattern_str, i + 3)
        while pattern_str[i].isspace():
            i = i + 1
    assert pattern_str[i] == ':'
    i = i + 1
    flag = 0
    j = i
    while True:
        if pattern_str[j] == '\\':
            j = j + 1
        elif pattern_str[j] == '{':
            flag = flag + 1
        elif pattern_str[j] == '}':
            if flag == 0:
                break
            else:
                flag = flag - 1
        j = j + 1
    new_def_layer = []
    assert len(sub_value_slots) == len(slots)
    for k in range(len(sub_value_slots)):
        assert isinstance(slots[k].get_type(), ListType)
        if slots[k].udf_func is None:
            sub_value_slots[k].slot_type = slots[k].get_type().element_type
        new_def_layer.append(sub_value_slots[k])
    slot_defs.append(new_def_layer)
    pattern, i = parse_pattern(pattern_str[i:j], 0, dialect, slot_defs)
    slot_defs.pop()
    return ForSlot(pattern, sub_value_slots, slots, strip_str), j + 1


def parse_slot(pattern_str: str, index_begin: int, dialect: str,
               slot_defs: List) -> tuple[ValueSlot, int]:
    assert pattern_str[index_begin] == '<'
    i = index_begin + 1
    while pattern_str[i] != ':' and pattern_str[i] != '>' and i < len(pattern_str):
        i = i + 1
    if i == len(pattern_str):
        raise ValueError(
            f'Translation Point Syntax Error for slot definition. '
            f'Point definition is {pattern_str}\\ Position is {index_begin}')
    name = pattern_str[index_begin + 1: i].strip()
    if pattern_str[i] == ':':
        if check_dup_name(name, slot_defs):
            raise ValueError(f"Duplicate name {name} for slot definition at position: {pattern_str[index_begin:]}")
        i = i + 1
        while pattern_str[i].isspace():
            i = i + 1
        if pattern_str[i] == '@':
            udf_function, i = parse_function(pattern_str, i, dialect, slot_defs)
            value_slot = ValueSlot(name, None, udf_function)
        else:
            slot_type, i = parse_type(pattern_str, i)
            value_slot = ValueSlot(name, slot_type)
        slot_defs[len(slot_defs) - 1].append(value_slot)
    else:
        value_slot = get_slot_by_name(name, slot_defs)
    while pattern_str[i].isspace():
        i = i + 1
    assert pattern_str[i] == '>'
    return value_slot, i + 1


function_name = set()


def parse_function(pattern_str: str, index_begin: int, dialect: str, slot_defs: List) -> tuple[UdfFunction, int]:
    assert pattern_str[index_begin] == '@'
    name = ''
    i = index_begin + 1
    while pattern_str[i] != '(':
        name = name + pattern_str[i]
        i = i + 1
    name = name.strip()
    assert pattern_str[i] == '('
    slots = []
    i = i + 1
    while pattern_str[i].isspace():
        i = i + 1
    while pattern_str[i] != ')':
        if pattern_str[i] == '\'':
            cur_str = ''
            i = i + 1
            while pattern_str[i] != '\'':
                if pattern_str[i] == '\\' and pattern_str[i + 1] == '\'':
                    cur_str = cur_str + '\''
                    i = i + 2
                else:
                    cur_str = cur_str + pattern_str[i]
                    i = i + 1
            i = i + 1
            slots.append(StringLiteralSlot(cur_str))
        elif pattern_str[i].isdigit():
            num = 0
            while pattern_str[i].isdigit():
                num = num * 10 + int(pattern_str[i])
                i = i + 1
            val = 0.1
            if pattern_str == '.':
                while pattern_str[i].isdigit():
                    num = num + int(pattern_str[i]) * val
                    val = val * 0.1
                    i = i + 1
            i = i + 1
            slots.append(NumberLiteralSlot(num))
        else:
            slot, i = parse_slot(pattern_str, i, dialect, slot_defs)
            slots.append(slot)
        while pattern_str[i].isspace():
            i = i + 1
        if pattern_str[i] == ',':
            i = i + 1
            while pattern_str[i].isspace():
                i = i + 1
        else:
            assert pattern_str[i] == ')'
    function_name.add(name)
    return UdfFunction(name, slots), i + 1


def parse_type(pattern_str: str, index_begin: int) -> tuple[BaseType, int]:
    # considering OPTION
    i = index_begin
    while pattern_str[i].isspace():
        i = i + 1
    type_def_begin = i
    # !LIST type and OPTION type have no attributes
    if pattern_str[type_def_begin:].startswith('LIST'):
        assert pattern_str[type_def_begin + len('LIST')] == '['
        i = find_same_layer_bracket(pattern_str, type_def_begin + len('LIST'), '[')
        element_type, _ = parse_type(pattern_str[type_def_begin + len('LIST') + 1: i], 0)
        while pattern_str[i].isspace():
            i = i + 1
        assert pattern_str[i] == ']'
        return ListType(element_type), i + 1
    elif pattern_str[type_def_begin:].startswith('OPTION'):
        assert pattern_str[type_def_begin + + len('OPTION')] == '['
        # parse option values
        i = type_def_begin + len('OPTION')
        option_map = {}
        while pattern_str[i] != ']':
            # TODO:change here to enable number input
            i = i + 1
            key_str, i = load_str(pattern_str, i)
            while pattern_str[i].isspace():
                i = i + 1
            assert pattern_str[i] == ',' or pattern_str[i] == ':' or pattern_str[i] == ']'
            if pattern_str[i] == ',' or pattern_str[i] == ']':
                value_str = key_str
            else:
                i = i + 1
                value_str, i = load_str(pattern_str, i)
            option_map[key_str] = value_str
            while pattern_str[i].isspace():
                i = i + 1
            assert pattern_str[i] == ',' or pattern_str[i] == ']'
        return OptionType(option_map), i + 1
    else:
        while pattern_str[i].isspace():
            i = i + 1
        type_name = ''
        in_paren = False
        while i < len(pattern_str) and not in_paren and not (pattern_str[i] == '>' or pattern_str[i] == ','):
            if pattern_str[i] == '(':
                in_paren = True
            type_name = type_name + pattern_str[i]
            i = i + 1
        attr_container = None
        if i < len(pattern_str) and pattern_str[i] == ',':
            i = i + 1
            attr_container, i = parse_attributes(pattern_str, i)

        return gen_type_through_str(type_name.strip(), attr_container), i


attributes_set = set()


def parse_attributes(pattern_str: str, index_begin: int) -> tuple[AttributeContainer, int]:
    i = index_begin
    while pattern_str[i].isspace():
        i = i + 1
    in_paren = False
    attributes = []
    attribute = ''
    attr_container = AttributeContainer()
    while i < len(pattern_str) and not (not in_paren and pattern_str[i] == '>'):
        if in_paren is False and pattern_str[i] in ['(', '[']:
            in_paren = True
        if in_paren is False and pattern_str[i] == ',':
            if attribute == '':
                raise ValueError('Empty attribute is not allowed: ' + pattern_str[i:])
            attributes.append(attribute.strip())
            attribute = ''
        else:
            if in_paren is not None and pattern_str[i] in [')', ']']:
                in_paren = False
            attribute = attribute + pattern_str[i]
        i = i + 1
    if i == index_begin:
        raise ValueError(f"Parsing Attributes Error: {pattern_str[index_begin:]}")
    if attribute != '':
        attributes.append(attribute.strip())
    for attribute in attributes:
        attr_container.add_attribute(attribute)
        attributes_set.add(attribute)
    return attr_container, i
