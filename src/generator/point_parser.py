# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: point_parser$
# @Author: 10379
# @Time: 2024/12/25 12:40
from typing import List

from generator.Pattern import Pattern
from generator.Slot.ForSlot import ForSlot
from generator.Slot.FunctionSlot import FunctionSlot
from generator.Slot.Slot import Slot
from generator.Type.ListType import ListType
from generator.Type.Type import Type
from generator.Type.ValueType import ValueType


def split(pattern: str):
    res = []
    i = 0
    cur_str = ""
    while i < len(pattern):
        if pattern[i] == '\"':
            if cur_str != '':
                res.append(cur_str)
                cur_str = ''
            while pattern[i] != "\"":
                if pattern[i] == '\\':
                    cur_str = cur_str + pattern[i]
                    i = i + 1
                cur_str = cur_str + pattern[i]
                i = i + 1
            cur_str = cur_str + pattern[i]
            i = i + 1
            res.append(cur_str)
            cur_str = ''
        elif pattern[i] == '\'':
            if cur_str != '':
                res.append(cur_str)
                cur_str = ''
            while pattern[i] != "\'":
                if pattern[i] == '\\':
                    cur_str = cur_str + pattern[i]
                    i = i + 1
                cur_str = cur_str + pattern[i]
                i = i + 1
            cur_str = cur_str + pattern[i]
            i = i + 1
            res.append(cur_str)
            cur_str = ''
        elif pattern[i] in ['@', '{', '}', ',', ':', '(', ')', '[', ']']:
            if cur_str != '':
                res.append(cur_str)
                cur_str = ''
            res.append(pattern[i])
            i = i + 1
        elif pattern[i] == ' ' or pattern[i] == '\n' or pattern[i] == '\t':
            if cur_str != '':
                res.append(cur_str)
                cur_str = ''
            i = i + 1
        else:
            cur_str = cur_str + pattern[i]
            i = i + 1
    return res


def parse_pattern(tokens: List[str], index_begin: int, index_end: int) -> tuple[Pattern, int]:
    i = index_begin
    while i < len(tokens):
        token = tokens[i]
        if token == '@':
            parse_function(tokens, i, index_end)
        elif token == '[':
            slot, i = parse_slot(tokens, i, index_end)
        elif token == '{':
            for_slot, i = parse_for_loop(tokens, i, index_end)
        else:
            pass


def parse_for_loop(tokens: List[str], index_begin: int, index_end: int) -> tuple[ForSlot, int]:
    assert tokens[index_begin] == '{'
    # parse_for_loop
    i = index_begin + 1
    while tokens[i] != 'for':
        i = i + 1
    ele_names = []
    i = i + 1
    while tokens[i] != 'in':
        ele_name = tokens[i]
        i = i + 1
        assert tokens[i] == ',' or tokens[i] == 'in'
        ele_names.append(ele_name)
    ele_slots = []
    assert tokens[i] == 'in'

    while tokens[i] != ':':
        i = i + 1
        slot, i = parse_arg_slot(tokens, i, index_end)
        ele_slots.append(slot)
        assert tokens[i] == ',' or tokens[i] == ':'
    i = i + 1
    pattern, i = parse_pattern(tokens, i, index_end)
    assert tokens[i] == '}'
    return ForSlot(pattern, ele_names, ele_slots), i + 1


def parse_arg_slot(tokens: List[str], index_begin: int, index_end: int) -> tuple[Slot, int]:
    i = index_begin
    name = ""
    while tokens[i] != ':' and i < index_end:
        name = name + tokens[i] + " "
        i = i + 1
    assert tokens[i] == ':'
    i = i + 1
    slot_type, i = parse_type(tokens, i, index_end)
    return Slot(name.strip(), slot_type), i


def parse_function(tokens: List[str], index_begin: int, index_end: int) -> tuple[FunctionSlot, int]:
    assert tokens[index_begin] == '@'
    name = ''
    i = index_begin
    while tokens[i] != '(':
        name = name + tokens[i] + " "
    name = name.strip()
    assert tokens[i] == '('
    slots = []
    while tokens[i] != ')':
        i = i + 1
        slot, i = parse_arg_slot(tokens, i, index_end)
        slots.append(slot)
        assert tokens[i] == ',' or tokens[i] == ')'
    i = i + 1
    return FunctionSlot(name, slots), i


def parse_type(tokens: List[str], index_begin: int, index_end: int) -> tuple[Type, int]:
    i = index_begin
    if tokens[i] == 'List':
        i = i + 1
        assert tokens[i] == '['
        i = i + 1
        ele_type, i = parse_type(tokens, i, index_end)
        assert tokens[i] == ']'
        i = i + 1
        return ListType(ele_type), i
    else:
        if tokens[i] == 'value':
            return ValueType(), i


def parse_slot(tokens: List[str], index_begin: int, index_end: int) -> tuple[Slot, int]:
    """
    Slot: [name: type]
    """
    assert tokens[index_begin] == '['
    i = index_begin + 1
    slot, i = parse_arg_slot(tokens, i, index_end)
    assert tokens[i] == ']'
    return slot, i + 1
