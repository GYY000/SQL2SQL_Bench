# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: point_parser$
# @Author: 10379
# @Time: 2024/12/25 12:40
from typing import List, Dict

from sql_gen.generator.element.Pattern import Pattern, ForSlot, Slot, UdfFunction, ValueSlot
from sql_gen.generator.element.Point import Point
from sql_gen.generator.element.Type import Type, ListType, MySQLType, PostgresType, OracleType, gen_type

slots_defs = [[]]


def get_slot_by_name(name: str):
    i = len(slots_defs) - 1
    while i >= 0:
        for j in range(len(slots_defs[i])):
            if name == slots_defs[i][j].name:
                return slots_defs[j][j]
        i = i - 1
    raise ValueError(f"can't find var def of {name}")


def parse_point(point: Dict, src_dialect: str, tgt_dialect: str) -> Point:
    src_pattern = point[src_dialect]
    tgt_pattern = point[tgt_dialect]

    slots_ori_def = point['slots_def']
    for ori_def in slots_ori_def:
        name = ori_def['name']
        src_type = ori_def[f"{src_dialect}_type"]
        tgt_type = ori_def[f"{tgt_dialect}_type"]
        if "gen_func" in ori_def:
            pass
        # TODO: check for unique
        slots_defs[0].append(ValueSlot(name, gen_type(src_dialect, src_type), gen_type(tgt_dialect, tgt_type)))

    splits = split(src_pattern)
    src_pattern, _ = parse_pattern(splits, 0, len(splits), src_dialect)

    splits = split(tgt_pattern)
    tgt_pattern, _ = parse_pattern(splits, 0, len(splits), tgt_dialect)

    return Point(src_pattern, tgt_pattern, slots_defs[0], point['type'])


def split(pattern: str):
    res = []
    i = 0
    cur_str = ""
    while i < len(pattern):
        if pattern[i] == '\"':
            if cur_str != '':
                res.append(cur_str)
            cur_str = '\"'
            i = i + 1
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
            cur_str = '\''
            i = i + 1
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
        elif pattern[i:].startswith('::'):
            if cur_str != '':
                res.append(cur_str)
                cur_str = ''
            res.append("::")
            i = i + 2
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
    if cur_str != '':
        res.append(cur_str)
    return res


def parse_pattern(tokens: List[str], index_begin: int, index_end: int, src_dialect: str) -> tuple[Pattern, int]:
    i = index_begin
    pattern = Pattern()
    while i < index_end:
        token = tokens[i]
        if token == '[':
            slot, i = parse_slot(tokens, i, index_end, src_dialect)
            pattern.add_slot(slot)
        elif token == '{':
            for_slot, i = parse_for_loop(tokens, i, index_end, src_dialect)
            pattern.add_slot(for_slot)
        else:
            pattern.add_keyword(tokens[i])
            i = i + 1
    return pattern, i


def parse_for_loop(tokens: List[str], index_begin: int, index_end: int, src_dialect: str) -> tuple[ForSlot, int]:
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
        slot, i = parse_slot(tokens, i, index_end, src_dialect)
        ele_slots.append(slot)
        assert tokens[i] == ',' or tokens[i] == ':'
    i = i + 1
    # find the corresponding }
    j = i
    flag = 0
    while True:
        if tokens[j] == '{':
            flag = flag + 1
        elif tokens[j] == '}':
            if flag == 0:
                break
            else:
                flag = flag - 1
        j = j + 1

    new_slots = []
    for ele_name in ele_names:
        new_slots.append(ValueSlot(ele_name))
    slots_defs.append(new_slots)
    pattern, i = parse_pattern(tokens, i, j, src_dialect)
    assert tokens[i] == '}'
    slots_defs.pop()
    return ForSlot(pattern, new_slots, ele_slots), i + 1


def parse_slot(tokens: List[str], index_begin: int, index_end: int, src_dialect: str) -> tuple[ValueSlot, int]:
    i = index_begin
    assert tokens[index_begin] == '['
    i = index_begin + 1
    name = ""
    while tokens[i] != ']' and i < index_end:
        name = name + tokens[i] + " "
        i = i + 1
    assert tokens[i] == ']'
    return get_slot_by_name(name.strip()), i + 1
    # slot_type, i = parse_type(tokens, i, index_end, src_dialect)
    # assert tokens[i] == ']'
    # if src_dialect == 'mysql':
    #     assert isinstance(slot_type, MySQLType)
    #     return MySQLValueSlot(name.strip(), slot_type), i + 1
    # elif src_dialect == 'pg':
    #     assert isinstance(slot_type, PostgresType)
    #     return PostgresValueSlot(name.strip(), slot_type), i + 1
    # elif src_dialect == 'oracle':
    #     assert isinstance(slot_type, OracleType)
    #     return OracleValueSlot(name.strip(), slot_type), i + 1
    # else:
    #     assert False


def parse_function(tokens: List[str], index_begin: int, index_end: int, src_dialect: str) -> tuple[UdfFunction, int]:
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
        slot, i = parse_slot(tokens, i, index_end, src_dialect)
        slots.append(slot)
        assert tokens[i] == ',' or tokens[i] == ')'
    i = i + 1
    return UdfFunction(name, slots), i


def parse_type(tokens: List[str], index_begin: int, index_end: int, src_dialect: str) -> tuple[Type, int]:
    i = index_begin
    if tokens[i] == 'List':
        i = i + 1
        assert tokens[i] == '['
        i = i + 1
        ele_type, i = parse_type(tokens, i, index_end, src_dialect)
        assert tokens[i] == ']'
        i = i + 1
        return ListType(ele_type), i
    else:
        return gen_type(src_dialect, tokens[i]), i + 1
