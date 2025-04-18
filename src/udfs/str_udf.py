# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: str_udf$
# @Author: 10379
# @Time: 2025/4/13 21:28
from antlr_parser.Tree import TreeNode
from sql_gen.generator.ele_type.type_def import StringGeneralType
from sql_gen.generator.element.Operand import Operand


def reg_escape(op: Operand):
    ori_str = op.value
    special_chars = r".^$*+?{}[]\|()"
    escaped_string = ''.join(
        '\\' + char if char in special_chars else char
        for char in ori_str
    )
    return escaped_string


def rtrim_reg(op: Operand):
    reg_str = reg_escape(op)
    return f"'[{reg_str}]+$'"


def ltrim_reg(op: Operand):
    reg_str = reg_escape(op)
    return f"'^[{reg_str}]+'"


def gen_hex_string():
    import random
    length = random.randint(4, 10)
    random_bytes = random.getrandbits(length * 4)
    hex_string = f"{random_bytes:0{length}x}"
    return Operand(hex_string, StringGeneralType())


def add_escape():
    pass


def gen_trans_str():
    pass


def gen_nested_replace():
    pass


def gen_reg_pattern():
    pass


def gen_xml_path():
    pass


def gen_count_literal():
    pass
