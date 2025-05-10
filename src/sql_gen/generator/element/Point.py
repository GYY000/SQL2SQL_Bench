# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Point.py$
# @Author: 10379
# @Time: 2025/1/10 22:54
from typing import List

from sql_gen.generator.ele_type.type_def import BaseType
from sql_gen.generator.element.Pattern import Pattern, ValueSlot


class Point:
    def __init__(self, src_pattern: Pattern, tgt_pattern: Pattern, slots: List, point_type: str, return_type: BaseType | None, predicate: str | None):
        self.src_pattern = src_pattern
        self.tgt_pattern = tgt_pattern
        self.slots = slots
        self.point_type = point_type
