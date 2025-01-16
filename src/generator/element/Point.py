# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Point.py$
# @Author: 10379
# @Time: 2025/1/10 22:54
from typing import List

from generator.element.Pattern import ValueSlot, Pattern


class Point:
    def __init__(self, src_pattern: Pattern, tgt_pattern: Pattern, slots: List, point_type: str):
        self.src_pattern = src_pattern
        self.tgt_pattern = tgt_pattern
        self.slots = slots
        self.point_type = point_type

    def instr_full_content(self, col_pairs: List):
        for value_slot in self.slots:
            assert isinstance(value_slot, ValueSlot)
            value_slot.prefill(col_pairs)
        return self.src_pattern.fulfill_pattern(True), self.tgt_pattern.fulfill_pattern(False)
