# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Point.py$
# @Author: 10379
# @Time: 2025/1/10 22:54
from typing import List


class Point:
    def __init__(self, src_pattern, tgt_pattern, slots: List, point_type: str):
        self.src_pattern = src_pattern
        self.tgt_pattern = tgt_pattern
        self.slots = slots
        self.point_type = point_type
