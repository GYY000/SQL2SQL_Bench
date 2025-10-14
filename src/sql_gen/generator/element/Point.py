# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Point.py$
# @Author: 10379
# @Time: 2025/1/10 22:54
from typing import List

from sql_gen.generator.ele_type.type_def import BaseType
from sql_gen.generator.element.Pattern import Pattern
from sql_gen.generator.point_type.TranPointType import TranPointType


class Point:
    def __init__(self, name: str, src_dialect, tgt_dialect, src_pattern: Pattern, tgt_pattern: Pattern, slots: List,
                 point_type: TranPointType,
                 return_type: BaseType | None, predicate: str | None, tag: dict | None):
        self.point_name = name
        self.src_dialect = src_dialect
        self.tgt_dialect = tgt_dialect
        self.src_pattern = src_pattern
        self.tgt_pattern = tgt_pattern
        self.slots = slots
        self.point_type = point_type
        self.return_type = return_type
        self.predicate = predicate
        self.tag = tag
