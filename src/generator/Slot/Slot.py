# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Slot$
# @Author: 10379
# @Time: 2024/12/25 0:18

from enum import Enum

from generator.Operand import Operand


class SlotType(Enum):
    value = 0
    FOR_SLOT = 1
    ACTION_SLOT = 2


class Slot:
    def __init__(self, type: SlotType):
        self.type = type

    def match(self, op: Operand):
        return True
