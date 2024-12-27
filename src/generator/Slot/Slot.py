# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Slot$
# @Author: 10379
# @Time: 2024/12/25 0:18

from enum import Enum

from generator.Operand import Operand
from generator.Type.Type import Type


class SlotType(Enum):
    value = 0
    FOR_SLOT = 1
    ACTION_SLOT = 2


class Slot:
    def __init__(self):
        pass

    def __str__(self):
        return 'slot'


class ValueSlot(Slot):
    def __init__(self, name: str, slot_type: Type):
        super().__init__()
        self.name = name
        self.slot_type = slot_type

    def __str__(self):
        return f"[{self.name}: {str(self.slot_type)}]"
