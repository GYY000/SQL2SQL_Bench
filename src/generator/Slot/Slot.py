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
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Value]"

    def __repr__(self):
        return str(self)


class IntSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: INT]"

    def __repr__(self):
        return str(self)


class FloatSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Float]"

    def __repr__(self):
        return str(self)


class DateSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Date]"

    def __repr__(self):
        return str(self)


class JsonSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Json]"

    def __repr__(self):
        return str(self)


class TimeStampSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Timestamp]"

    def __repr__(self):
        return str(self)


class TextSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Text]"

    def __repr__(self):
        return str(self)


class BoolSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Bool]"

    def __repr__(self):
        return str(self)


class PointSlot(ValueSlot):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"[{self.name}: Point]"

    def __repr__(self):
        return str(self)
