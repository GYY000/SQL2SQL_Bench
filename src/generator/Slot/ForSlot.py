# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: ForSlot$
# @Author: 10379
# @Time: 2024/12/25 0:24
from typing import List

from generator.Pattern import Pattern
from generator.Slot import Slot
from generator.Slot.Slot import SlotType


class ForSlot(Slot):
    def __init__(self, pattern: Pattern, ele_names: List[str], slots: List[Slot]):
        super().__init__()
        self.slots = []

