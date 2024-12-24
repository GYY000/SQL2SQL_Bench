# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: ForSlot$
# @Author: 10379
# @Time: 2024/12/25 0:24
from generator.Slot import Slot
from generator.Slot.Slot import SlotType


class ForSlot(Slot):
    def __init__(self):
        super().__init__(SlotType.FOR_SLOT)
