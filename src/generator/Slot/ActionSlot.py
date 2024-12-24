# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: ActionSlot$
# @Author: 10379
# @Time: 2024/12/25 0:26
from generator.Slot.Slot import Slot, SlotType


class ActionSlot(Slot):
    def __init__(self):
        super().__init__(SlotType.ACTION_SLOT)