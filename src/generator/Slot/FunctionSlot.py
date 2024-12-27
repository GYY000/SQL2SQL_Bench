# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: FunctionSlot$
# @Author: 10379
# @Time: 2024/12/25 0:26
from typing import List

from generator.Slot.Slot import Slot, SlotType


class FunctionSlot(Slot):
    def __init__(self, func_name: str, slots: List[Slot], func_def: str = None):
        super().__init__()
        self.slots = slots
        self.func_def = func_def
        self.func_name = func_name

    def add_func_def(self, func_def: str):
        self.func_def = func_def
