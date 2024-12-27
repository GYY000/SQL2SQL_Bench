# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Pattern$
# @Author: 10379
# @Time: 2024/12/25 0:13
from generator.Slot.ForSlot import ForSlot
from generator.Slot.FunctionSlot import FunctionSlot
from generator.Slot.Slot import Slot, ValueSlot


class Pattern:
    def __init__(self):
        self.elements = []
        self.slots = []
        self.ori_slot_set = {}

    def add_keyword(self, keyword: str):
        self.elements.append(keyword)

    def set_or_get_value_slot(self, slot: ValueSlot):
        if slot.name in self.ori_slot_set:
            return self.ori_slot_set[slot.name]
        else:
            self.slots.append(slot)
            self.ori_slot_set[slot.name] = slot
            return slot

    def add_slot(self, slot: Slot) -> Slot:
        if isinstance(slot, ForSlot):
            self.elements.append(slot)
            self.slots.append(slot)
            temp_slots = []
            for for_slot in slot.slots:
                if isinstance(for_slot, ValueSlot):
                    temp_slots.append(self.set_or_get_value_slot(for_slot))
                else:
                    temp_slots.append(for_slot)
            slot.slots = temp_slots
            return slot
        elif isinstance(slot, FunctionSlot):
            self.elements.append(slot)
            self.slots.append(slot)
            temp_slots = []
            for func_slot in slot.slots:
                if isinstance(func_slot, ValueSlot):
                    temp_slots.append(self.set_or_get_value_slot(func_slot))
                else:
                    temp_slots.append(func_slot)
            slot.slots = temp_slots
            return slot
        elif isinstance(slot, ValueSlot):
            slot = self.set_or_get_value_slot(slot)
            self.elements.append(slot)
            return slot
