# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Pattern$
# @Author: 10379
# @Time: 2024/12/25 0:13
from typing import List

from generator.Slot.Slot import ValueSlot, Slot


class Pattern:
    def __init__(self):
        self.elements = []
        self.slots = []
        self.value_slots = {}
        self.func_slots = []
        self.for_slots = []

    def add_keyword(self, keyword: str):
        self.elements.append(keyword)

    def set_or_get_value_slot(self, slot: ValueSlot):
        if slot.name in self.value_slots:
            return self.value_slots[slot.name]
        else:
            self.slots.append(slot)
            self.value_slots[slot.name] = slot
            return slot

    def add_slot(self, slot: Slot) -> Slot:
        if isinstance(slot, ForSlot):
            self.elements.append(slot)
            self.for_slots.append(slot)
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
            self.func_slots.append(slot)
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


class ForSlot(Slot):
    def __init__(self, pattern: Pattern, ele_names: List[str], slots: List[Slot]):
        super().__init__()
        self.slots = []
        self.pattern = pattern
        self.ele_names = ele_names
        self.slots = slots

    def __str__(self):
        # TODO:
        return "For loop"


class FunctionSlot(Slot):
    def __init__(self, func_name: str, slots: List[Slot], func_def: str = None):
        super().__init__()
        self.slots = slots
        self.func_def = func_def
        self.func_name = func_name

    def add_func_def(self, func_def: str):
        self.func_def = func_def

    def __str__(self):
        params = ""
        for slot in self.slots:
            if params != '':
                params = params + ", "
            params = params + str(slot)
        return f"{self.func_name}({params.strip()})"
