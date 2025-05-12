# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Attribute$
# @Author: 10379
# @Time: 2025/5/3 10:02
from abc import ABC


class AttributeContainer:
    def __init__(self):
        self.attributes = []

    def has_literal(self):
        for attr in self.attributes:
            if attr == 'LITERAL':
                return True
        return False

    def has_column(self):
        for attr in self.attributes:
            if attr == 'COLUMN':
                return True
        return False

    def has_group_by(self):
        for attr in self.attributes:
            if attr == 'GROUP_BY':
                return True
        return False

    def has_range(self):
        for attr in self.attributes:
            if attr['attr_name'] == 'range':
                return True
        return False

    def get_range(self):
        for attr in self.attributes:
            if attr['attr_name'] == 'range':
                return attr
        return None

    def add_attribute(self, attr_name: str | dict):
        if attr_name in ['LITERAL', 'COLUMN', 'GROUP_BY']:
            self.attributes.append(attr_name)
        else:
            # range attribute
            # only NUMBER now
            range_attribute = None
            for attr in self.attributes:
                if attr == 'range':
                    range_attribute = attr
            if range_attribute is None:
                range_attribute = {
                    'attr_name': 'range',
                    'ranges': []
                }
                self.attributes.append(range_attribute)
            i = 0
            while attr_name[i].isspace():
                i += 1
            if attr_name[i] == '(':
                margin_left = False
            else:
                assert attr_name[i] == '['
                margin_left = True
            i += 1
            attr_name = attr_name[i:]
            i = 0
            while attr_name[-1].isspace():
                attr_name = attr_name[:-1]
            if attr_name[-1] == ')':
                margin_right = False
            else:
                assert attr_name[-1] == ']'
                margin_right = True
            num_lowest = 0
            if attr_name[i] == '-':
                sign = -1
                i += 1
            elif attr_name[i] == '+':
                sign = 1
                i += 1
            else:
                sign = 1
            if attr_name[i] == '∞':
                margin_left = None
            while attr_name[i].isdigit():
                num_lowest = num_lowest * 10 + int(attr_name[i])
                i += 1
            num_lowest *= sign
            while attr_name[i].isspace():
                i += 1
            assert attr_name[i] == ','
            i += 1
            while attr_name[i].isspace():
                i += 1
            if attr_name[i] == '-':
                sign = -1
                i += 1
            elif attr_name[i] == '+':
                sign = 1
                i += 1
            else:
                sign = 1
            num_highest = 0
            if attr_name[i] == '∞':
                margin_right = None
            else:
                assert attr_name[i].isdigit()
                while attr_name[i].isdigit():
                    num_highest = num_highest * 10 + int(attr_name[i])
                    i += 1
                num_highest *= sign
                assert num_lowest <= num_highest
            if margin_left is None and margin_right is None:
                return
            elif margin_left is None:
                range_attribute['ranges'].append({
                    'margin_right': margin_right,
                    'num_highest': num_highest,
                })
            elif margin_right is None:
                range_attribute['ranges'].append({
                    'margin_left': margin_left,
                    'num_lowest': num_lowest,
                })
            else:
                range_attribute['ranges'].append({
                    'margin_left': margin_left,
                    'num_lowest': num_lowest,
                    'margin_right': margin_right,
                    'num_highest': num_highest
                })
