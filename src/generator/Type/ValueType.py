# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: ValueType$
# @Author: 10379
# @Time: 2024/12/26 21:15
from generator.Type.Type import Type


class ValueType(Type):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return 'Value'


class DateType(ValueType):
    def __init__(self):
        super().__init__()


class IntType(ValueType):
    def __init__(self):
        super().__init__()


class FloatType(ValueType):
    def __init__(self):
        super().__init__()


class TextType(ValueType):
    def __init__(self):
        super().__init__()


class JsonType(ValueType):
    def __init__(self):
        super().__init__()


class TimeStampType(ValueType):
    def __init__(self):
        super().__init__()


class PointType(ValueType):
    def __init__(self):
        super().__init__()
