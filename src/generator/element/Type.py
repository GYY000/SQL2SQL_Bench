# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Type$
# @Author: 10379
# @Time: 2024/12/26 19:50
from enum import Enum

from enum import Enum


class Type:
    pass


class MySQLType(Type, Enum):
    ANY_VALUE = 0
    INT = 1
    BOOL = 2
    FLOAT = 3
    DATE = 4
    TIME = 5
    TIMESTAMP = 6
    TEXT = 7
    JSON = 8
    POINT = 9


class PostgresType(Type, Enum):
    ANY_VALUE = 0
    INT = 1
    BOOL = 2
    FLOAT = 3
    DATE = 4
    TIME = 5
    TIMESTAMP = 6
    TIMESTAMP_TZ = 7
    TEXT = 8
    UUID = 9
    JSON = 10
    POINT = 11


class OracleType(Type, Enum):
    ANY_VALUE = 0
    NUMBER = 1
    DATE = 2
    TIMESTAMP = 3
    TIMESTAMP_TZ = 4
    VARCHAR2 = 5
    SDO_GEOMETRY = 6


class ListType(Type):
    def __init__(self, ele_type: Type):
        super().__init__()
        self.ele_type = ele_type

    def __str__(self):
        return f"List[{self.ele_type}]"

    def __repr__(self):
        return str(self)
