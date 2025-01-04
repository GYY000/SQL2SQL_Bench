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


def gen_type(src_dialect: str, value_type: str) -> Type:
    if src_dialect == 'mysql':
        if value_type == 'VALUE':
            return MySQLType.ANY_VALUE
        elif value_type == 'INT':
            return MySQLType.INT
        elif value_type == 'BOOL':
            return MySQLType.BOOL
        elif value_type == 'FLOAT':
            return MySQLType.FLOAT
        elif value_type == 'DATE':
            return MySQLType.DATE
        elif value_type == 'TIME':
            return MySQLType.TIME
        elif value_type == 'TIMESTAMP':
            return MySQLType.TIMESTAMP
        elif value_type == 'TEXT':
            return MySQLType.TEXT
        elif value_type == 'JSON':
            return MySQLType.JSON
        elif value_type == 'POINT':
            return MySQLType.POINT
    elif src_dialect == 'pg':
        if value_type == 'VALUE':
            return PostgresType.ANY_VALUE
        elif value_type == 'INT':
            return PostgresType.INT
        elif value_type == 'BOOL':
            return PostgresType.BOOL
        elif value_type == 'FLOAT':
            return PostgresType.FLOAT
        elif value_type == 'DATE':
            return PostgresType.DATE
        elif value_type == 'TIME':
            return PostgresType.TIME
        elif value_type == 'TIMESTAMP':
            return PostgresType.TIMESTAMP
        elif value_type == 'TIMESTAMPTZ':
            return PostgresType.TIMESTAMP_TZ
        elif value_type == 'TEXT':
            return PostgresType.TEXT
        elif value_type == 'UUID':
            return PostgresType.UUID
        elif value_type == 'JSON':
            return PostgresType.JSON
        elif value_type == 'POINT':
            return PostgresType.POINT
    elif src_dialect == 'oracle':
        if value_type == 'VALUE':
            return OracleType.ANY_VALUE
        elif value_type == 'NUMBER':
            return OracleType.NUMBER
        elif value_type == 'DATE':
            return OracleType.DATE
        elif value_type == 'TIMESTAMP':
            return OracleType.TIMESTAMP
        elif value_type == 'TIMESTAMPTZ':
            return OracleType.TIMESTAMP_TZ
        elif value_type == 'VARCHAR2':
            return OracleType.VARCHAR2
        elif value_type == 'SDO_GEOMETRY':
            return OracleType.SDO_GEOMETRY
    else:
        raise ValueError(f"Type {value_type} does not exist in {src_dialect} of this system")
