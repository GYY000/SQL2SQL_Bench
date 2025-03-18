# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: Operand$
# @Author: 10379
# @Time: 2024/12/25 0:16
from generator.element.Type import Type, MySQLType, PostgresType, OracleType


class Operand:
    def __init__(self, value: str, op_type: str, dialect: str):
        self.value = value
        self.op_type = type_map(op_type, dialect)

    def __str__(self):
        return f"value: {self.value} type: {self.op_type}"

    def __repr__(self):
        return f"value: {self.value} type: {self.op_type}"


def type_map(op_type: str, dialect: str) -> Type:
    # TODO: change the json file can also work
    if dialect == 'mysql':
        if op_type in ['DECIMAL', 'FLOAT', 'DOUBLE', 'NEWDECIMAL']:
            return MySQLType.FLOAT
        elif op_type in ['SHORT', 'LONG', 'LONGLONG', 'INT24', "BIT"]:
            return MySQLType.INT
        elif op_type == 'TINY':
            return MySQLType.BOOL
        elif op_type == 'GEOMETRY':
            return MySQLType.POINT
        elif op_type in ["DATE", "NEWDATE"]:
            return MySQLType.DATE
        elif op_type == 'TIME':
            return MySQLType.TIME
        elif op_type in ['TIMESTAMP', 'DATETIME']:
            return MySQLType.TIMESTAMP
        elif op_type in ['VAR_STRING', 'BLOB', 'STRING', 'MEDIUM_BLOB', 'TINY_BLOB', 'LONG_BLOB', 'VARCHAR']:
            return MySQLType.TEXT
        elif op_type == 'JSON':
            return MySQLType.JSON
        elif op_type == 'NULL':
            return MySQLType.NULL
        else:
            raise ValueError(f"MySQL Type {op_type} is not supported yet")
    elif dialect == 'pg':
        """
          "22": "int2vector",
          "705": "unknown",
          "1033": "aclitem",
          "1186": "interval",
          "1560": "bit",
          "1562": "varbit",
          "3615": "tsquery",
          "3734": "regconfig",
          "3769": "regdictionary",
          "4072": "jsonpath",
          "2249": "record",
          "2287": "_record",
          "2275": "cstring",
          "2276": "any",
          "2277": "anyarray",
          "2278": "void",
          "2281": "internal",
          "2282": "opaque",
          "3831": "anyrange",
        """
        if op_type == 'uuid':
            return PostgresType.UUID
        elif op_type in ['float4', 'float8', 'numeric', 'NEWDECIMAL']:
            return PostgresType.FLOAT
        elif op_type == 'point':
            return PostgresType.POINT
        elif op_type == 'bool':
            return PostgresType.BOOL
        elif op_type in ['char', 'varchar', 'text']:
            return PostgresType.TEXT
        elif op_type in ["int8", "int2", "int4"]:
            return PostgresType.INT
        elif op_type == 'timestamp':
            return PostgresType.TIMESTAMP
        elif op_type == ['time', 'timetz']:
            return PostgresType.TIME
        elif op_type == 'date':
            return PostgresType.DATE
        elif op_type == 'json':
            return PostgresType.JSON
        elif op_type == 'timestamptz':
            return PostgresType.TIMESTAMP_TZ
        else:
            raise ValueError(f"PG Type {op_type} is not supported yet")
    elif dialect == 'oracle':
        pass
    else:
        raise ValueError(f"dialect {dialect} is not supported")


def op_trans(op: Operand, tgt_type: Type):
    ori_type = op.op_type
    if isinstance(ori_type, MySQLType):
        if ori_type == MySQLType.INT:
            pass
        elif ori_type == MySQLType.TEXT:
            pass
        elif ori_type == MySQLType.BOOL:
            pass
        elif ori_type == MySQLType.TIME:
            pass
        elif ori_type == MySQLType.DATE:
            pass
        elif ori_type == MySQLType.FLOAT:
            pass
        elif ori_type == MySQLType.YEAR:
            pass
        elif ori_type == MySQLType.POINT:
            pass
        elif ori_type == MySQLType.TIMESTAMP:
            pass
        elif ori_type == MySQLType.JSON:
            pass
        elif ori_type == MySQLType.NULL:
            pass
        else:
            raise ValueError(f"Left MySQL Type {ori_type}")
    elif isinstance(op, PostgresType):
        pass
    elif isinstance(op, OracleType):
        pass
