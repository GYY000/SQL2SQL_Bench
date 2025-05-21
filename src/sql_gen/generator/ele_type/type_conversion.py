# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: type_conversion$
# @Author: 10379
# @Time: 2025/4/1 22:36
from sql_gen.generator.ele_type.type_def import *


# Translate execution type of each dialect into our intermediate Type representation

def type_mapping(dialect: str, op_type: str) -> BaseType:
    # return BaseType(type, dialect)
    # TODO: change the json file can also work
    if dialect == 'mysql':
        if op_type in ['DECIMAL', 'NEWDECIMAL']:
            assert op_type == 'NEWDECIMAL'
            return FloatGeneralType()
        if op_type == 'DOUBLE':
            return DoubleType()
        elif op_type in ['SHORT', 'LONG', 'LONGLONG', 'INT24']:
            return IntType()
        elif op_type == 'TINY':
            return BoolType()
        elif op_type == 'GEOMETRY':
            return PointType()
        elif op_type in ["DATE", "NEWDATE"]:
            return DateType()
        elif op_type == 'TIME':
            return TimeType()
        elif op_type == 'DATETIME':
            return DatetimeType()
        elif op_type == 'TIMESTAMP':
            return TimestampType()
        elif op_type == 'YEAR':
            return YearType()
        elif op_type in ['VARCHAR', 'VAR_STRING', 'STRING']:
            return VarcharType()
        elif op_type in ['BLOB', 'MEDIUM_BLOB', 'TINY_BLOB', 'LONG_BLOB']:
            return BlobType()
        elif op_type == 'JSON':
            return JsonType()
        elif op_type == 'NULL':
            return NullType()
        else:
            raise ValueError(f"MySQL Type {op_type} is not supported yet")
    elif dialect == 'pg':
        if op_type.startswith('_'):
            ele_type = type_mapping(dialect, op_type[1:])
            return ArrayType(ele_type)
        if op_type == 'uuid':
            return UuidType()
        elif op_type == 'bytea':
            return BlobType()
        elif op_type == 'jsonb':
            return JsonbType()
        elif op_type == 'varchar':
            return VarcharType()
        elif op_type == 'float8':
            return DoubleType()
        elif op_type == 'numeric' or op_type == 'float4':
            return FloatGeneralType()
        elif op_type == 'point':
            return PointType()
        elif op_type == 'bool':
            return BoolType()
        elif op_type == 'text':
            return TextType()
        elif op_type == 'char':
            return StringGeneralType()
        elif op_type in ["int8", "int2", "int4"]:
            return IntType()
        elif op_type == 'timestamp':
            return TimestampType()
        elif op_type == 'time':
            return TimeType()
        elif op_type == 'date':
            return DateType()
        elif op_type == 'json':
            return JsonType()
        elif op_type == 'xml':
            return XmlType()
        else:
            raise ValueError(f"PG Type {op_type} is not supported yet")
    elif dialect == 'oracle':
        if op_type == 'NUMBER':
            return NumberType()
        if op_type == 'CHAR':
            return StringGeneralType()
        raise ValueError(f"Oracle Type {op_type} is not supported yet")
    else:
        raise ValueError(f"dialect {dialect} is not supported")
