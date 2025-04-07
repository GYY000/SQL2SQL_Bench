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
        """
  "19": "name",
  "22": "int2vector",
  "24": "regproc",
  "26": "oid",
  "27": "tid",
  "28": "xid",
  "29": "cid",
  "30": "oidvector",
  "71": "pg_type",
  "75": "pg_attribute",
  "81": "pg_proc",
  "83": "pg_class",
  "194": "pg_node_tree",
  "3361": "pg_ndistinct",
  "3402": "pg_dependencies",
  "5017": "pg_mcv_list",
  "601": "lseg",
  "602": "path",
  "603": "box",
  "604": "polygon",
  "628": "line",
  "705": "unknown",
  "718": "circle",
  "790": "money",
  "829": "macaddr",
  "869": "inet",
  "650": "cidr",
  "774": "macaddr8",
  "1033": "aclitem",
  "1083": "time",
  "1184": "timestamptz",
  "1186": "interval",
  "1266": "timetz",
  "1560": "bit",
  "1562": "varbit",
  "1790": "refcursor",
  "2202": "regprocedure",
  "2203": "regoper",
  "2204": "regoperator",
  "2205": "regclass",
  "2206": "regtype",
  "4096": "regrole",
  "4089": "regnamespace",
  "3220": "pg_lsn",
  "3614": "tsvector",
  "3642": "gtsvector",
  "3615": "tsquery",
  "3734": "regconfig",
  "3769": "regdictionary",
  "4072": "jsonpath",
  "2970": "txid_snapshot",
  "3904": "int4range",
  "3906": "numrange",
  "3908": "tsrange",
  "3910": "tstzrange",
  "3912": "daterange",
  "3926": "int8range",
  "2249": "record",
  "2287": "_record",
  "2275": "cstring",
  "2276": "any",
  "2277": "anyarray",
  "2278": "void",
  "2281": "internal",
  "2282": "opaque",
  "2283": "anyelement",
  "2776": "anynonarray",
  "3500": "anyenum",
  "3831": "anyrange",
  "1000": "_bool",
  "1001": "_bytea",
  "1002": "_char",
  "1003": "_name"
        """
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
        pass
    else:
        raise ValueError(f"dialect {dialect} is not supported")
