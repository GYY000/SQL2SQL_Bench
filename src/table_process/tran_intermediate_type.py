# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: tran_intermediate_type$
# @Author: 10379
# @Time: 2025/5/26 23:13
import csv
import os
from datetime import datetime

from sql_gen.generator.ele_type.type_def import IntType, PointType, VarcharType, CharType, NvarcharType, XmlType, \
    TimestampTZType, JsonType, IntervalYearMonthType, DateType, TimestampType, BlobType, DecimalType, FloatType, \
    DoubleType, DatetimeType, BoolType, TextType, BigIntType, EnumType, FloatGeneralType, IntGeneralType
from utils.tools import get_data_path


def csv_data_loader(db_name, table_name):
    csv_file_path = os.path.join(get_data_path(), db_name, f'{table_name.lower()}.csv')
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        return list(csv_reader)


def fetch_arg_strs_in_paren(args) -> str:
    return args[args.find('(') + 1: args.find(')')].strip()


def fetch_args(args: str) -> list:
    res = []
    for arg in args.split(','):
        res.append(arg.strip())
    return res


def type_has_attribute(type_def: str, type_name: str) -> bool:
    if len(type_def.strip()) == len(type_name):
        return False
    else:
        return type_def[len(type_name):].strip()[0] == '('


def data_restructure(data: list[dict], ori_cols: list[str]):
    final_json_data = []
    first_row = [col.lower() for col in ori_cols]
    final_json_data.append(first_row)
    for row in data:
        new_row = []
        for key in ori_cols:
            new_row.append(row[key])
        final_json_data.append(new_row)
    return final_json_data


mysql_type_set = set()
oracle_type_set = set()


def type_revising(col_type: str, dialect: str, col_name: str, values: list[dict]):
    ori_col_type = col_type.strip()
    col_type = ori_col_type.upper()
    res_type = None
    if dialect == 'mysql':
        if col_type.startswith('INT'):
            mysql_type_set.add('INT')
            res_type = IntType()
        elif col_type.startswith('BIGINT'):
            mysql_type_set.add('BIGINT')
            res_type = BigIntType()
        elif col_type.startswith('VARCHAR'):
            mysql_type_set.add('VARCHAR')
            if not type_has_attribute(col_type, 'VARCHAR'):
                assert False
            else:
                res_type = VarcharType(int(fetch_args(fetch_arg_strs_in_paren(col_type))[0]))
        elif col_type.startswith('DATETIME'):
            mysql_type_set.add('DATETIME')
            if not type_has_attribute(col_type, 'DATETIME'):
                res_type = DatetimeType()
            else:
                res_type = DatetimeType(int(fetch_args(fetch_arg_strs_in_paren(col_type))[0]))
        elif col_type.startswith('TINYINT'):
            mysql_type_set.add('TINYINT')
            assert type_has_attribute(col_type, 'TINYINT')
            assert int(fetch_args(fetch_arg_strs_in_paren(col_type))[0]) == 1
            res_type = BoolType()
        elif col_type.startswith('TEXT') or col_type.startswith('MEDIUMTEXT'):
            mysql_type_set.add('TEXT')
            res_type = TextType()
        elif col_type.startswith('DOUBLE'):
            mysql_type_set.add('DOUBLE')
            res_type = DoubleType()
        elif col_type.startswith('FLOAT'):
            mysql_type_set.add('FLOAT')
            res_type = FloatType()
        elif col_type.startswith('ENUM'):
            mysql_type_set.add('ENUM')
            type_def_values = fetch_args(fetch_arg_strs_in_paren(ori_col_type))
            enum_values = []
            for value in type_def_values:
                enum_values.append(value.strip("'"))
            res_type = EnumType(enum_values)
        elif col_type.startswith('DECIMAL'):
            mysql_type_set.add('DECIMAL')
            args = fetch_args(fetch_arg_strs_in_paren(col_type))
            assert len(args) == 2
            res_type = DecimalType(int(args[0]), int(args[1]))
    elif dialect == 'pg':
        pass
    elif dialect == 'oracle':
        if col_type.startswith('SDO_GEOMETRY'):
            oracle_type_set.add('SDO_GEOMETRY')
            res_type = PointType()
        elif col_type.startswith('VARCHAR2'):
            oracle_type_set.add('VARCHAR2')
            if not type_has_attribute(col_type, 'VARCHAR2'):
                res_type = VarcharType(None)
            else:
                res_type = VarcharType(int(fetch_args(fetch_arg_strs_in_paren(col_type))[0]))
        elif col_type.startswith('CHAR'):
            oracle_type_set.add('CHAR')
            if not type_has_attribute(col_type, 'CHAR'):
                res_type = CharType(None)
            else:
                res_type = CharType(int(fetch_args(fetch_arg_strs_in_paren(col_type))[0]))
        elif col_type.startswith('NVARCHAR2'):
            oracle_type_set.add('NVARCHAR2')
            if not type_has_attribute(col_type, 'NVARCHAR2'):
                res_type = NvarcharType(1000)
            else:
                res_type = NvarcharType(int(fetch_args(fetch_arg_strs_in_paren(col_type))[0]))
        elif col_type.startswith('NUMBER'):
            oracle_type_set.add('NUMBER')
            if not type_has_attribute(col_type, 'NUMBER'):
                # TODO: check if it is a float or int
                res_type = IntType()
            else:
                args = fetch_args(fetch_arg_strs_in_paren(col_type))
                if len(args) == 1:
                    res_type = IntType()
                else:
                    assert len(args) == 2
                    res_type = DecimalType(int(args[0]), int(args[1]))
        elif col_type.startswith('BINARY_FLOAT'):
            oracle_type_set.add('BINART_FLOAT')
            res_type = FloatType()
        elif col_type.startswith('BINARY_DOUBLE'):
            oracle_type_set.add('BINARY_DOUBLE')
            res_type = DoubleType()
        elif col_type.startswith('XMLTYPE'):
            oracle_type_set.add('XMLTYPE')
            res_type = XmlType()
        elif col_type.startswith('TIMESTAMP WITH TIME ZONE'):
            oracle_type_set.add('TIMESTAMP WITH TIME ZONE')
            res_type = TimestampTZType()
        elif col_type.startswith('JSON'):
            oracle_type_set.add('JSON')
            res_type = JsonType()
        elif col_type.startswith('INTERVAL YEAR TO MONTH'):
            oracle_type_set.add('INTERVAL YEAR TO MONTH')
            res_type = IntervalYearMonthType()
        elif col_type.startswith('DATE'):
            oracle_type_set.add('DATE')
            res_type = DateType()
        elif col_type.startswith('TIMESTAMP'):
            oracle_type_set.add('TIMESTAMP')
            if not type_has_attribute(col_type, 'TIMESTAMP'):
                res_type = TimestampType()
            else:
                args = fetch_args(fetch_arg_strs_in_paren(col_type))
                assert len(args) == 1
                if len(args) == 1:
                    res_type = TimestampType(int(args[0]))
        elif col_type.startswith('BLOB'):
            res_type = BlobType()
    if res_type is None:
        print(col_type)
        print(dialect)
        assert False
    ori_type = res_type
    revised_type = value_check(res_type, dialect, col_name, values)
    cnt = 0
    while revised_type is None:
        if cnt == 0:
            if not isinstance(col_type, IntGeneralType):
                res_type = IntType()
                revised_type = value_check(res_type, dialect, col_name, values)
        elif cnt == 1:
            if not isinstance(col_type, FloatGeneralType):
                res_type = DoubleType()
                revised_type = value_check(res_type, dialect, col_name, values)
        elif cnt == 2:
            if not isinstance(col_type, DateType):
                res_type = DateType()
                revised_type = value_check(res_type, dialect, col_name, values)
        elif cnt == 3:
            if not isinstance(col_type, DatetimeType):
                res_type = DatetimeType()
                revised_type = value_check(res_type, dialect, col_name, values)
        elif cnt == 4:
            if not isinstance(col_type, VarcharType):
                res_type = VarcharType()
                revised_type = value_check(res_type, dialect, col_name, values)
        cnt = cnt + 1
    assert revised_type is not None
    if revised_type != ori_type:
        print(f"Revised {col_name} type from {ori_type} to {revised_type}")
    return revised_type


"""
{'FLOAT', 'DECIMAL', 'BIGINT', 'INT', 'ENUM', 'DATETIME', 'TINYINT', 'VARCHAR', 'TEXT'}
{'BINARY_DOUBLE', 'VARCHAR2', 'CHAR', 'NUMBER', 'DATE'}
"""


def value_check(col_type, dialect, key, values: list[dict]):
    # TODO: TO BE FURTHER accomplished for other parameter
    flag = True
    for value in values:
        if value[key] == '':
            value[key] = None
    if isinstance(col_type, IntGeneralType):
        for value in values:
            used_value = value[key]
            if used_value is None:
                continue
            try:
                int(used_value)
            except ValueError:
                print(used_value)
                flag = False
                break
        if flag:
            for value in values:
                if value[key] is not None:
                    value[key] = int(value[key])
    elif isinstance(col_type, BoolType):
        for value in values:
            if value[key] is None:
                continue
            if value[key] not in ['1', '0', 'true', 'false']:
                flag = False
                break
        if flag:
            for value in values:
                if value[key] is not None:
                    if value[key] in ['1', 'true']:
                        value[key] = True
                    else:
                        value[key] = False
    elif isinstance(col_type, EnumType):
        for value in values:
            col_value = value[key]
            if col_value is None:
                continue
            if value not in col_type.values:
                flag = False
                break
    elif isinstance(col_type, DatetimeType):
        for value in values:
            date_str = value[key]
            if date_str is None:
                continue
            try:
                datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                flag = False
                break
        if flag:
            return col_type
    elif isinstance(col_type, DateType):
        for value in values:
            date_str = value[key]
            if date_str is None:
                continue
            try:
                if dialect == 'oracle':
                    datetime.strptime(date_str, '%d-%b-%y')
                else:
                    datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                flag = False
                break
        if flag:
            for value in values:
                date_str = value[key]
                if date_str is None:
                    continue
                if dialect == 'oracle':
                    dt = datetime.strptime(date_str, '%d-%b-%y')
                    if dt.year % 100 < 50:
                        value[key] = f"20{dt.year % 100}-{dt.month}-{dt.day}"
                    else:
                        value[key] = f"19{dt.year % 100}-{dt.month}-{dt.day}"
            return col_type
    elif isinstance(col_type, FloatGeneralType):
        for value in values:
            col_value = value[key]
            if col_value is None:
                continue
            try:
                float(col_value)
            except ValueError:
                flag = False
                break
        if flag:
            for value in values:
                if value[key] is not None:
                    value[key] = float(value[key])
    elif isinstance(col_type, VarcharType) or isinstance(col_type, CharType) or isinstance(col_type, NvarcharType):
        max_len = 0
        for value in values:
            col_value = value[key]
            if col_value is None:
                continue
            if len(col_value) > max_len:
                max_len = len(col_value)
        if col_type.length is None or max_len > col_type.length:
            assert max_len <= 4000
            max_len = min(4000, max_len + 50)
            final_length = (max_len // 100) * 100
            if max_len % 100 != 0:
                final_length += 100
            if final_length == 0:
                final_length = 100
            col_type.length = final_length
    if not flag:
        return None
    else:
        return col_type
