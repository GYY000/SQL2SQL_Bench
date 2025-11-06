# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: udf_entry$
# @Author: 10379
# @Time: 2025/4/14 12:30
import random
import re
import string
from abc import ABC, abstractmethod
from collections import Counter
from datetime import datetime, timedelta, date

import isodate
from faker import Faker
import xml.etree.ElementTree as ET

from antlr_parser.Tree import TreeNode
from antlr_parser.general_tree_analysis import inside_aggregate_function
from sql_gen.generator.ele_type.type_def import StringGeneralType, NumberType, IntLiteralType, IntervalType, \
    StringLiteralType, WordLiteralType, ListType, AnyValueType, BaseType
from sql_gen.generator.element.Operand import Operand
from sql_gen.generator.fetch_operand_type import sample_value
from sql_gen.generator.udfs.udf_assist_function import parse_time_string, parse_ori_pg_interval_string, reg_escape, \
    is_valid_pg_time_string
from utils.ExecutionEnv import ExecutionEnv
from utils.tools import no_space_and_case_insensitive_str, no_space_and_case_insensitive_str_eq

function_registry = {}


def function_registered(func_name):
    return func_name in function_registry


def register_function(cls):
    function_registry[cls.__name__] = cls
    return cls


class UdfFunction(ABC):
    def __init__(self, execution_env: ExecutionEnv = None, select_stmt_node: TreeNode = None, dialect: str = None):
        self.execution_env = execution_env
        self.select_stmt_node = select_stmt_node
        self.dialect = dialect

    @abstractmethod
    def getReturnType(self):
        pass

    @abstractmethod
    def execute(self, *args):
        if not all(isinstance(arg, Operand) or isinstance(arg, TreeNode) for arg in args):
            raise ValueError("All the parameter should be Operand Type")
        pass

    def fulfill_cond(self, *args):
        return True


def getReturnType(class_name):
    if class_name not in function_registry:
        raise ValueError(f"UDF {class_name} not registered.")
    cls = function_registry[class_name]
    instance = cls()
    func_name = 'getReturnType'
    if not hasattr(instance, func_name):
        raise AttributeError(f"{instance.__class__.__name__} has no method named {func_name}")
    method = getattr(instance, func_name)
    return method()


def execute(class_name, dialect: str, execution_env: ExecutionEnv, select_stmt_node: TreeNode, *args):
    if class_name not in function_registry:
        raise ValueError(f"UDF {class_name} not registered.")
    cls = function_registry[class_name]
    instance = cls(execution_env, select_stmt_node, dialect)
    func_name = 'execute'
    if not hasattr(instance, func_name):
        raise AttributeError(f"{instance.__class__.__name__} has no method named {func_name}")
    for arg in args:
        if not (isinstance(arg, Operand) or isinstance(arg, list)):
            print(type(arg))
            print(arg)
            raise ValueError("All parameter should be Operand or List Type")
    method = getattr(instance, func_name)
    return method(*args)


def fulfill_cond(class_name, *args):
    if class_name not in function_registry:
        raise ValueError(f"UDF {class_name} not registered.")
    cls = function_registry[class_name]
    instance = cls()
    func_name = 'fulfill_cond'
    if not hasattr(instance, func_name):
        raise AttributeError(f"{instance.__class__.__name__} has no method named {func_name}")
    if not all(isinstance(arg, TreeNode) for arg in args):
        raise ValueError("All parameter should be Operand Type")
    method = getattr(instance, func_name)
    return method(*args)


# Date UdfFunctions

@register_function
class GenMySQLTimeFormat(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 0
        # Currently don't use '%W %M' because they will cause indent difference between Oracle, PG and MySQL.
        # Only use those return value is digit or %b
        flag = random.randint(0, 7)
        if flag == 0:
            format_value = '%Y-%m-%d %H:%i:%S'
        elif flag == 1:
            format_value = '%d-%b-%Y %H:%i:%S'
        elif flag == 2:
            format_value = '%Y-%m-%d'
        elif flag == 3:
            format_value = '%d-%b-%Y'
        elif flag == 4:
            format_value = '%H:%i:%S'
        elif flag == 5:
            format_value = '%Y-%m-%d %h:%i:%s %p'
        elif flag == 6:
            format_value = '%d-%b-%Y %h:%i:%s %p'
        elif flag == 7:
            format_value = '%Y-%m-%d %H:%i:%S.%f'
        else:
            assert False
        return Operand(f"'{format_value}'", self.getReturnType())


@register_function
class TranMySQLDateFormatToPG(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        format_str = args[0].str_value()
        assert isinstance(format_str, str)
        format_str = (format_str.replace('%Y', 'YYYY').replace('%I', 'HH12').
                      replace('%i', 'MI').replace('%S', 'SS').replace('%s', 'SS').
                      replace('%f', 'FF').replace('%H', 'HH24').replace('%h', 'HH').
                      replace('%M', 'MONTH').replace('%b', 'Mon').replace('%m', 'MM').
                      replace('%d', 'DD').replace('%a', 'Dy').replace('%p', 'AM').
                      replace('%y', 'RR'))
        return Operand(f"{format_str}", self.getReturnType())


@register_function
class TranPgDateFormatToMySQL(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        format_str = args[0].str_value()
        assert isinstance(format_str, str)
        format_str = (format_str.replace('YYYY', '%Y').replace('HH12', '%I').
                      replace('MI', '%i').replace('SS', '%S').replace('SS', '%s').
                      replace('FF', '%f').replace('HH24', '%H').replace('HH', '%h').
                      replace('MONTH', '%M').replace('Mon', '%b').replace('MM', '%m').
                      replace('DD', '%d').replace('Dy', '%a').replace('AM', '%p').
                      replace('RR', '%y'))
        return Operand(f"{format_str}", self.getReturnType())


@register_function
class GenTimestampOracleStr(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        # gen random_time
        start = datetime(year=1965, month=1, day=1)
        end = datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59)
        delta = (end - start).total_seconds()
        random_second = random.uniform(0, delta)
        time = start + timedelta(seconds=random_second)
        if args[0].str_value() == 'DD-MON-RR HH.MI.SSXFF AM':
            month = time.strftime('%b')
            month = month.upper()
            time_str1 = time.strftime(f'%d-')
            time_str3 = time.strftime(f'-%y %H.%M.%S.%f %p')
            time_str = time_str1 + month + time_str3
        elif args[0].str_value() == 'Mon dd, YYYY, HH24:MI:SS':
            time_str = time.strftime('%b %d, %Y, %H:%M:%S')
        elif args[0].str_value() == 'DD-MON-RR':
            month = time.strftime('%b')
            month = month.upper()
            day = time.strftime('%d')
            year = time.strftime('%y')
            time_str = f'{day}-{month}-{year}'
        elif args[0].str_value() == 'Mon dd, YYYY':
            time_str = time.strftime('%b %d, %Y')
        else:
            assert False

        return Operand(f"'{time_str}'", self.getReturnType())


#  @TranToMySQLFormat(<timestamp_str>, 'Mon dd, YYYY')
@register_function
class TranToStandardTimeFormat(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 2
        assert isinstance(args[0], Operand)
        assert isinstance(args[1], Operand)
        time_str = args[0].str_value().strip('\'')
        if args[1].str_value() == 'Mon dd, YYYY':
            dt = datetime.strptime(time_str, '%b %d, %Y')
            return Operand(f"'{dt.strftime('%Y-%m-%d')}'", self.getReturnType())
        elif args[1].str_value() == 'DD-MON-RR':
            dt = datetime.strptime(time_str, '%d-%b-%y')
            if dt.year >= 2050:
                dt = dt.replace(year=dt.year - 100)
            return Operand(f"'{dt.strftime('%Y-%m-%d')}'", self.getReturnType())
        elif args[1].str_value() == 'DD-MON-RR HH.MI.SSXFF AM':
            dt = datetime.strptime(time_str, '%d-%b-%y %I.%M.%S.%f %p')
            if dt.year >= 2050:
                dt = dt.replace(year=dt.year - 100)
            return Operand(f"'{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'", self.getReturnType())
        elif args[1].str_value() == 'Mon dd, YYYY, HH24:MI:SS':
            dt = datetime.strptime(time_str, '%b %d, %Y, %H:%M:%S')
            return Operand(f"'{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'", self.getReturnType())
        else:
            assert False


@register_function
class GenPGDateParaStr(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        year = random.randint(1985, 2025)
        month = random.randint(1, 12)
        day = random.randint(1, 12)
        while month == day:
            day = random.randint(1, 12)
        time = datetime(year=year, month=month, day=day)
        if args[0].str_value() == 'MDY':
            return Operand(f"'{time.strftime('%m/%d/%Y')}'", self.getReturnType())
        elif args[0].str_value() == 'DMY':
            return Operand(f"'{time.strftime('%d/%m/%Y')}'", self.getReturnType())
        else:
            assert False


@register_function
class GenStandardDate(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 2
        date_type = args[1].str_value()
        date_value_str = args[0].str_value().strip('\'')
        if date_type == 'MDY':
            date_value = datetime.strptime(date_value_str, '%m/%d/%Y')
        elif date_type == 'DMY':
            date_value = datetime.strptime(date_value_str, '%d/%m/%Y')
        else:
            assert False
        return Operand(f"'{date_value.strftime('%Y-%m-%d')}'", self.getReturnType())


@register_function
class GenOracleStrNumber(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        number = random.randint(-500000, 500000)
        if number < 0:
            sign = '-'
            number = -number
        else:
            sign = ''
        decimal = (number % 100)
        val = (number - decimal) / 100
        above1000 = int(val // 1000)
        below1000 = int(val - above1000 * 1000)
        if args[0].str_value() == '.,':
            if above1000 == 0:
                return Operand(f"'{sign}{below1000}.{decimal}'", self.getReturnType())
            else:
                return Operand(f"'{sign}{above1000},{below1000}.{decimal}'", self.getReturnType())
        else:
            if above1000 == 0:
                return Operand(f"'{sign}{below1000},{decimal}'", self.getReturnType())
            else:
                return Operand(f"'{sign}{above1000}.{below1000},{decimal}'", self.getReturnType())


@register_function
class GenNumber(UdfFunction):
    def getReturnType(self):
        return NumberType()

    def execute(self, *args):
        assert len(args) == 2
        assert isinstance(args[0], Operand)
        value_op = args[0].str_value().strip().strip('\'')
        type_op = args[1].str_value()
        if type_op == '.,':
            num_str = value_op.replace(',', '')
            return Operand(f"{num_str}", self.getReturnType())
        else:
            num_str = value_op.replace('.', '').replace(',', '.')
            return Operand(f"{num_str}", self.getReturnType())


# Literal Function
@register_function
class GenMySQLIntervalInt(UdfFunction):
    def getReturnType(self):
        return IntLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        value_op = args[0].str_value()
        # 'MICROSECOND', 'SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'
        year = random.randint(0, 5)
        month = random.randint(0, 12)
        day = random.randint(0, 30)  # 简单处理，避免超过月份最大天数
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        microsecond = random.randint(0, 999999)
        if value_op == 'MICROSECOND':
            return Operand(f"{microsecond}", self.getReturnType())
        elif value_op == 'SECOND':
            return Operand(f"{second}", self.getReturnType())
        elif value_op == 'MINUTE':
            return Operand(f"{minute}", self.getReturnType())
        elif value_op == 'HOUR':
            return Operand(f"{hour}", self.getReturnType())
        elif value_op == 'DAY':
            return Operand(f"{day}", self.getReturnType())
        elif value_op == 'WEEK':
            return Operand(f"{day // 7}", self.getReturnType())
        elif value_op == 'MONTH':
            return Operand(f"{month}", self.getReturnType())
        elif value_op == 'QUARTER':
            return Operand(f"{month // 3}", self.getReturnType())
        elif value_op == 'YEAR':
            return Operand(f"{year}", self.getReturnType())
        else:
            assert False


@register_function
class GenMySQLIntervalString(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        year = random.randint(0, 5)
        month = random.randint(0, 12)
        day = random.randint(0, 30)  # 简单处理，避免超过月份最大天数
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        microsecond = random.randint(0, 999999)
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        value_op = args[0].str_value()
        # 'MINUTE_MICROSECOND', 'HOUR_SECOND', 'HOUR_MINUTE', 'DAY_SECOND', 'DAY_MINUTE', 'DAY_HOUR', 'YEAR_MONTH'
        if value_op == 'MINUTE_MICROSECOND':
            return Operand(f"'{minute}:{second}.{microsecond}'", self.getReturnType())
        elif value_op == 'HOUR_SECOND':
            return Operand(f"'{hour}:{minute}:{second}'", self.getReturnType())
        elif value_op == 'HOUR_MINUTE':
            return Operand(f"'{hour}:{minute}'", self.getReturnType())
        elif value_op == 'DAY_SECOND':
            return Operand(f"'{day} {hour}:{minute}:{second}'", self.getReturnType())
        elif value_op == 'DAY_MINUTE':
            return Operand(f"'{day} {hour}:{minute}'", self.getReturnType())
        elif value_op == 'DAY_HOUR':
            return Operand(f"'{day} {hour}'", self.getReturnType())
        elif value_op == 'YEAR_MONTH':
            return Operand(f"'{year}-{month}'", self.getReturnType())
        else:
            assert False


@register_function
class GenMySQLOracleIntervalValue(UdfFunction):
    def getReturnType(self):
        return IntervalType()

    def execute(self, *args):
        assert len(args) == 2
        assert isinstance(args[0], Operand)
        literal_op = args[0].str_value()
        unit_op = args[1].str_value()
        if unit_op == 'MICROSECOND':
            microsecond = int(literal_op)
            return Operand(f"INTERVAL '0.{microsecond}' SECOND(6)", self.getReturnType())
        elif unit_op == 'SECOND':
            second = int(literal_op)
            return Operand(f"INTERVAL '{second}' SECOND", self.getReturnType())
        elif unit_op == 'MINUTE':
            minute = int(literal_op)
            return Operand(f"INTERVAL '{minute}' MINUTE", self.getReturnType())
        elif unit_op == 'HOUR':
            hour = int(literal_op)
            return Operand(f"INTERVAL '{hour}' HOUR", self.getReturnType())
        elif unit_op == 'DAY':
            day = int(literal_op)
            return Operand(f"INTERVAL '{day}' DAY", self.getReturnType())
        elif unit_op == 'WEEK':
            week = int(literal_op)
            return Operand(f"INTERVAL '{7 * week}' DAY", self.getReturnType())
        elif unit_op == 'MONTH':
            month = int(literal_op)
            return Operand(f"INTERVAL '{month}' MONTH", self.getReturnType())
        elif unit_op == 'QUARTER':
            quarter = int(literal_op)
            return Operand(f"INTERVAL '{3 * quarter}' MONTH", self.getReturnType())
        elif unit_op == 'YEAR':
            year = int(literal_op)
            return Operand(f"INTERVAL '{year}' YEAR", self.getReturnType())
        elif unit_op == 'MINUTE_MICROSECOND':
            pattern_string = "(\\d+):(\\d+)\\.(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            minute = int(match.group(1))
            second = int(match.group(2))
            microsecond = int(match.group(3))
            return Operand(f"INTERVAL '{minute}:{second}.{microsecond}' MINUTE TO SECOND(6)", self.getReturnType())
        elif unit_op == 'HOUR_SECOND':
            pattern_string = "(\\d+):(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            hour = int(match.group(1))
            minute = int(match.group(2))
            second = int(match.group(3))
            return Operand(f"INTERVAL '{hour}:{minute}:{second}' HOUR TO SECOND", self.getReturnType())
        elif unit_op == 'HOUR_MINUTE':
            pattern_string = "(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            hour = int(match.group(1))
            minute = int(match.group(2))
            return Operand(f"INTERVAL '{hour}:{minute}' HOUR TO MINUTE", self.getReturnType())
        elif unit_op == 'DAY_SECOND':
            pattern_string = "(\\d+) (\\d+):(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            minute = int(match.group(3))
            second = int(match.group(4))
            return Operand(f"INTERVAL '{day} {hour}:{minute}:{second}' DAY TO SECOND", self.getReturnType())
        elif unit_op == 'DAY_MINUTE':
            pattern_string = "(\\d+) (\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            minute = int(match.group(3))
            return Operand(f"INTERVAL '{day} {hour}:{minute}' DAY TO MINUTE", self.getReturnType())
        elif unit_op == 'DAY_HOUR':
            pattern_string = "(\\d+) (\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            return Operand(f"INTERVAL '{day} {hour}' DAY TO HOUR", self.getReturnType())
        elif unit_op == 'YEAR_MONTH':
            pattern_string = "(\\d+)-(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            year = int(match.group(1))
            month = int(match.group(2))
            return Operand(f"INTERVAL '{year}-{month}' YEAR TO MONTH", self.getReturnType())
        else:
            assert False


@register_function
class GenMySQLPGIntervalValue(UdfFunction):
    def getReturnType(self):
        return IntervalType()

    def execute(self, *args):
        assert len(args) == 2
        assert isinstance(args[0], Operand)
        literal_op = args[0].str_value()
        unit_op = args[1].str_value()
        if unit_op == 'MICROSECOND':
            microsecond = int(literal_op)
            return Operand(f"INTERVAL '{microsecond} microseconds'", self.getReturnType())
        elif unit_op == 'SECOND':
            second = int(literal_op)
            return Operand(f"INTERVAL '{second} seconds'", self.getReturnType())
        elif unit_op == 'MINUTE':
            minute = int(literal_op)
            return Operand(f"INTERVAL '{minute} minutes'", self.getReturnType())
        elif unit_op == 'HOUR':
            hour = int(literal_op)
            return Operand(f"INTERVAL '{hour} hours'", self.getReturnType())
        elif unit_op == 'DAY':
            day = int(literal_op)
            return Operand(f"INTERVAL '{day} days'", self.getReturnType())
        elif unit_op == 'WEEK':
            week = int(literal_op)
            return Operand(f"INTERVAL '{7 * week} days'", self.getReturnType())
        elif unit_op == 'MONTH':
            month = int(literal_op)
            return Operand(f"INTERVAL '{month} months'", self.getReturnType())
        elif unit_op == 'QUARTER':
            quarter = int(literal_op)
            return Operand(f"INTERVAL '{3 * quarter} months'", self.getReturnType())
        elif unit_op == 'YEAR':
            year = int(literal_op)
            return Operand(f"INTERVAL '{year} years'", self.getReturnType())
        elif unit_op == 'MINUTE_MICROSECOND':
            pattern_string = "(\\d+):(\\d+)\\.(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            minute = int(match.group(1))
            second = int(match.group(2))
            microsecond = int(match.group(3))
            return Operand(f"INTERVAL '{minute} minutes {second} seconds {microsecond} microseconds'",
                           self.getReturnType())
        elif unit_op == 'HOUR_SECOND':
            pattern_string = "(\\d+):(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            hour = int(match.group(1))
            minute = int(match.group(2))
            second = int(match.group(3))
            return Operand(f"INTERVAL '{hour} hours {minute} minutes {second} seconds'", self.getReturnType())
        elif unit_op == 'HOUR_MINUTE':
            pattern_string = "(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            hour = int(match.group(1))
            minute = int(match.group(2))
            return Operand(f"INTERVAL '{hour} hours {minute} minutes'", self.getReturnType())
        elif unit_op == 'DAY_SECOND':
            pattern_string = "(\\d+) (\\d+):(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            minute = int(match.group(3))
            second = int(match.group(4))
            return Operand(f"INTERVAL '{day} days {hour} hours {minute} minutes {second} seconds'",
                           self.getReturnType())
        elif unit_op == 'DAY_MINUTE':
            pattern_string = "(\\d+) (\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            minute = int(match.group(3))
            return Operand(f"INTERVAL '{day} days {hour} hours {minute} minutes'", self.getReturnType())
        elif unit_op == 'DAY_HOUR':
            pattern_string = "(\\d+) (\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            return Operand(f"INTERVAL '{day} days {hour} hours'", self.getReturnType())
        elif unit_op == 'YEAR_MONTH':
            pattern_string = "(\\d+)-(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            year = int(match.group(1))
            month = int(match.group(2))
            return Operand(f"INTERVAL '{year} years {month} months'", self.getReturnType())
        else:
            assert False


@register_function
class GenOracleMySQLUnit(UdfFunction):
    def getReturnType(self):
        return WordLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        unit_op = no_space_and_case_insensitive_str(args[0].str_value())
        # 'DAY TO SECOND(3)', 'DAY TO MINUTE', 'HOUR TO MINUTE', 'YEAR(3) TO MONTH', 'YEAR TO MONTH'
        print(unit_op)
        if unit_op == 'DAYTOSECOND(3)'.lower():
            return Operand(f"DAY_SECOND", self.getReturnType())
        elif unit_op == 'DAYTOSECOND'.lower():
            return Operand(f"DAY_SECOND", self.getReturnType())
        elif unit_op == 'DAYTOMINUTE'.lower():
            return Operand(f"DAY_MINUTE", self.getReturnType())
        elif unit_op == 'HOURTOMINUTE'.lower():
            return Operand(f"HOUR_MINUTE", self.getReturnType())
        elif unit_op == 'YEAR(3)TOMONTH'.lower():
            return Operand(f"YEAR_MONTH", self.getReturnType())
        elif unit_op == 'YEARTOMONTH'.lower():
            return Operand(f"YEAR_MONTH", self.getReturnType())
        else:
            assert False


@register_function
class GenOraclePGIntervalValue(UdfFunction):
    def getReturnType(self):
        return IntervalType()

    def execute(self, *args):
        assert len(args) == 2
        assert isinstance(args[0], Operand)
        # 'SECOND', 'MINUTE', 'HOUR', 'DAY', 'MONTH', 'YEAR'
        # 'DAY TO SECOND(3)', 'DAY TO SECOND', 'DAY TO MINUTE', 'HOUR TO MINUTE', 'YEAR(3) TO MONTH', 'YEAR TO MONTH'
        literal_op = args[0].str_value()
        unit_op = args[1].str_value()
        if no_space_and_case_insensitive_str_eq(unit_op, 'SECOND'):
            return Operand(f"INTERVAL '{literal_op} seconds'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'MINUTE'):
            return Operand(f"INTERVAL '{literal_op} minutes'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'HOUR'):
            return Operand(f"INTERVAL '{literal_op} hours'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'DAY'):
            return Operand(f"INTERVAL '{literal_op} days'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'MONTH'):
            return Operand(f"INTERVAL '{literal_op} months'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'YEAR'):
            return Operand(f"INTERVAL '{literal_op} years'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'DAY TO SECOND(3)') or no_space_and_case_insensitive_str_eq(
                unit_op, 'DAY TO SECOND'):
            pattern_string = "(\\d+) (\\d+):(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            minute = int(match.group(3))
            second = int(match.group(4))
            return Operand(f"INTERVAL '{day} days {hour} hours {minute} minutes {second} seconds'",
                           self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'DAY TO MINUTE'):
            pattern_string = "(\\d+) (\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            day = int(match.group(1))
            hour = int(match.group(2))
            minute = int(match.group(3))
            return Operand(f"INTERVAL '{day} days {hour} hours {minute} minutes'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'HOUR TO MINUTE'):
            pattern_string = "(\\d+):(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            hour = int(match.group(1))
            minute = int(match.group(2))
            return Operand(f"INTERVAL '{hour} hours {minute} minutes'", self.getReturnType())
        elif no_space_and_case_insensitive_str_eq(unit_op, 'YEAR(3) TO MONTH') or no_space_and_case_insensitive_str_eq(
                unit_op, 'YEAR TO MONTH'):
            pattern_string = "(\\d+)-(\\d+)"
            match = re.match(pattern_string, literal_op.strip().strip('\''))
            assert match
            year = int(match.group(1))
            month = int(match.group(2))
            return Operand(f"INTERVAL '{year} years {month} months'", self.getReturnType())
        else:
            assert False


@register_function
class GenOracleIntervalSingle(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        unit_op = args[0].str_value()
        # SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'
        if unit_op == 'SECOND':
            seconds = random.randint(0, 59)
            return Operand(f"'{seconds}'", self.getReturnType())
        elif unit_op == 'MINUTE':
            minutes = random.randint(0, 59)
            return Operand(f"'{minutes}'", self.getReturnType())
        elif unit_op == 'HOUR':
            hours = random.randint(0, 23)
            return Operand(f"'{hours}'", self.getReturnType())
        elif unit_op == 'DAY':
            days = random.randint(0, 30)
            return Operand(f"'{days}'", self.getReturnType())
        elif unit_op == 'WEEK':
            weeks = random.randint(0, 5)
            return Operand(f"'{weeks}'", self.getReturnType())
        elif unit_op == 'MONTH':
            months = random.randint(0, 11)
            return Operand(f"'{months}'", self.getReturnType())
        elif unit_op == 'YEAR':
            years = random.randint(0, 5)
            return Operand(f"'{years}'", self.getReturnType())
        else:
            assert False


@register_function
class GenOracleIntervalDouble(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 0
        # assert isinstance(args[0], Operand)
        # unit_op = args[0].str_value()
        # 'DAY TO SECOND(3)', 'DAY TO MINUTE', 'HOUR TO MINUTE', 'YEAR(3) TO MONTH', 'YEAR TO MONTH','DAY TO SECOND'
        days = random.randint(0, 30)
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        return Operand(f"'{days} {hours}:{minutes}:{seconds}'", self.getReturnType())
        # if unit_op == 'DAY TO MINUTE':
        #     return Operand(f"'{days} {hours}:{minutes}'", self.getReturnType())
        # elif unit_op == 'DAY TO SECOND(3)':
        #     return Operand(f"'{days} {hours}:{minutes}:{seconds}'", self.getReturnType())
        # elif unit_op == 'HOUR TO MINUTE':
        #     return Operand(f"'{hours}:{minutes}'", self.getReturnType())
        # elif unit_op == 'YEAR(3) TO MONTH':
        #     years = random.randint(0, 5)
        #     months = random.randint(0, 11)
        #     return Operand(f"'{years}-{months}'", self.getReturnType())
        # elif unit_op == 'YEAR TO MONTH':
        #     years = random.randint(0, 5)
        #     months = random.randint(0, 11)
        #     return Operand(f"'{years}-{months}'", self.getReturnType())
        # elif unit_op == 'DAY TO SECOND':
        #     return Operand(f"'{days} {hours}:{minutes}:{seconds}'", self.getReturnType())
        # else:
        #     assert False


# Numeric UserDefineFunctions


# StringUserDefineFunctions

@register_function
class GenDoubleHexString(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 0
        bits = random.randint(1, 4)
        bits = bits * 2
        return Operand(f"'{''.join(random.choices(string.hexdigits, k=bits))}'", self.getReturnType())


@register_function
class GenHexString(UdfFunction):
    def getReturnType(self):
        return WordLiteralType()

    def execute(self, *args):
        assert len(args) == 0
        bits = random.randint(1, 9)
        bits = bits * 2
        return Operand(f"{''.join(random.choices(string.hexdigits, k=bits))}", self.getReturnType())


@register_function
class ContainBackSlash(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], TreeNode)
        value_op = str(args[0])
        if '\\' in value_op:
            print(True)
            return True
        else:
            return False

    def execute(self, *args):
        assert len(args) == 0
        fake = Faker()
        return Operand(f'\'{fake.sentence()}\\n\'', self.getReturnType())


@register_function
class RepBackSlash(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        value_op = str(args[0])
        i = 0
        new_str = ''
        while i < len(value_op):
            if value_op[i] == '\\':
                if value_op[i + 1] == 'n':
                    new_str += '\n'
                elif value_op[i + 1] == 'r':
                    new_str += '\r'
                elif value_op[i + 1] == 't':
                    new_str += '\t'
                elif value_op[i + 1] == 'b':
                    new_str += '\b'
                elif value_op[i + 1] == 'f':
                    new_str += '\f'
                elif value_op[i + 1] == '\\':
                    new_str += '\\'
                elif value_op[i + 1] == '\'':
                    new_str += '\''
                i += 1
            else:
                new_str += value_op[i]
            i += 1
        return Operand(f'\'{value_op}\'', self.getReturnType())


@register_function
class GenQstring(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], TreeNode)
        value_op = str(args[0])
        print('---')
        print(value_op)
        print('---')
        if value_op.startswith('Q\'') and value_op.endswith('\''):
            print(True)
            return True
        else:
            return False

    def execute(self, *args):
        assert len(args) == 0
        fake = Faker()
        if random.randint(0, 1) == 0:
            return Operand(f"Q'<{fake.sentence()}>", self.getReturnType())
        else:
            return Operand(f"Q'!{fake.sentence()}!'", self.getReturnType())


@register_function
class AddQstrEscape(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        value_op = args[0].str_value()
        print(value_op)
        assert value_op.startswith('Q\'')
        assert value_op.endswith('\'')
        # additionally jump the bracket
        value_op = value_op[3:-2]
        for i in range(len(value_op)):
            if value_op[i] == '\'':
                value_op = value_op[:i] + '\\' + value_op[i:]
            elif value_op[i] == '\\':
                value_op = value_op[:i] + '\\' + value_op[i:]
        return Operand(f"'{value_op}'", self.getReturnType())


@register_function
class GenPGSQLStandardLiteral(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], TreeNode)
        string_value = str(args[0]).strip().strip('\'')
        return parse_ori_pg_interval_string(string_value) is not None

    def execute(self, *args):
        assert len(args) == 0
        year = random.randint(-5, 5)
        month = random.randint(0, 11)
        days = random.randint(-30, 30)
        hours = random.randint(-23, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        if random.randint(0, 1) == 0:
            return Operand(f"'{year}-{month}'", self.getReturnType())
        elif random.randint(0, 1) == 0:
            return Operand(f"'{days} {hours}:{minutes}:{seconds}'", self.getReturnType())
        elif random.randint(0, 1) == 0:
            return Operand(f"'{days} {hours}:{minutes}'", self.getReturnType())
        else:
            return Operand(f"'{year}-{month} {days} {hours}:{minutes}:{seconds}'", self.getReturnType())


@register_function
class GenISOIntervalLiteral(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], TreeNode)
        string_value = str(args[0]).strip().strip('\'')
        try:
            isodate.parse_duration(string_value)
            return True
        except Exception as e:
            return False

    def execute(self, *args):
        assert len(args) <= 1
        if len(args) == 1:
            assert isinstance(args[0], Operand)
            string_value = args[0].str_value()
            mode = string_value
        else:
            mode = random.choice(['YM', 'DS'])
        year = random.randint(-5, 5)
        month = random.randint(0, 11)
        days = random.randint(-30, 30)
        hours = random.randint(-23, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        if mode == 'YM':
            if year < 0:
                year = abs(year)
                return Operand(f"'-P{year}Y{month}M'", self.getReturnType())
            else:
                return Operand(f"'P{year}Y{month}M'", self.getReturnType())
        elif random.randint(0, 1) == 0:
            total_seconds = days * 24 * 3600 + hours * 3600 + minutes * 60 + seconds
            if total_seconds < 0:
                sign = '-'
                total_seconds = -total_seconds
            else:
                sign = ''
            days = total_seconds // (24 * 60 * 60)
            total_seconds = total_seconds % (24 * 60 * 60)
            hours = total_seconds // (60 * 60)
            total_seconds = total_seconds % (60 * 60)
            minutes = total_seconds // 60
            total_seconds = total_seconds % 60
            return Operand(f"'{sign}P{days}DT{hours}H{minutes}M{total_seconds}S'", self.getReturnType())
        else:
            total_seconds = days * 24 * 3600 + hours * 3600 + minutes * 60
            if total_seconds < 0:
                sign = '-'
                total_seconds = -total_seconds
            else:
                sign = ''
            days = total_seconds // (24 * 60 * 60)
            total_seconds = total_seconds % (24 * 60 * 60)
            hours = total_seconds // (60 * 60)
            total_seconds = total_seconds % (60 * 60)
            minutes = total_seconds // 60
            return Operand(f"'{sign}P{days}DT{hours}H{minutes}M'", self.getReturnType())


@register_function
class GenPGOriIntervalLiteral(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], TreeNode)
        string_value = str(args[0]).strip().strip('\'')
        is_valid = is_valid_pg_time_string(string_value)
        return is_valid

    def execute(self, *args):
        assert len(args) == 0
        year = 0
        month = 0
        days = 0
        hours = 0
        minutes = 0
        seconds = 0
        if random.randint(0, 1) == 0:
            year = random.randint(-5, 5)
            month = random.randint(0, 11)
        elif random.randint(0, 1) == 0:
            days = random.randint(-30, 30)
            hours = random.randint(-23, 23)
            minutes = random.randint(0, 59)
            seconds = random.randint(0, 59)
        elif random.randint(0, 1) == 0:
            year = random.randint(-5, 5)
            month = random.randint(0, 11)
            days = random.randint(-30, 30)
            hours = random.randint(-23, 23)
            minutes = random.randint(0, 59)
            seconds = random.randint(0, 59)
        else:
            days = random.randint(-30, 30)
            hours = random.randint(-23, 23)
            minutes = random.randint(0, 59)
        if random.randint(0, 1) == 0:
            res = '@ '
        else:
            res = ''
        if year != 0:
            if res != '':
                res += ' '
            res += f'{year} years'
        if month != 0:
            if res != '':
                res += ' '
            res += f'{month} mons'
        if days != 0:
            if res != '':
                res += ' '
            res += f'{days} days'
        if hours != 0:
            if res != '':
                res += ' '
            res += f'{hours} hours'
        if minutes != 0:
            if res != '':
                res += ' '
            res += f'{minutes} mins'
        if seconds != 0:
            if res != '':
                res += ' '
            res += f'{seconds} secs'
        if random.randint(0, 1) == 0:
            res += ' ago'
        return Operand(f"'{res}'", self.getReturnType())


@register_function
class GenPGOracleInterval(UdfFunction):
    def getReturnType(self):
        return IntervalType()

    def execute(self, *args):
        assert len(args) == 2
        mode = args[1].str_value()
        literal_value = args[0].str_value().strip().strip('\'')
        year_month_interval = None
        day_second_interval = None
        if mode == 'STANDARD':
            parsed_str_dict = parse_ori_pg_interval_string(literal_value)
            assert parsed_str_dict is not None
            if 'year_month' in parsed_str_dict and parsed_str_dict['year_month'] is not None:
                year_month = parsed_str_dict['year_month']
                assert isinstance(year_month, str)
                if year_month.startswith('-'):
                    month = int(year_month[1:].split('-')[1])
                    year = int(year_month[1:].split('-')[0])
                    year_month_interval = f"TO_YMINTERVAL('-P{year}Y{month}M')"
                else:
                    month = int(year_month.split('-')[1])
                    year = int(year_month.split('-')[0])
                    year_month_interval = f"TO_YMINTERVAL('P{year}Y{month}M')"
            days = 0
            hours = 0
            minutes = 0
            seconds = 0
            sign_time = 1
            if 'days' in parsed_str_dict:
                days = int(parsed_str_dict['days'])
            if 'time' in parsed_str_dict:
                time = parsed_str_dict['time']
                assert isinstance(time, str)
                if time.startswith('-'):
                    time = time[1:]
                    sign_time = -1
                time_split = time.split(':')
                if len(time_split) > 1:
                    hours = int(time_split[0])
                if len(time_split) > 2:
                    minutes = int(time_split[1])
                if len(time_split) > 3:
                    seconds = int(time_split[2])
            total_seconds = days * 24 * 60 * 60 + sign_time * (hours * 60 * 60 + minutes * 60 + seconds)
            if total_seconds != 0:
                if total_seconds < 0:
                    sign = '-'
                    total_seconds = -total_seconds
                else:
                    sign = ''
                days = total_seconds // (24 * 60 * 60)
                total_seconds = total_seconds % (24 * 60 * 60)
                hours = total_seconds // (60 * 60)
                total_seconds = total_seconds % (60 * 60)
                minutes = total_seconds // 60
                total_seconds = total_seconds % 60
                day_second_interval = f"TO_DSINTERVAL('{sign}P{days}D{hours}H{minutes}M{total_seconds}S')"
            if year_month_interval is not None and day_second_interval is not None:
                return Operand(f"{year_month_interval} + {day_second_interval}", self.getReturnType())
            elif year_month_interval is not None:
                return Operand(f"{year_month_interval}", self.getReturnType())
            elif day_second_interval is not None:
                return Operand(f"{day_second_interval}", self.getReturnType())
        if mode == 'ISO8601':
            duration = isodate.parse_duration(literal_value)
            years = duration.years
            months = duration.months
            days = duration.days
            hours = duration.seconds // 3600
            remaining_seconds = duration.seconds % 3600
            minutes = remaining_seconds // 60
            seconds = remaining_seconds % 60
            if years != 0 or months != 0:
                assert years * months >= 0
                if years < 0:
                    year_month_interval = f"TO_YMINTERVAL('-P{-1 * years}Y{-1 * months}M')"
            if days != 0:
                if days < 0:
                    day_interval = f"TO_DSINTERVAL('-P{-1 * days}D')"
                else:
                    day_interval = f"TO_DSINTERVAL('P{days}D')"
            else:
                day_interval = None
            if hours != 0 or minutes != 0 or seconds != 0:
                assert hours >= 0 and minutes >= 0 and seconds >= 0
                day_second_interval = f"TO_DSINTERVAL('P{hours}H{minutes}M{seconds}S')"
            res_str = ''
            if year_month_interval is not None:
                res_str += year_month_interval
            if day_interval is not None:
                if res_str != '':
                    res_str += ' + '
                res_str += day_interval
            if day_second_interval is not None:
                if res_str != '':
                    res_str += ' + '
                res_str += day_second_interval
            return Operand(f"{res_str}", self.getReturnType())
        else:
            assert mode == 'PG'
            delta = parse_time_string(literal_value)
            year = delta.years
            month = delta.months
            day = delta.days
            hour = delta.hours
            minute = delta.minutes
            second = delta.seconds
            total_months = year * 12 + month
            if total_months != 0:
                if total_months < 0:
                    total_months = -total_months
                    years = total_months // 12
                    months = total_months % 12
                    year_month_interval = f"TO_YMINTERVAL('-P{years}Y{months}M')"
                else:
                    years = total_months // 12
                    months = total_months % 12
                    year_month_interval = f"TO_YMINTERVAL('P{years}Y{months}M')"
            total_seconds = day * 24 * 60 * 60 + hour * 60 * 60 + minute * 60 + second
            if total_seconds != 0:
                if total_seconds < 0:
                    sign = '-'
                    total_seconds = -total_seconds
                else:
                    sign = ''
                days = total_seconds // (24 * 60 * 60)
                total_seconds = total_seconds % (24 * 60 * 60)
                hours = total_seconds // (60 * 60)
                total_seconds = total_seconds % (60 * 60)
                minutes = total_seconds // 60
                total_seconds = total_seconds % 60
                day_second_interval = f"TO_DSINTERVAL('{sign}P{days}D{hours}H{minutes}M{total_seconds}S')"
            if year_month_interval is not None and day_second_interval is not None:
                return Operand(f"{year_month_interval} + {day_second_interval}", self.getReturnType())
            elif year_month_interval is not None:
                return Operand(f"{year_month_interval}", self.getReturnType())
            elif day_second_interval is not None:
                return Operand(f"{day_second_interval}", self.getReturnType())


@register_function
class GenPGMySQLInterval(UdfFunction):
    def getReturnType(self):
        return IntervalType()

    def execute(self, *args):
        assert len(args) == 2
        mode = args[1].str_value()
        literal_value = args[0].str_value().strip().strip('\'')
        year_month_interval = None
        day_second_interval = None
        if mode == 'STANDARD':
            parsed_str_dict = parse_ori_pg_interval_string(literal_value)
            assert parsed_str_dict is not None
            if 'year_month' in parsed_str_dict:
                year_month = parsed_str_dict['year_month']
                assert isinstance(year_month, str)
                if year_month.startswith('-'):
                    month = int(year_month[1:].split('-')[1])
                    year = int(year_month[1:].split('-')[0])
                    year_month_interval = f"INTERVAL '-{year}-{month}' YEAR_MONTH"
                else:
                    month = int(year_month.split('-')[1])
                    year = int(year_month.split('-')[0])
                    year_month_interval = f"INTERVAL '{year}-{month}' YEAR_MONTH"
            days = 0
            hours = 0
            minutes = 0
            seconds = 0
            sign_time = 1
            if 'days' in parsed_str_dict:
                days = int(parsed_str_dict['days'])
            if 'time' in parsed_str_dict:
                time = parsed_str_dict['time']
                assert isinstance(time, str)
                if time.startswith('-'):
                    time = time[1:]
                    sign_time = -1
                time_split = time.split(':')
                if len(time_split) > 1:
                    hours = int(time_split[0])
                if len(time_split) > 2:
                    minutes = int(time_split[1])
                if len(time_split) > 3:
                    seconds = int(time_split[2])
            total_seconds = days * 24 * 60 * 60 + sign_time * (hours * 60 * 60 + minutes * 60 + seconds)
            if total_seconds != 0:
                if total_seconds < 0:
                    sign = '-'
                    total_seconds = -total_seconds
                else:
                    sign = ''
                days = total_seconds // (24 * 60 * 60)
                total_seconds = total_seconds % (24 * 60 * 60)
                hours = total_seconds // (60 * 60)
                total_seconds = total_seconds % (60 * 60)
                minutes = total_seconds // 60
                total_seconds = total_seconds % 60
                day_second_interval = f"INTERVAL '{sign}{days} {hours}:{minutes}:{total_seconds}' DAY_SECOND"
            if year_month_interval is not None and day_second_interval is not None:
                return Operand(f"{year_month_interval} + {day_second_interval}", self.getReturnType())
            elif year_month_interval is not None:
                return Operand(f"{year_month_interval}", self.getReturnType())
            elif day_second_interval is not None:
                return Operand(f"{day_second_interval}", self.getReturnType())
        if mode == 'ISO8601':
            duration = isodate.parse_duration(literal_value)
            if hasattr(duration, 'years'):
                years = duration.years
            else:
                years = 0
            if hasattr(duration, 'months'):
                months = duration.months
            else:
                months = 0
            if hasattr(duration, 'days'):
                days = duration.days
            else:
                days = 0
            if hasattr(duration, 'seconds'):
                dur_seconds = duration.seconds
            else:
                dur_seconds = 0
            hours = dur_seconds // 3600
            remaining_seconds = dur_seconds % 3600
            minutes = remaining_seconds // 60
            seconds = remaining_seconds % 60
            if years != 0 or months != 0:
                assert years * months >= 0
                if years < 0:
                    year_month_interval = f"INTERVAL '-{-1 * years}-{-1 * months}' YEAR_MONTH"
                else:
                    year_month_interval = f"INTERVAL '{years}-{months}' YEAR_MONTH"
            if days != 0:
                day_interval = f"INTERVAL {days} DAY"
            else:
                day_interval = None
            if hours != 0 or minutes != 0 or seconds != 0:
                assert hours >= 0 and minutes >= 0 and seconds >= 0
                day_second_interval = f"INTERVAL '{hours}:{minutes}:{seconds}' HOUR_SECOND"
            res_str = ''
            if year_month_interval is not None:
                res_str += year_month_interval
            if day_interval is not None:
                if res_str != '':
                    res_str += ' + '
                res_str += day_interval
            if day_second_interval is not None:
                if res_str != '':
                    res_str += ' + '
                res_str += day_second_interval
            return Operand(f"{res_str}", self.getReturnType())
        else:
            assert mode == 'PG'
            delta = parse_time_string(literal_value)
            year = delta.years
            month = delta.months
            day = delta.days
            hour = delta.hours
            minute = delta.minutes
            second = delta.seconds
            total_months = year * 12 + month
            if total_months != 0:
                if total_months < 0:
                    total_months = -total_months
                    years = total_months // 12
                    months = total_months % 12
                    year_month_interval = f"INTERVAL '-{years}-{months}' YEAR_MONTH"
                else:
                    years = total_months // 12
                    months = total_months % 12
                    year_month_interval = f"INTERVAL '{years}-{months}' YEAR_MONTH"
            total_seconds = day * 24 * 60 * 60 + hour * 60 * 60 + minute * 60 + second
            if total_seconds != 0:
                if total_seconds < 0:
                    sign = '-'
                    total_seconds = -total_seconds
                else:
                    sign = ''
                days = total_seconds // (24 * 60 * 60)
                total_seconds = total_seconds % (24 * 60 * 60)
                hours = total_seconds // (60 * 60)
                total_seconds = total_seconds % (60 * 60)
                minutes = total_seconds // 60
                total_seconds = total_seconds % 60
                day_second_interval = f"INTERVAL '{sign}{days} {hours}:{minutes}:{total_seconds}' DAY_SECOND"
            if year_month_interval is not None and day_second_interval is not None:
                return Operand(f"{year_month_interval} + {day_second_interval}", self.getReturnType())
            elif year_month_interval is not None:
                return Operand(f"{year_month_interval}", self.getReturnType())
            elif day_second_interval is not None:
                return Operand(f"{day_second_interval}", self.getReturnType())


@register_function
class RtrimReg(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        reg_str = reg_escape(args[0])
        return Operand(f"'({reg_str})+$'", self.getReturnType())


@register_function
class LtrimReg(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        reg_str = reg_escape(args[0])
        return Operand(f"'^({reg_str})+'", self.getReturnType())


@register_function
class GenTrim(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 2
        value_op = args[0].str_value()
        mode = args[1].str_value()
        select_stmt_node = self.select_stmt_node
        execution_env = self.execution_env
        assert isinstance(select_stmt_node, TreeNode)
        values = sample_value(value_op, select_stmt_node, execution_env)
        if len(values) == 0:
            return Operand("'<>=.'", self.getReturnType())
        else:
            if mode == 'BOTH':
                if random.randint(0, 1) == 0:
                    mode = 'LEFT'
                else:
                    mode = 'RIGHT'
            if mode == 'LEFT':
                used_value = str(random.choice(values)[0])
                res = used_value[:2]
                if len(res) == 1:
                    raise ValueError("Trim Length is only 1")
                return Operand(f"'{res}'", self.getReturnType())
            else:
                assert mode == 'RIGHT'
                used_value = str(random.choice(values)[0])
                res = used_value[-2:]
                if len(res) == 1:
                    raise ValueError("Trim Length is only 1")
                return Operand(f"'{res}'", self.getReturnType())


## Structural Revision

@register_function
class EnumCube(UdfFunction):
    def getReturnType(self):
        return ListType(ListType(AnyValueType()))

    def execute(self, *args):
        assert len(args) == 1
        list_values = args[0]
        enum_flag_num = (1 << len(list_values)) - 1
        res = []
        while enum_flag_num > 0:
            temp_res = []
            for i, value in enumerate(list_values):
                if enum_flag_num & (1 << i) != 0:
                    temp_res.append(value)
            enum_flag_num -= 1
            res.append(temp_res)
        return res


def rep_node(node: TreeNode, rm_nodes: list[Operand], dialect: str):
    while len(node.children) == 1:
        node = node.children[0]
    string_node = str(node)
    for rm_node in rm_nodes:
        if no_space_and_case_insensitive_str_eq(string_node, rm_node.str_value()):
            if not inside_aggregate_function(dialect, node):
                node.value = 'NULL'
                node.is_terminal = True
                return
    for child in node.children:
        rep_node(child, rm_nodes, dialect)


@register_function
class ReviseSelectList(UdfFunction):
    def getReturnType(self):
        return ListType(AnyValueType())

    def execute(self, *args):
        assert len(args) == 3
        select_list = args[0]
        group_by_list = args[1]
        all_cols = args[2]
        res = []
        rm_cols = []
        for col in all_cols:
            if col not in group_by_list:
                rm_cols.append(col)
        for select_node in select_list:
            assert isinstance(select_node, Operand)
            assert isinstance(select_node.value, TreeNode)
            new_node = select_node.value.clone()
            rep_node(new_node, rm_cols, self.dialect)
            res.append(Operand(new_node, AnyValueType()))
        return res


@register_function
class ReviseSelectListSets(UdfFunction):
    def getReturnType(self):
        return ListType(AnyValueType())

    def execute(self, *args):
        assert len(args) == 3
        select_list = args[0]
        group_by_list = args[1]
        sets_elements_list = args[2]
        res = []
        rm_cols = []
        all_group_by_list = []
        all_group_by_sets = set()
        for col_list in sets_elements_list:
            for col in col_list:
                assert isinstance(col, Operand)
                if no_space_and_case_insensitive_str(col.str_value()) not in all_group_by_sets:
                    all_group_by_list.append(col)
                    all_group_by_sets.add(no_space_and_case_insensitive_str(col.str_value()))
        for all_col in all_group_by_list:
            flag = False
            for group_by_col in group_by_list:
                if no_space_and_case_insensitive_str_eq(group_by_col.str_value(), all_col.str_value()):
                    flag = True
            if not flag:
                rm_cols.append(all_col)
        for select_node in select_list:
            assert isinstance(select_node, Operand)
            assert isinstance(select_node.value, TreeNode)
            new_node = select_node.value.clone()
            rep_node(new_node, rm_cols, self.dialect)
            res.append(Operand(new_node, AnyValueType()))
        return res


@register_function
class SampleDate(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        if len(args) == 1:
            value_op = str(args[0]).strip().strip('\'')
            try:
                datetime.strptime(value_op, '%Y-%m-%d')
                return True
            except ValueError:
                return False
        else:
            value_op = str(args[0]).strip().strip('\'')
            assert args[1].str_value() == 'DD-MON-RR'
            try:
                datetime.strptime(value_op, '%d-%b-%y')
                return True
            except ValueError:
                print('parse Error-------------------')
                print(value_op)
                return False

    def execute(self, *args):
        if len(args) == 1:
            value_op = args[0].str_value()
            select_stmt_node = self.select_stmt_node
            execution_env = self.execution_env
            assert isinstance(select_stmt_node, TreeNode)
            values = sample_value(value_op, select_stmt_node, execution_env)
            if len(values) == 0:
                start_date = datetime(year=2000, month=1, day=1)
                end_date = datetime(year=2021, month=12, day=31)
                delta_days = (end_date - start_date).days
                random_days = random.randint(0, delta_days)
                used_value = start_date + timedelta(days=random_days)
            else:
                to_choice_values = []
                for value in values:
                    if isinstance(value[0], date):
                        to_choice_values.append(value[0])
                used_value = Counter(to_choice_values).most_common(1)[0][0]
            return Operand(f"'{used_value.strftime('%Y-%m-%d')}'", self.getReturnType())
        else:
            value_op = args[0].str_value()
            select_stmt_node = self.select_stmt_node
            execution_env = self.execution_env
            assert isinstance(select_stmt_node, TreeNode)
            values = sample_value(value_op, select_stmt_node, execution_env)
            if len(values) == 0:
                start_date = datetime(year=2000, month=1, day=1)
                end_date = datetime(year=2021, month=12, day=31)
                delta_days = (end_date - start_date).days
                random_days = random.randint(0, delta_days)
                used_value = start_date + timedelta(days=random_days)
            else:
                to_choice_values = []
                for value in values:
                    if isinstance(value[0], date):
                        to_choice_values.append(value[0])
                used_value = Counter(to_choice_values).most_common(1)[0][0]
            if used_value.year >= 2050:
                used_value = used_value.replace(year=used_value.year - 100)
            return Operand(f"'{used_value.strftime('%d-%b-%y')}'", self.getReturnType())


@register_function
class SampleDateOra(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def fulfill_cond(self, *args):
        value_op = str(args[0]).strip().strip('\'')
        try:
            datetime.strptime(value_op, '%d-%b-%y')
            return True
        except ValueError:
            try:
                datetime.strptime(value_op, '%b %d, %Y')
                return True
            except ValueError:
                return False

    def execute(self, *args):
        value_op = args[0].str_value()
        option = args[1].str_value()
        if option == 'DD-MON-RR':
            select_stmt_node = self.select_stmt_node
            execution_env = self.execution_env
            assert isinstance(select_stmt_node, TreeNode)
            values = sample_value(value_op, select_stmt_node, execution_env)
            if len(values) == 0:
                start_date = datetime(year=2000, month=1, day=1)
                end_date = datetime(year=2021, month=12, day=31)
                delta_days = (end_date - start_date).days
                random_days = random.randint(0, delta_days)
                used_value = start_date + timedelta(days=random_days)
            else:
                to_choice_values = []
                for value in values:
                    if isinstance(value[0], date):
                        to_choice_values.append(value[0])
                used_value = Counter(to_choice_values).most_common(1)[0][0]
            if used_value.year >= 2050:
                used_value = used_value.replace(year=used_value.year - 100)
            return Operand(f"'{used_value.strftime('%d-%b-%y')}'", self.getReturnType())
        else:
            assert option == 'Mon dd, YYYY'
            select_stmt_node = self.select_stmt_node
            execution_env = self.execution_env
            assert isinstance(select_stmt_node, TreeNode)
            values = sample_value(value_op, select_stmt_node, execution_env)
            if len(values) == 0:
                start_date = datetime(year=2000, month=1, day=1)
                end_date = datetime(year=2021, month=12, day=31)
                delta_days = (end_date - start_date).days
                random_days = random.randint(0, delta_days)
                used_value = start_date + timedelta(days=random_days)
            else:
                to_choice_values = []
                for value in values:
                    if isinstance(value[0], date):
                        to_choice_values.append(value[0])
                used_value = Counter(to_choice_values).most_common(1)[0][0]
            return Operand(f"'{used_value.strftime('%b %d, %Y')}'", self.getReturnType())


@register_function
class SampleTimeStamp(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        value_op = str(args[0]).strip().strip('\'')
        try:
            datetime.strptime(value_op, '%Y-%m-%d %H:%M:%S.%f')
            return True
        except ValueError:
            return False

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value()
        select_stmt_node = self.select_stmt_node
        execution_env = self.execution_env
        assert isinstance(select_stmt_node, TreeNode)
        values = sample_value(value_op, select_stmt_node, execution_env)
        if len(values) == 0:
            start_date = datetime(year=2000, month=1, day=1)
            end_date = datetime(year=2021, month=12, day=31)
            delta_days = (end_date - start_date).days
            random_days = random.randint(0, delta_days)
            random_date = start_date + timedelta(days=random_days)
            random_hours = random.randint(0, 23)
            random_minutes = random.randint(0, 59)
            random_seconds = random.randint(0, 59)
            used_value = random_date + timedelta(
                hours=random_hours,
                minutes=random_minutes,
                seconds=random_seconds
            )
        else:
            to_choice_values = []
            for value in values:
                if value[0] is not None:
                    to_choice_values.append(value[0])
            used_value = Counter(to_choice_values).most_common(1)[0][0]
        return Operand(f"'{used_value.strftime('%Y-%m-%d %H:%M:%S.%f')}'", self.getReturnType())


@register_function
class OraMySQLOrderBy(UdfFunction):
    def getReturnType(self):
        return ListType(BaseType(''))

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], list)
        res = []
        for op in args[0]:
            nulls_first_flag = False
            nulls_last_flag = False
            asc_flag = True
            desc_flag = False
            assert isinstance(op, Operand)
            node = op.value
            assert isinstance(node, TreeNode)
            expression_node = node.get_child_by_value('expression')
            if node.get_child_by_value('DESC') is not None:
                desc_flag = True
                asc_flag = False
            if node.get_child_by_value('NULLS') is not None:
                if node.get_child_by_value('FIRST') is not None:
                    nulls_first_flag = True
                elif node.get_child_by_value('LAST') is not None:
                    nulls_last_flag = True
            if asc_flag and nulls_first_flag:
                order_by_item = f"{str(expression_node)} ASC"
            elif asc_flag:
                order_by_item = f"{str(expression_node)} IS NULL, {str(expression_node)}"
            elif desc_flag and nulls_last_flag:
                order_by_item = f"{str(expression_node)} DESC"
            elif desc_flag:
                order_by_item = f"{str(expression_node)} IS NULL DESC, {str(expression_node)} DESC"
            else:
                assert False
            res.append(Operand(order_by_item, BaseType('')))
        return res


@register_function
class PgMySQLOrderBy(UdfFunction):
    def getReturnType(self):
        return ListType(BaseType(''))

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], list)
        res = []
        for op in args[0]:
            nulls_first_flag = False
            nulls_last_flag = False
            asc_flag = True
            desc_flag = False
            if isinstance(op, Operand):
                op = op.value
            assert isinstance(op, TreeNode)
            expression_node = op.get_child_by_value('a_expr')
            asc_node = op.get_child_by_value('opt_asc_desc')
            nulls_node = op.get_child_by_value('opt_nulls_order')
            if asc_node is not None and asc_node.get_child_by_value('DESC') is not None:
                asc_flag = False
                desc_flag = True
            if nulls_node is not None and nulls_node.get_child_by_value('FIRST') is not None:
                nulls_first_flag = True
            elif nulls_node is not None and nulls_node.get_child_by_value('LAST') is not None:
                nulls_last_flag = True
            if asc_flag and nulls_first_flag:
                order_by_item = f"{str(expression_node)} ASC"
            elif asc_flag:
                order_by_item = f"{str(expression_node)} IS NULL, {str(expression_node)}"
            elif desc_flag and nulls_last_flag:
                order_by_item = f"{str(expression_node)} DESC"
            elif desc_flag:
                order_by_item = f"{str(expression_node)} IS NULL DESC, {str(expression_node)} DESC"
            else:
                assert False
            res.append(Operand(order_by_item, BaseType('')))
        return res


@register_function
class MySQLOrderBy(UdfFunction):
    def getReturnType(self):
        return ListType(BaseType(''))

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], list)
        res = []
        for op in args[0]:
            asc_flag = True
            desc_flag = False
            print(type(op))
            assert isinstance(op, Operand)
            node = op.value
            assert isinstance(node, TreeNode)
            expression_node = node.get_child_by_value('expression')
            if node.get_child_by_value('DESC') is not None:
                asc_flag = False
                desc_flag = True
            if asc_flag:
                order_by_item = f"{str(expression_node)} ASC NULLS FIRST"
            elif desc_flag:
                order_by_item = f"{str(expression_node)} DESC NULLS LAST"
            else:
                assert False
            res.append(Operand(order_by_item, BaseType('')))
        return res


@register_function
class GenMySQLTimeFormat(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 0
        if random.randint(1, 3) == 1:
            fmt_str = '%Y-%m-%d'
        elif random.randint(1, 3) == 1:
            fmt_str = '%d/%m/%Y'
        else:
            fmt_str = '%b %d, %Y'
        return Operand(f"'{fmt_str}'", self.getReturnType())


@register_function
class GenOraPgTimeFormat(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 0
        if random.randint(1, 3) == 1:
            fmt_str = 'YYYY-MM-DD'
        elif random.randint(1, 3) == 1:
            fmt_str = 'DD/MM/YYYY'
        else:
            fmt_str = 'Mon DD, YYYY'
        return Operand(f"'{fmt_str}'", self.getReturnType())


@register_function
class GenDateLikePat(UdfFunction):
    def getReturnType(self):
        return StringLiteralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value()
        select_stmt_node = self.select_stmt_node
        execution_env = self.execution_env
        assert isinstance(select_stmt_node, TreeNode)
        values = sample_value(value_op, select_stmt_node, execution_env)
        to_choice_values = []
        for value in values:
            if value[0] is not None:
                to_choice_values.append(value[0])
        used_value = Counter(to_choice_values).most_common(1)[0][0]
        if random.randint(1, 2) == 1:
            return Operand(f"'%{used_value.strftime('%Y')}%'", self.getReturnType())
        else:
            return Operand(f"'%{used_value.strftime('%m')}%'", self.getReturnType())


@register_function
class GenXmlPath(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value()
        select_stmt_node = self.select_stmt_node
        execution_env = self.execution_env
        assert isinstance(select_stmt_node, TreeNode)
        values = sample_value(value_op, select_stmt_node, execution_env)
        to_choice_values = []
        for value in values:
            if value[0] is not None:
                to_choice_values.append(value[0])
        used_value = Counter(to_choice_values).most_common(1)[0][0]
        root = ET.fromstring(used_value)
        elements = [child.tag for child in root]
        selected = random.choice(elements)
        return Operand(f'\'/{root.tag}/{selected}\'', self.getReturnType())


@register_function
class GenCountLiteral(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value()
        select_stmt_node = self.select_stmt_node
        execution_env = self.execution_env
        assert isinstance(select_stmt_node, TreeNode)
        values = sample_value(value_op, select_stmt_node, execution_env)
        to_choice_values = []
        for value in values:
            if value[0] is not None:
                to_choice_values.append(value[0])
        used_value = Counter(to_choice_values).most_common(1)[0][0]
        root = ET.fromstring(used_value)
        elements = [child.tag for child in root]
        selected = random.choice(elements)
        return Operand(f'\'count(/{root.tag}/{selected})\'', self.getReturnType())


@register_function
class TranCountLiteralOra(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def fulfill_cond(self, *args):
        assert len(args) == 1
        value_op = str(args[0]).strip().strip('\'')
        match = re.search(r'count\s*\(\s*(.*?)\s*\)', value_op, re.IGNORECASE)
        if match:
            return True
        return False

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value().strip('\'')
        match = re.search(r'count\s*\(\s*(.*?)\s*\)', value_op, re.IGNORECASE)
        assert match
        path = f'\'{match.group(1)}\''
        return Operand(path, self.getReturnType())


@register_function
class GenCountXpath(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value().strip('\'')
        return Operand(f'\'count({value_op})\'', self.getReturnType())


@register_function
class GenIlikePattern(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value()
        select_stmt_node = self.select_stmt_node
        execution_env = self.execution_env
        assert isinstance(select_stmt_node, TreeNode)
        values = sample_value(value_op, select_stmt_node, execution_env)
        to_choice_values = []
        for value in values:
            if value[0] is not None:
                to_choice_values.append(value[0])
        used_value = Counter(to_choice_values).most_common(1)[0][0]
        assert isinstance(used_value, str)
        strings = used_value.split('\\')
        for string in strings:
            if len(string) != 0:
                return Operand(f'\'%{string.upper()}%\'', self.getReturnType())
        assert False


@register_function
class GenRegPattern(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0].str_value()
        select_stmt_node = self.select_stmt_node
        execution_env = self.execution_env
        assert isinstance(select_stmt_node, TreeNode)
        values = sample_value(value_op, select_stmt_node, execution_env)
        to_choice_values = []
        for value in values:
            if value[0] is not None:
                to_choice_values.append(value[0])
        if len(to_choice_values) == 0:
            return None
        used_value = Counter(to_choice_values).most_common(1)[0][0]
        assert isinstance(used_value, str)
        strings = used_value.split('\\')
        final_str = ''
        for string in strings:
            if len(string) != 0:
                return Operand(f'\'.*{string}.*\'', self.getReturnType())
        assert False


@register_function
class MySQLGetValueFormat(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0]
        print(value_op)
        assert isinstance(value_op, Operand)
        assert value_op.semantic_attribute is not None
        assert value_op.semantic_attribute.is_date
        format = value_op.semantic_attribute.date_format
        if 'RR' in format or ('YY' in format and 'YYYY' not in format):
            raise ValueError('Don\' use RR')
        fmt = (format.replace('YYYY', '%Y').replace('HH12', '%I').replace('MI', '%i').replace('SS', '%S').
               replace('SS', '%s').replace('FF', '%f').replace('HH24', '%H').replace('HH', '%h').
               replace('MONTH', '%M').replace('Mon', '%b').replace('MM', '%m').replace('DD', '%d').
               replace('Dy', '%a').replace('AM', '%p').replace('RR', '%y'))
        return Operand(f'\'{fmt}\'', self.getReturnType())


@register_function
class OraPgGetFormat(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value_op = args[0]
        assert isinstance(value_op, Operand)
        assert value_op.semantic_attribute is not None
        assert value_op.semantic_attribute.is_date
        format = value_op.semantic_attribute.date_format
        if 'RR' in format or ('YY' in format and 'YYYY' not in format):
            raise ValueError('Don\' use RR')
        return Operand(f"'{format}'", self.getReturnType())


@register_function
class GenSubString(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        from sql_gen.generator.element.Pattern import NumberLiteralSlot
        assert len(args) == 3
        value_op = args[0].str_value()
        values = sample_value(value_op, self.select_stmt_node, self.execution_env)
        to_choice_values = []
        for value in values:
            if value[0] is not None:
                to_choice_values.append(value[0])
        if len(to_choice_values) == 0:
            raise ValueError('No value to choice')
        used_value = Counter(to_choice_values).most_common(1)[0][0]
        if isinstance(args[1], NumberLiteralSlot):
            begin_pos = args[1].num
        else:
            begin_pos = int(args[1].str_value())
        if isinstance(args[2], NumberLiteralSlot):
            occurrence = args[2].num
        else:
            occurrence = int(args[2].str_value())
        used_str = used_value[begin_pos:]
        slice_len = random.randint(1, 3)
        choice_space = len(used_str) - slice_len
        if choice_space < 0:
            raise ValueError('No value to choice')
        begin_pos = random.randint(0, choice_space - 1)
        final_str = used_str[begin_pos: begin_pos + slice_len]
        return Operand(f'\'{final_str}\'', self.getReturnType())


@register_function
class GenToCharFmt(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        value = int(args[0].str_value())
        res = "9999999999"
        if value != 0:
            res = res + '.'
            for i in range(value):
                res = res + '0'
        return Operand(f'\'{res}\'', self.getReturnType())
