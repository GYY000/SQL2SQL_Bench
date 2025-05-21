# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: udf_entry$
# @Author: 10379
# @Time: 2025/4/14 12:30
import random
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from sql_gen.generator.ele_type.type_def import StringGeneralType, NumberType
from sql_gen.generator.element.Operand import Operand

function_registry = {}


def register_function(cls):
    function_registry[cls.__name__] = cls
    return cls


class UdfFunction(ABC):

    @abstractmethod
    def getReturnType(self):
        pass

    @abstractmethod
    def execute(self, *args):
        if not all(isinstance(arg, Operand) for arg in args):
            raise ValueError("所有参数必须是Operand类型")
        pass


def getReturnType(class_name):
    if class_name not in function_registry:
        raise ValueError(f"Class {class_name} not found.")
    cls = function_registry[class_name]
    instance = cls()
    func_name = 'getReturnType'
    if not hasattr(instance, func_name):
        raise AttributeError(f"{instance.__class__.__name__} has no method named {func_name}")
    method = getattr(instance, func_name)
    return method()


def execute(class_name, *args):
    if class_name not in function_registry:
        raise ValueError(f"Class {class_name} not found.")
    cls = function_registry[class_name]
    instance = cls()
    func_name = 'execute'
    if not hasattr(instance, func_name):
        raise AttributeError(f"{instance.__class__.__name__} has no method named {func_name}")
    if not all(isinstance(arg, Operand) for arg in args):
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
        # Currently don't use '%W %M because they will cause indent difference between Oracle, PG and MySQL.
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
        print(format_str)
        assert isinstance(format_str, str)
        format_str = (format_str.replace('%Y', 'YYYY').replace('%I', 'HH12').
                      replace('%i', 'MI').replace('%S', 'SS').replace('%s', 'SS').
                      replace('%f', 'FF').replace('%H', 'HH24').replace('%h', 'HH').
                      replace('%M', 'MONTH').replace('%b', 'Mon').replace('%m', 'MM').
                      replace('%d', 'DD').replace('%a', 'Dy').replace('%p', 'AM').
                      replace('%y', 'RR'))
        return Operand(f"{format_str}", self.getReturnType())


@register_function
class GenTimestampOracleStr(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 0
        # gen random_time
        start = datetime(year=1950, month=1, day=1)
        end = datetime(year=2049, month=12, day=31, hour=23, minute=59, second=59)
        delta = (end - start).total_seconds()
        random_second = random.uniform(0, delta)
        time = start + timedelta(seconds=random_second)
        if args[0].str_value() == 'DD-MON-RR HH.MI.SSXFF AM':
            month = time.strftime('%b')
            month = month.upper()
            time_str1 = time.strftime(f'%d-')
            time_str3 = time.strftime(f'-%y %H.%M.%S.%f %p')
            time_str = time_str1 + month + time_str3
        elif args[0].str_value() == 'Mon dd, YYYY, HH:MI:SS':
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


@register_function
class GenTimestampOracleStr(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 0
        # gen random_time
        start = datetime(year=1950, month=1, day=1)
        end = datetime(year=2049, month=12, day=31, hour=23, minute=59, second=59)
        delta = (end - start).total_seconds()
        random_second = random.uniform(0, delta)
        time = start + timedelta(seconds=random_second)
        if args[0].str_value() == 'DD-MON-RR HH.MI.SSXFF AM':
            month = time.strftime('%b')
            month = month.upper()
            time_str1 = time.strftime(f'%d-')
            time_str3 = time.strftime(f'-%y %I.%M.%S.%f %p')
            time_str = time_str1 + month + time_str3
        elif args[0].str_value() == 'Mon dd, YYYY, HH:MI:SS':
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
        elif args[1].str_value() == 'Mon dd, YYYY, HH:MI:SS':
            dt = datetime.strptime(time_str, '%b %d, %Y, %H:%M:%S')
            return Operand(f"'{dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'", self.getReturnType())
        else:
            assert False


@register_function
class GenPGDateParaStr(UdfFunction):
    def getReturnType(self):
        return StringGeneralType()

    def execute(self, *args):
        assert len(args) == 1
        assert isinstance(args[0], Operand)
        year = random.randint(1950, 2049)
        month = random.randint(1, 12)
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
        date_value_str = args[1].str_value()
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
        number = args[0].str_value(-1000000, 1000000)
        if number < 0:
            sign = '-'
            number = -number
        else:
            sign = ''
        decimal = (number % 100)
        val = (number - decimal) / 100
        above1000 = val / 1000
        below1000 = val - above1000 * 1000
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
        value_op = args[0].str_value()
        type_op = args[1].str_value()
        if type_op == '.,':
            num_str = value_op.replace(',', '')
            return Operand(f"{num_str}", self.getReturnType())
        else:
            num_str = value_op.replace('.', '').replace(',', '.')
            return Operand(f"{num_str}", self.getReturnType())

# Numeric UserDefineFunctions


# StringUserDefineFunctions
