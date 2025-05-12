# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: udf_entry$
# @Author: 10379
# @Time: 2025/4/14 12:30
import random
from abc import ABC, abstractmethod

from sql_gen.generator.ele_type.type_def import StringGeneralType
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
        format_str = args[0].value
        assert isinstance(format_str, str)
        format_str = (format_str.replace('%Y', 'YYYY').replace('%I', 'HH12').
                      replace('%i', 'MI').replace('%S', 'SS').replace('%s', 'SS').
                      replace('%f', 'FF').replace('%H', 'HH24').replace('%h', 'HH').
                      replace('%M', 'MONTH').replace('%b', 'Mon').replace('%m', 'MM').
                      replace('%d', 'DD').replace('%a', 'Dy').replace('%p', 'AM').
                      replace('%y', 'RR'))
        return Operand(f"{format_str}", self.getReturnType())
