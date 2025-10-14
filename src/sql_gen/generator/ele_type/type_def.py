# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: type_def$
# @Author: 10379
# @Time: 2025/3/24 19:59
import json
import random
from abc import ABC, abstractmethod
from collections import Counter
from datetime import datetime

import faker
from faker import Faker

from sql_gen.generator.ele_type.Attribute import AttributeContainer
from sql_gen.generator.ele_type.SemanticAttribute import SemanticAttribute
from utils.tools import date_format_trans, scale_name_into_length


def type_json_build(type_name, type_attributes, attr_container):
    res = {'type_name': type_name}
    assert type_name is not None
    if type_attributes is not None:
        for key, value in type_attributes.items():
            res[key] = value
    if attr_container is not None:
        res['attr_container'] = attr_container
    return res


class BaseType(dict, ABC):
    def __init__(self, type_name, type_attributes=None, attr_container: AttributeContainer = AttributeContainer()):
        dict.__init__(self, type_json_build(type_name, type_attributes, attr_container))
        self.attr_container = attr_container

    def get_str_attributes(self):
        if self.attr_container is None:
            return ''
        return ', '.join([str(attr) for attr in self.attr_container.attributes])

    @abstractmethod
    def __str__(self):
        return 'BaseType' + self.get_str_attributes()

    @abstractmethod
    def get_type_name(self, dialect: str) -> str | None:
        return "BaseType"

    @abstractmethod
    def gen_value(self, dialect: str, value=None) -> str | None:
        return str(value)

    def gen_demo_value(self):
        return f"element"

    @abstractmethod
    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        pass


class ExpressionType(BaseType):
    def __init__(self, type_name=None, attributes: dict = None,
                 attr_container: AttributeContainer = AttributeContainer()):
        super().__init__(type_name if type_name is not None else 'EXPRESSION', attributes, attr_container)

    def get_type_name(self, dialect: str) -> str | None:
        return 'EXPRESSION'

    def __str__(self):
        return 'EXPRESSION' + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        return None

    def gen_demo_value(self):
        return '1'

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        can_be_used_cols = []
        type1 = self
        if self.attr_container.has_number():
            can_be_used_cols = []
            for col in usable_cols:
                assert isinstance(col, Operand)
                if (isinstance(col.op_type, StringGeneralType) and
                        not col.semantic_attribute is None and
                        col.semantic_attribute.number):
                    can_be_used_cols.append(col)
            if len(can_be_used_cols) > 0:
                value = random.choice(can_be_used_cols)
                usable_cols.remove(value)
                return value
            else:
                value = random.randint(1, 1500)
                semantic_attr = SemanticAttribute({"NUMBER": True})
                return Operand(f"\'{str(value)}\'", StringGeneralType(), semantic_attr)
        else:
            for col in usable_cols:
                type2 = col.op_type
                if col.semantic_attribute is not None and col.semantic_attribute.non_arithmetical:
                    continue
                if isinstance(self, AnyValueType):
                    can_be_used_cols.append(col)
                else:
                    if type(type2).__name__ == type(self).__name__:
                        can_be_used_cols.append(col)
                    if self.attr_container.has_strict():
                        continue
                    if is_num_type(type1) and is_num_type(type2):
                        can_be_used_cols.append(col)
                    elif is_str_type(type1) and is_str_type(type2):
                        can_be_used_cols.append(col)
                    elif is_time_type(type1) and is_time_type(type2):
                        can_be_used_cols.append(col)
            if len(can_be_used_cols) == 0:
                return None
            else:
                value = random.choice(can_be_used_cols)
                usable_cols.remove(value)
                return value


class NumberType(ExpressionType):
    def __init__(self, type_name=None, attributes: dict = None,
                 attr_container: AttributeContainer = AttributeContainer()):
        super().__init__(type_name if type_name is not None else 'NUMBER', attributes, attr_container)

    def get_type_name(self, dialect: str):
        return "NUMBER"

    def __str__(self):
        return "NUMBER" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return str(value)

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        if self.attr_container.has_range() or self.attr_container.has_literal():
            from sql_gen.generator.element.Operand import Operand
            min_range, max_range = self.attr_container.get_int_allowed_range()
            if min_range is None:
                min_range = -10
                max_range = 10
            return Operand(str(random.randint(min_range, max_range)), BaseType(''))
        else:
            return super().generate_value(usable_cols, root_node, length, src_dialect, execution_env)


class IntGeneralType(NumberType):
    def __init__(self, type_name=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__(type_name if type_name is not None else 'INT_GENERAL', None, attr_container)

    def __str__(self):
        return 'INT_GENERAL' + self.get_str_attributes()

    def get_type_name(self, dialect: str):
        return 'INT_GENERAL'

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, int) or isinstance(value, str)
            return str(int(value))


class IntType(IntGeneralType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("INT", attr_container)

    def __str__(self):
        return 'INT' + self.get_str_attributes()

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return 'NUMBER'
        else:
            return 'INT'

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, int) or isinstance(value, str)
            return str(int(value))


class BigIntType(IntGeneralType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("BIGINT", attr_container)

    def __str__(self):
        return 'INT' + self.get_str_attributes()

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return 'NUMBER(19)'
        else:
            return 'BIGINT'

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, int) or isinstance(value, str)
            return str(int(value))


class BoolType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("BOOL", None, attr_container)

    def __str__(self):
        return 'BOOL' + self.get_str_attributes()

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return 'NUMBER(1)'
        else:
            return 'BOOL'

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, bool)
            if dialect == 'oracle':
                if value:
                    return '1'
                else:
                    return '0'
            else:
                if value:
                    return 'True'
                else:
                    return 'False'

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str,
                       execution_env):
        from sql_gen.generator.fetch_operand_type import sample_value
        from sql_gen.generator.element.Operand import Operand
        date_cols = []
        str_cols = []
        for col in usable_cols:
            assert isinstance(col, Operand)
            if isinstance(col.op_type, DateType):
                date_cols.append(col)
            elif isinstance(col.op_type, StringGeneralType):
                str_cols.append(col)
        all_usable_cols = date_cols + str_cols
        used_col = random.choice(all_usable_cols)
        if isinstance(used_col.op_type, StringGeneralType):
            try:
                values = sample_value(used_col.str_value(), root_node, execution_env)
                to_choice_values = []
                for value in values:
                    if value[0] is not None:
                        to_choice_values.append(value[0])
                if len(to_choice_values) == 0:
                    return None
                used_value = Counter(to_choice_values).most_common(1)[0][0]
                strings = used_value.split()
                for string in strings:
                    if len(string) != 0:
                        return Operand(f'{used_col.str_value()} LIKE \'%{string}%\'', BoolType())
            except Exception as e:
                raise e
        elif isinstance(used_col.op_type, DateType):
            try:
                values = sample_value(used_col.str_value(), root_node, execution_env)
                to_choice_values = []
                for value in values:
                    if value[0] is not None:
                        to_choice_values.append(value[0])
                if len(to_choice_values) == 0:
                    return None
                used_value = Counter(to_choice_values).most_common(1)[0][0]
                if not isinstance(used_value, datetime):
                    return None
                else:
                    month = used_value.month
                    return Operand(f'EXTRACT(MONTH FROM {used_col.str_value()}) = {month}', BoolType())
            except Exception as e:
                raise e


class FloatGeneralType(NumberType):
    def __init__(self, type_name=None, type_attributes: dict = None,
                 attr_container: AttributeContainer = AttributeContainer()):
        super().__init__(type_name if type_name is not None else 'FLOAT', type_attributes, attr_container)

    def get_type_name(self, dialect: str):
        return f'FLOAT'

    def __str__(self):
        return f"FLOAT" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, float) or isinstance(value, str)
            return str(float(value))


class DecimalType(FloatGeneralType):
    def __init__(self, precision, scale, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("DECIMAL", {"precision": precision, "scale": scale}, attr_container)
        self.precision = precision
        self.scale = scale

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'NUMBER({self.precision}, {self.scale})'
        else:
            return f'DECIMAL({self.precision}, {self.scale})'

    def __str__(self):
        return f"DECIMAL({self.precision},{self.scale})" + self.get_str_attributes()


class DoubleType(FloatGeneralType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("DOUBLE", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'BINARY_DOUBLE'
        elif dialect == 'pg':
            return 'DOUBLE PRECISION'
        elif dialect == 'mysql':
            return 'DOUBLE'
        else:
            assert False

    def __str__(self):
        return "DOUBLE"


class FloatType(FloatGeneralType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("FLOAT", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'BINARY_FLOAT'
        elif dialect == 'pg':
            return 'REAL'
        elif dialect == 'mysql':
            return 'FLOAT'
        else:
            assert False

    def __str__(self):
        return "DOUBLE"


class DateType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("DATE", None, attr_container)

    def get_type_name(self, dialect: str):
        return 'DATE'

    def __str__(self):
        return "DATE" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if not isinstance(value, dict):
                used_value = value
                used_format = 'YYYY-MM-DD'
            else:
                used_value = value['value']
                used_format = value['format']
            if dialect == 'mysql':
                date_format = date_format_trans(used_format)
                return f"STR_TO_DATE('{used_value}', '{date_format}')"
            elif dialect == 'pg':
                return f"TO_DATE('{used_value}', '{used_format}')"
            elif dialect == 'oracle':
                return f"TO_DATE('{used_value}', '{used_format}')"


class TimeType(ExpressionType):
    def __init__(self, fraction=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("TIME", {"fraction": fraction}, attr_container)
        self.fraction = fraction

    def get_type_name(self, dialect: str):
        if self.fraction is None:
            if dialect == 'oracle' or dialect == 'pg':
                return f'INTERVAL DAY TO SECOND'
            else:
                return f'TIME'
        else:
            if dialect == 'oracle' or dialect == 'pg':
                return f'INTERVAL DAY TO SECOND({self.fraction})'
            else:
                return f'TIME({self.fraction})'

    def __str__(self):
        if self.fraction is None:
            return 'TIME' + self.get_str_attributes()
        else:
            return f'TIME({self.fraction})' + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            if dialect == 'mysql':
                return f"CAST('{value}' AS {self.get_type_name(dialect)})"
            else:
                parts = value.split(':')
                negative = False
                hours = int(parts[0])
                if hours < 0:
                    negative = True
                    hours = -1 * hours
                minutes = int(parts[1])
                seconds = float(parts[2])
                days = hours // 24
                hours %= 24
                if negative:
                    days = -1 * days
                if dialect == 'oracle':
                    if self.fraction is None:
                        return (f"INTERVAL '{days} {hours:02}:{minutes:02}:{seconds:02}' "
                                f"DAY TO SECOND")
                    else:
                        return (f"INTERVAL '{days} {hours:02}:{minutes:02}:{seconds:02}' "
                                f"DAY TO SECOND({self.fraction})")
                elif dialect == 'pg':
                    return (f"'{days} days {hours}:{minutes}:{seconds:02}'::"
                            f"{self.get_type_name(dialect)}")


class YearType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("YEAR", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'YEAR'
        elif dialect == 'pg':
            return 'SMALLINT'
        elif dialect == 'oracle':
            return 'NUMBER(4)'

    def __str__(self):
        return "YEAR" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, int) or isinstance(value, str)
            return str(value)


class TimestampType(ExpressionType):
    def __init__(self, fraction=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("TIMESTAMP", {"fraction": fraction}, attr_container)
        self.fraction = fraction

    def get_type_name(self, dialect: str):
        if self.fraction is None:
            return f'TIMESTAMP'
        else:
            return f'TIMESTAMP({self.fraction})'

    def __str__(self):
        if self.fraction is None:
            return 'TIMESTAMP' + self.get_str_attributes()
        else:
            return f'TIMESTAMP{self.fraction}' + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if isinstance(value, dict):
                used_value = value['value']
                used_format = value['format']
            else:
                used_value = value
                used_format = 'YYYY-MM-DD HH24:MI:SS'
            timestamp_obj = datetime.strptime(used_value, date_format_trans(used_format))
            formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            if dialect == 'oracle':
                return f"TO_TIMESTAMP('{used_value}', '{used_format}')"
            elif dialect == 'pg':
                return f"'{formatted_timestamp_str}'::timestamp"
            elif dialect == 'mysql':
                return f"TIMESTAMP('{formatted_timestamp_str}')"


class DatetimeType(ExpressionType):
    def __init__(self, fraction=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("DATETIME", {"fraction": fraction}, attr_container)
        self.fraction = fraction

    def get_type_name(self, dialect: str):
        if self.fraction is None:
            if dialect == 'mysql':
                return 'DATETIME'
            else:
                return 'TIMESTAMP'
        else:
            if dialect == 'mysql':
                return f'DATETIME({self.fraction})'
            else:
                return f'TIMESTAMP({self.fraction})'

    def __str__(self):
        if self.fraction is None:
            return 'DATETIME' + self.get_str_attributes()
        else:
            return f'DATETIME({self.fraction})' + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if isinstance(value, dict):
                used_value = value['value']
                used_format = value['format']
            else:
                used_value = value
                used_format = 'YYYY-MM-DD HH24:MI:SS'
            used_format = date_format_trans(used_format)
            timestamp_obj = datetime.strptime(used_value, used_format)
            formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            if dialect == 'mysql':
                return f"STR_TO_DATE('{formatted_timestamp_str}', '%Y-%m-%d %H:%i:%s')"
            else:
                if dialect == 'oracle':
                    return f"TO_TIMESTAMP('{formatted_timestamp_str}', 'YYYY-MM-DD HH24:MI:SS')"
                elif dialect == 'pg':
                    return f"'{formatted_timestamp_str}'::timestamp"


class IntervalType(ExpressionType):
    # TODO
    def __init__(self, typename=None, units: list = None, attr_container: AttributeContainer = AttributeContainer()):
        """
        units can be one of:
        microsecond, second, minute, hour, day, week, month, quarter, year, decade
        century, millennium
        """
        super().__init__("INTERVAL", {"units": units}, attr_container)
        self.units = units

    def __str__(self):
        if self.units is None:
            return 'INTERVAL' + self.get_str_attributes()
        else:
            return f'INTERVAL({self.units})' + self.get_str_attributes()

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        if random.randint(1, 2) == 1:
            days = random.randint(1, 5)
            return Operand(f"INTERVAL '{days}' DAY", BaseType(''))
        else:
            months = random.randint(1, 3)
            return Operand(f"INTERVAL '{months}' MONTH", BaseType(''))


class IntervalYearMonthType(IntervalType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("INTERVAL YEAR TO MONTH", ['year', 'month'], attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'oracle' or dialect == 'pg':
            return 'INTERVAL YEAR TO MONTH'
        else:
            return None

    def __str__(self):
        return "INTERVAL YEAR TO MONTH"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, dict)
            if dialect == 'pg':
                if not value['sign']:
                    value['year'] = -1 * value['year']
                return f"'{value['year']} years {value['month']} months'::INTERVAL YEAR TO MONTH"
            elif dialect == 'oracle':
                if not value['sign']:
                    value['year'] = -1 * value['year']
                return f"to_yminterval('{value['year']}-{value['month']}')"
            else:
                return None


class TimestampTZType(ExpressionType):
    def __init__(self, fraction=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("TIMESTAMPTZ", {"fraction": fraction}, attr_container)
        self.fraction = fraction

    def __str__(self):
        if self.fraction is None:
            return 'TIMESTAMP WITH TIME ZONE'
        else:
            return f'TIMESTAMP WITH TIME ZONE({self.fraction})'

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            return None

    def get_type_name(self, dialect: str):
        if self.fraction is None:
            if dialect == 'mysql':
                return 'TIMESTAMP'
            else:
                return 'TIMESTAMP WITH TIME ZONE'
        else:
            if dialect == 'mysql':
                return f'TIMESTAMP({self.fraction})'
            else:
                return f'TIMESTAMP WITH TIME ZONE({self.fraction})'


class StringGeneralType(ExpressionType):
    def __init__(self, type_name=None, attributes: dict = None,
                 attr_container: AttributeContainer = AttributeContainer()):
        super().__init__(type_name if type_name is not None else 'STRING', attributes, attr_container)

    def get_type_name(self, dialect: str):
        return "STRING"

    def __str__(self):
        return "STRING" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            # value = value.replace('\\n', '\n')
            if len(value.encode('utf-8')) > 4000:
                # print(f"Warning: {value} is too long for oracle")
                return 'NULL'
            if dialect == 'mysql':
                value = (value.replace('\\', '\\\\').
                         replace('-', '\\-').replace('\n', '\\n'))
            return f"\'{value}\'"

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        if self.attr_container.has_number():
            can_be_used_cols = []
            for col in usable_cols:
                assert isinstance(col, Operand)
                if (isinstance(col.op_type, StringGeneralType) and col.semantic_attribute is not None
                        and col.semantic_attribute.number):
                    can_be_used_cols.append(col)
            if len(can_be_used_cols) > 0:
                value = random.choice(can_be_used_cols)
                usable_cols.remove(value)
                return value
            else:
                return None
                # value = random.randint(1, 1500)
                # semantic_attr = SemanticAttribute({"NUMBER": True})
                # return Operand(f"\'{str(value)}\'", StringGeneralType(), semantic_attr)
        elif self.attr_container.has_date():
            can_be_used_cols = []
            for col in usable_cols:
                assert isinstance(col, Operand)
                if (isinstance(col.op_type, StringGeneralType) and
                        col.semantic_attribute is not None and
                        col.semantic_attribute.is_date and
                        'YYYY' in col.semantic_attribute.date_format):
                    can_be_used_cols.append(col)
            if len(can_be_used_cols) > 0:
                return random.choice(can_be_used_cols)
            else:
                return None
                # from datetime import datetime, timedelta
                # start_date = datetime(year=2020, month=1, day=1)
                # end_date = datetime(year=2025, month=12, day=31)
                # delta = end_date - start_date
                # random_days = random.randint(0, delta.days)
                # random_date = start_date + timedelta(days=random_days)
                # if random.randint(1, 2) == 1:
                #     semantic_attr = SemanticAttribute({"DATE": 'YYYY-MM-DD'})
                #     return Operand(f"\'{str(random_date.strftime('%Y-%m-%d'))}\'",
                #                    StringGeneralType(), semantic_attr)
                # else:
                #     semantic_attr = SemanticAttribute({"DATE": 'DD/MM/YYYY'})
                #     return Operand(f"\'{str(random_date.strftime('%d/%m/%Y'))}\'",
                #                    StringGeneralType(), semantic_attr)
        else:
            return super().generate_value(usable_cols, root_node, length, src_dialect, execution_env)


class VarcharType(StringGeneralType):
    def __init__(self, length=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("VARCHAR", {"length": length}, attr_container)
        self.length = length

    def get_type_name(self, dialect: str):
        length = self.length
        if length is None:
            length = 400
        if dialect == 'oracle':
            return f'VARCHAR2({length})'
        else:
            return f'VARCHAR({length})'

    def __str__(self):
        return f"VARCHAR({self.length})" + self.get_str_attributes()


class EnumType(StringGeneralType):
    def __init__(self, values: list, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("Enum", {"values": values}, attr_container)
        self.values = values
        self.len = 0
        for value in self.values:
            if self.len < len(value) + 5:
                self.len = len(value) + 5

    def get_type_name(self, dialect: str):
        values = ''
        for value in self.values:
            if values != '':
                values = values + ', '
            values = values + f"'{value}'"
        if dialect == 'mysql':
            return f"ENUM({values})"
        elif dialect == 'oracle':
            return f'VARCHAR2({self.len})'
        else:
            return f'VARCHAR({self.len})'

    def __str__(self):
        return f"VARCHAR({self.len})" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            assert value in self.values
            if dialect == 'mysql':
                value = value.replace('\\', '\\\\').replace('-', '\\-')
            return f"\'{value}\'"


class NvarcharType(StringGeneralType):
    def __init__(self, length, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("NVARCHAR", {"length": length}, attr_container)
        self.length = length

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return f"NVARCHAR({self.length})"
        elif dialect == 'pg':
            return f'VARCHAR({self.length})'
        elif dialect == 'oracle':
            return f"VARCHAR2({self.length} CHAR)"

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        can_be_used_cols = []
        for col in usable_cols:
            type2 = col.op_type
            if type(type2).__name__ == type(self).__name__:
                can_be_used_cols.append(col)
        if len(can_be_used_cols) == 0:
            return None
        else:
            value = random.choice(can_be_used_cols)
            usable_cols.remove(value)
            return value

    def __str__(self):
        return f"NVARCHAR({self.length})" + self.get_str_attributes()


class CharType(StringGeneralType):
    # using varchar instead of char in case padding comparison
    def __init__(self, length, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("VARCHAR", {"length": length}, attr_container)
        self.length = length

    def get_type_name(self, dialect: str):
        # if dialect == 'mysql' and self.length > 255:
        #     return "TEXT"
        # return f'CHAR({self.length})'
        if dialect == 'oracle':
            return f'VARCHAR2({self.length})'
        else:
            return f'VARCHAR({self.length})'

    def __str__(self):
        return f"VARCHAR({self.length})" + self.get_str_attributes()


class TextType(StringGeneralType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("TEXT", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f"VARCHAR2(4000)"
        else:
            return 'TEXT'

    def __str__(self):
        return f"TEXT" + self.get_str_attributes()


class UuidType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("UUID", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'mysql' or dialect == 'oracle':
            return f"VARCHAR(36)"
        else:
            return 'UUID'

    def __str__(self):
        return f"UUID" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql' or dialect == 'oracle':
                return f"\'{value}\'"
            else:
                return f"\'{value}\'::uuid"


class JsonType(ExpressionType):
    def __init__(self, json_structure=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("JSON", {"structure": json_structure}, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'JSON'
        elif dialect == 'pg':
            return 'JSON'
        else:
            return None

    def __str__(self):
        return f"JSON" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql':
                return "\'" + json.dumps(value).replace('\\', '\\\\').replace('-', '\\-') + "\'"
            elif dialect == 'pg':
                return "\'" + json.dumps(value) + "\'::json"
            else:
                return None


class JsonbType(ExpressionType):
    def __init__(self, json_structure=None, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("JSONB", {"structure": json_structure}, attr_container)
        self.json_structure = json_structure

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'JSON'
        elif dialect == 'pg':
            return 'JSONB'
        else:
            return None

    def __str__(self):
        return f"JSONB" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql':
                return "\'" + json.dumps(value).replace('\\', '\\\\').replace('-', '\\-') + "\'"
            elif dialect == 'pg':
                return "\'" + json.dumps(value) + "\'::jsonb"
            else:
                return None


class PointType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("POINT", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return 'SDO_GEOMETRY'
        elif dialect == 'pg':
            return 'GEOMETRY'
        else:
            return 'POINT'

    def __str__(self):
        return f"POINT" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, dict)
            if value['longitude'] is None or value['latitude'] is None:
                return 'NULL'
            if dialect == 'oracle':
                return (f"SDO_GEOMETRY(2001, 4326, "
                        f"SDO_POINT_TYPE({value['longitude']}, {value['latitude']}, NULL), NULL, NULL)")
            elif dialect == 'pg':
                return f"ST_GeomFromText('POINT({value['longitude']} {value['latitude']})', 4326)"
            elif dialect == 'mysql':
                return f"ST_GeomFromText('POINT({value['latitude']} {value['longitude']})', 4326)"


class XmlType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("XML", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'TEXT'
        elif dialect == 'pg':
            return 'XML'
        elif dialect == 'oracle':
            return 'XMLType'

    def __str__(self):
        return f"XML" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql':
                value = value.replace('\\', '\\\\').replace('-', '\\-')
                return f"'{value}'"
            elif dialect == 'pg':
                return f"\'{value}\'::xml"
            elif dialect == 'oracle':
                return f"xmltype(\'{value}\')"


class BlobType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("BLOB", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'BLOB'
        elif dialect == 'pg':
            return 'BYTEA'
        elif dialect == 'oracle':
            return 'BLOB'

    def __str__(self):
        return f"BLOB" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            return None


class ArrayType(ExpressionType):
    def __init__(self, element_type: BaseType = None, col_name=None, length=None,
                 attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("ARRAY", {"element_type": element_type}, attr_container)
        self.element_type = element_type
        self.col_name = col_name
        self.length = length

    def gen_demo_value(self):
        return 'col'

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return scale_name_into_length(f'{self.col_name}_varray_type')
        elif dialect == 'mysql':
            return 'JSON'
        elif dialect == 'pg':
            ele_name = self.element_type.get_type_name(dialect)
            assert isinstance(ele_name, str)
            return f'{ele_name}[]'

    def __str__(self):
        return f"ARRAY({self.element_type})[{self.length}]" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql':
                return f"'{json.dumps(value)}'"
            else:
                ele_str = ''
                for ele in value:
                    if ele_str != '':
                        ele_str = ele_str + ', '
                    ele_str = ele_str + self.element_type.gen_value(dialect, ele)
                if dialect == 'oracle':
                    return f"{self.col_name}_varray_type({ele_str})"
                elif dialect == 'pg':
                    return f"ARRAY[{ele_str}]"
                else:
                    assert False


class NullType(ExpressionType):
    def __init__(self):
        super().__init__("NULL")

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'NULL'
        elif dialect == 'pg':
            return 'NULL'
        elif dialect == 'oracle':
            return 'NULL'

    def __str__(self):
        return f"NULL"

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert value is None
        return 'NULL'


class ListType(BaseType):
    def __init__(self, element_type: BaseType):
        super().__init__("LIST", {"element_type": element_type})
        self.element_type = element_type

    def get_type_name(self, dialect: str):
        return f'LIST[{self.element_type.get_type_name(dialect)}]'

    def __str__(self):
        return f"LIST[{self.element_type}]"

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def generate_value(self, usable_cols: list, root_node, length: int, src_dialect: str, execution_env):
        res = []
        i = 0
        if length is None:
            length = random.randint(1, 3)
        while i < length:
            child_value = self.element_type.generate_value(usable_cols, root_node, None, src_dialect, execution_env)
            if child_value is None:
                return None
            res.append(child_value)
            i += 1
        return res


class OptionType(BaseType):
    def __init__(self, map_dict: dict):
        super().__init__("OPTION")
        self.map_dict = map_dict

    def get_type_name(self, dialect: str):
        map_str = ''
        for key, value in self.map_dict.items():
            if map_str != '':
                map_str = map_str + ', '
            map_str = map_str + f"{key}: {value}"
        return f'OPTION[{map_str}]'

    def __str__(self):
        return self.get_type_name('')

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is not None:
            assert value in self.map_dict
            return self.map_dict[value]

    def gen_demo_value(self):
        return list(self.map_dict.keys())[0]

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        return Operand(random.choice(list(self.map_dict.keys())), BaseType(''))


class AnyValueType(ExpressionType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("ANY_VALUE", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'ANY_VALUE'

    def __str__(self):
        return f"ANY_VALUE" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False


class AliasType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("ALIAS", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'ALIAS'

    def __str__(self):
        return f"ALIAS"

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return 'alias'

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        return Operand('alias_' + str(random.randint(1, 500)), BaseType(''))


class TableType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("TABLE", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'TABLE'

    def __str__(self):
        return f"TABLE" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return f"table_element"

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        assert False


class QueryType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("QUERY", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'QUERY'

    def __str__(self):
        return f"QUERY" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self) -> str | None:
        return f"SELECT 1 FROM tbl"

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        assert False


class StringLiteralType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("STRING LITERAL", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'STRING LITERAL'

    def __str__(self):
        return f"STRING LITERAL" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return f"'AAAA11'"

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        fake = Faker()
        from sql_gen.generator.element.Operand import Operand
        return Operand(f'\'{fake.sentence()}\'', StringGeneralType())


class WordLiteralType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("WORD LITERAL", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'WORD LITERAL'

    def __str__(self):
        return f"WORD LITERAL" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return f"AAAA11"


class IntLiteralType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("INT_LITERAL", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'INT_LITERAL'

    def __str__(self):
        return f"INT_LITERAL" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return f"1"

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        min_range, max_range = self.attr_container.get_int_allowed_range()
        if min_range is None:
            min_range = -10
            max_range = 10
        return Operand(str(random.randint(min_range, max_range)), BaseType(''))


class FloatLiteralType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("FLOAT LITERAL", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'FLOAT LITERAL'

    def __str__(self):
        return f"FLOAT LITERAL" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return f"1.0"

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        return Operand(str(random.uniform(1, 7)), BaseType(''))


class OrderByElementType(BaseType):
    def __init__(self, type_attributes=None, attr_container: AttributeContainer = AttributeContainer()):
        dict.__init__(self, type_json_build('ORDER_BY_ELEMENT', type_attributes, attr_container))
        self.attr_container = attr_container

    def get_str_attributes(self):
        if self.attr_container is None:
            return ''
        return ', '.join([str(attr) for attr in self.attr_container.attributes])

    def __str__(self):
        return 'ORDER_BY_ELEMENT' + self.get_str_attributes()

    def get_type_name(self, dialect: str) -> str | None:
        return "ORDER_BY_ELEMENT"

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return f"col ASC"

    @abstractmethod
    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        if src_dialect == 'mysql':
            assert root_node.value == 'querySpecification' or root_node.value == 'querySpecificationNointo'
            select_elements_node = root_node.get_child_by_value('selectElements')
            assert select_elements_node is not None
            length = len(select_elements_node.get_children_by_value('selectElement'))
            value = random.randint(1, length)
            if random.randint(1, 2) == 1:
                res = f"{value} ASC"
            elif random.randint(1, 2) == 2:
                res = f"{value} DESC"
            else:
                res = f"{value}"
            return Operand(res, BaseType(''))
        elif src_dialect == 'pg':
            assert root_node.value == 'simple_select_pramary'
            if root_node.get_child_by_value('opt_target_list') is not None:
                target_list_node = root_node.get_child_by_value('opt_target_list')
                target_list_node = target_list_node.get_child_by_value('target_list')
            else:
                target_list_node = root_node.get_child_by_value('target_list')
            assert target_list_node is not None
            select_elements_node = target_list_node.get_child_by_value('target_el')
            length = len(select_elements_node)
            value = random.randint(1, length)
            if random.randint(1, 3) == 1:
                nulls_str = 'NULLS FIRST'
            elif random.randint(1, 3) == 1:
                nulls_str = 'NULLS LAST'
            else:
                nulls_str = ''
            if random.randint(1, 3) == 1:
                asc_str = 'DESC'
            elif random.randint(1, 3) == 1:
                asc_str = 'ASC'
            else:
                asc_str = ''
            return Operand(f"{value} {asc_str} {nulls_str}".strip(), BaseType(''))
        else:
            assert root_node.value == 'query_block'
            select_list_node = root_node.get_child_by_value('selected_list')
            select_elements_node = select_list_node.get_children_by_value('select_list_elements')
            assert select_elements_node is not None
            length = len(select_elements_node)
            value = random.randint(1, length)
            if random.randint(1, 3) == 1:
                nulls_str = 'NULLS FIRST'
            elif random.randint(1, 3) == 1:
                nulls_str = 'NULLS LAST'
            else:
                nulls_str = ''
            if random.randint(1, 3) == 1:
                asc_str = 'DESC'
            elif random.randint(1, 3) == 1:
                asc_str = 'ASC'
            else:
                asc_str = ''
            return Operand(f"{value} {asc_str} {nulls_str}".strip(), BaseType(''))


class NoneType(BaseType):
    def __init__(self, attr_container: AttributeContainer = AttributeContainer()):
        super().__init__("NontType", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'NoneType'

    def __str__(self):
        return f"NoneType" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self) -> str | None:
        assert False

    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        assert False


class WindowDefinitionType(BaseType):
    def __init__(self, type_attributes=None, attr_container: AttributeContainer = AttributeContainer()):
        dict.__init__(self, type_json_build('WindowDefinitionType', type_attributes, attr_container))
        self.attr_container = attr_container

    def get_str_attributes(self):
        if self.attr_container is None:
            return ''
        return ', '.join([str(attr) for attr in self.attr_container.attributes])

    def __str__(self):
        return 'WindowDefinitionType' + self.get_str_attributes()

    def get_type_name(self, dialect: str) -> str | None:
        return "WindowDefinitionType"

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

    def gen_demo_value(self):
        return f"PARTITION BY col"

    @abstractmethod
    def generate_value(self, usable_cols: list, root_node, length: int | None, src_dialect: str, execution_env):
        from sql_gen.generator.element.Operand import Operand
        can_be_used_col = []
        for col in usable_cols:
            if col.semantic_attribute is not None and col.semantic_attribute.categorical:
                can_be_used_col.append(col)
        partition_len = random.randint(2, 3)
        partition_len = min(partition_len, len(can_be_used_col))
        if partition_len == 0:
            return None
        else:
            cols = random.sample(can_be_used_col, partition_len)
            partition_cols = ''
            for col in cols:
                assert isinstance(col, Operand)
                if partition_cols != '':
                    partition_cols = partition_cols + ','
                partition_cols = partition_cols + col.str_value()
            return Operand(f"PARTITION BY {partition_cols}".strip(), BaseType(''))


def is_num_type(type: BaseType):
    if isinstance(type, NumberType):
        return True


def is_str_type(type: BaseType):
    if isinstance(type, StringGeneralType):
        return True
    elif isinstance(type, VarcharType):
        return True
    elif isinstance(type, NvarcharType):
        return True
    elif isinstance(type, CharType):
        return True
    elif isinstance(type, TextType):
        return True
    elif isinstance(type, EnumType):
        return True
    elif isinstance(type, BlobType):
        return True
    return False


def is_time_type(type: BaseType):
    if isinstance(type, TimestampType):
        return True
    elif isinstance(type, DateType):
        return True
    elif isinstance(type, TimeType):
        return True
    elif isinstance(type, DatetimeType):
        return True
    elif isinstance(type, TimestampTZType):
        return True
    else:
        return False
