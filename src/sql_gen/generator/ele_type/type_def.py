# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: type_def$
# @Author: 10379
# @Time: 2025/3/24 19:59
import json
from abc import ABC, abstractmethod
from datetime import datetime

from sql_gen.generator.ele_type.Attribute import AttributeContainer
from utils.tools import date_format_trans


def type_json_build(type_name, type_attributes, attr_container):
    res = {}
    assert type_name is not None
    if type_attributes is not None:
        for key, value in type_attributes.items():
            res[key] = value
    if attr_container is not None:
        res['attr_container'] = attr_container
    return res


class BaseType(dict, ABC):
    def __init__(self, type_name, type_attributes=None, attr_container: AttributeContainer | None = None):
        dict.__init__(self, type=type_json_build(type_name, type_attributes, attr_container))
        self.attr_container = attr_container

    def get_str_attributes(self):
        if self.attr_container is None:
            return ''
        return ', ' + ', '.join([str(attr) for attr in self.attr_container.attributes])

    @abstractmethod
    def __str__(self):
        return 'BaseType' + self.get_str_attributes()

    @abstractmethod
    def get_type_name(self, dialect: str) -> str | None:
        return "BaseType"

    @abstractmethod
    def gen_value(self, dialect: str, value=None) -> str | None:
        return str(value)


class NumberType(BaseType):
    def __init__(self, type_name=None, attributes: dict = None, attr_container: AttributeContainer | None = None):
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


class IntType(NumberType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("INT", None, attr_container)

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


class BoolType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
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


class FloatGeneralType(NumberType):
    def __init__(self, type_name=None, type_attributes: dict = None, attr_container: AttributeContainer | None = None):
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
    def __init__(self, precision, scale, attr_container: AttributeContainer | None = None):
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
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("DOUBLE", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'FLOAT(126)'
        elif dialect == 'pg':
            return 'DOUBLE PRECISION'
        elif dialect == 'mysql':
            return 'DOUBLE'
        else:
            assert False

    def __str__(self):
        return "DOUBLE"


class DateType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("DATE", None, attr_container)

    def get_type_name(self, dialect: str):
        return 'DATE'

    def __str__(self):
        return "DATE" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, dict)
            if dialect == 'mysql':
                date_format = date_format_trans(value['format'])
                return f"STR_TO_DATE('{value['value']}', '{date_format}')"
            elif dialect == 'pg':
                return f"TO_DATE('{value['value']}', '{value['format']}')"
            elif dialect == 'oracle':
                return f"TO_DATE('{value['value']}', '{value['format']}')"


class TimeType(BaseType):
    def __init__(self, fraction=None, attr_container: AttributeContainer | None = None):
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
                return f"CAST({value} AS {self.get_type_name(dialect)}))"
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


class YearType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
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


class TimestampType(BaseType):
    def __init__(self, fraction=None, attr_container: AttributeContainer | None = None):
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
            assert isinstance(value, dict)
            timestamp_obj = datetime.strptime(value['value'], date_format_trans(value['format']))
            formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            if dialect == 'oracle':
                return f"TO_TIMESTAMP('{value['value']}', '{value['format']}')"
            elif dialect == 'pg':
                return f"'{formatted_timestamp_str}'::timestamp"
            elif dialect == 'mysql':
                return f"TIMESTAMP('{formatted_timestamp_str}')"


class DatetimeType(BaseType):
    def __init__(self, fraction=None, attr_container: AttributeContainer | None = None):
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
            assert isinstance(value, dict)
            timestamp_obj = datetime.strptime(value['value'], date_format_trans(value['format']))
            formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            if dialect == 'mysql':
                return f"STR_TO_DATE('{formatted_timestamp_str}', '%Y-%m-%d %H:%i:%s')"
            else:
                if dialect == 'oracle':
                    return f"TO_TIMESTAMP('{formatted_timestamp_str}', 'yyyy-MM-dd HH24:mi:ss')"
                elif dialect == 'pg':
                    return f"'{formatted_timestamp_str}'::timestamp"


class IntervalType(BaseType):
    # TODO
    def __init__(self, typename=None, units: list = None, attr_container: AttributeContainer | None = None):
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


class IntervalYearMonthType(IntervalType):
    def __init__(self, attr_container: AttributeContainer | None = None):
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


class TimestampTZType(BaseType):
    def __init__(self, fraction=None, attr_container: AttributeContainer | None = None):
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


class StringGeneralType(BaseType):
    def __init__(self, type_name=None, attributes: dict = None, attr_container: AttributeContainer | None = None):
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
            return f"\'{value}\'"


class VarcharType(BaseType):
    def __init__(self, length=None, attr_container: AttributeContainer | None = None):
        super().__init__("VARCHAR", {"length": length}, attr_container)
        self.length = length

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'VARCHAR2({self.length})'
        else:
            return f'VARCHAR({self.length})'

    def __str__(self):
        return f"VARCHAR({self.length})" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class EnumType(BaseType):
    def __init__(self, values: list, attr_container: AttributeContainer | None = None):
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
            return f"\'{value}\'"


class NvarcharType(BaseType):
    def __init__(self, length, attr_container: AttributeContainer | None = None):
        super().__init__("NVARCHAR", {"length": length}, attr_container)
        self.length = length

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return f"NVARCHAR({self.length})"
        elif dialect == 'pg':
            return f'VARCHAR({self.length})'
        elif dialect == 'oracle':
            return f"VARCHAR2({self.length} CHAR)"

    def __str__(self):
        return f"NVARCHAR({self.length})" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class CharType(BaseType):
    def __init__(self, length, attr_container: AttributeContainer | None = None):
        super().__init__("CHAR", {"length": length}, attr_container)
        self.length = length

    def get_type_name(self, dialect: str):
        return f'CHAR({self.length})'

    def __str__(self):
        return f"CHAR({self.length})" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class TextType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("TEXT", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f"VARCHAR2(4000)"
        else:
            return 'TEXT'

    def __str__(self):
        return f"TEXT" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class UuidType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("UUID", None, attr_container)

    def get_type_name(self, dialect: str):
        if dialect == 'mysql' or dialect == 'oracle':
            return f"CHAR(36)"
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


class JsonType(BaseType):
    def __init__(self, json_structure, attr_container: AttributeContainer | None = None):
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
                return "\'" + json.dumps(value) + "\'"
            elif dialect == 'pg':
                return "\'" + json.dumps(value) + "\'::json"
            else:
                return None


class JsonbType(BaseType):
    def __init__(self, json_structure=None, attr_container: AttributeContainer | None = None):
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
                return "\'" + json.dumps(value) + "\'"
            elif dialect == 'pg':
                return "\'" + json.dumps(value) + "\'::jsonb"
            else:
                return None


class PointType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
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


class XmlType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
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
                return f"'{value}'"
            elif dialect == 'pg':
                return f"\'{value}\'::xml"
            elif dialect == 'oracle':
                return f"xmltype(\'{value}\')"


class BlobType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
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


class ArrayType(BaseType):
    def __init__(self, element_type: BaseType, col_name=None, length=None, attr_container: AttributeContainer | None = None):
        super().__init__("ARRAY", {"element_type": element_type}, attr_container)
        self.element_type = element_type
        self.col_name = col_name
        self.length = length

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'{self.col_name}_varray_type'
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


class NullType(BaseType):
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


class OptionType(BaseType):
    def __init__(self, map_dict):
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


class AnyValueType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("ANY_VALUE", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'ANY_VALUE'

    def __str__(self):
        return f"ANY_VALUE" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False


class AliasType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("ALIAS", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'ALIAS'

    def __str__(self):
        return f"ALIAS"

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False


class TableType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("TABLE", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'TABLE'

    def __str__(self):
        return f"TABLE" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False


class QueryType(BaseType):
    def __init__(self, attr_container: AttributeContainer | None = None):
        super().__init__("QUERY", None, attr_container)

    def get_type_name(self, dialect: str):
        return f'QUERY'

    def __str__(self):
        return f"QUERY" + self.get_str_attributes()

    def gen_value(self, dialect: str, value=None) -> str | None:
        assert False

def is_num_type(type: BaseType):
    if isinstance(type, NumberType):
        return True
    elif isinstance(type, BoolType):
        return True
    elif isinstance(type, IntType):
        return True
    elif isinstance(type, FloatGeneralType):
        return True
    elif isinstance(type, DoubleType):
        return True
    elif isinstance(type, DecimalType):
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