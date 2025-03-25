# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: type_def$
# @Author: 10379
# @Time: 2025/3/24 19:59
import json
from abc import ABC, abstractmethod
from datetime import datetime

from udfs.date_udf import date_format_udf


def type_json_build(type_name, attributes):
    if attributes is None:
        return {"type_name": type_name}
    else:
        ori_dict = {"type_name": type_name}
        for key, value in attributes.items():
            ori_dict[key] = value
        return ori_dict


class BaseType(dict, ABC):
    def __init__(self, type_name, attributes=None):
        dict.__init__(self, type=type_json_build(type_name, attributes))

    @abstractmethod
    def __str__(self):
        return 'BaseType'

    @abstractmethod
    def get_type_name(self, dialect: str):
        return "BaseType"

    @abstractmethod
    def gen_value(self, dialect: str, value=None) -> str | None:
        return str(value)


class IntType(BaseType):
    def __init__(self):
        super().__init__("INT")

    def __str__(self):
        return 'INT'

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
    def __init__(self):
        super().__init__("BOOL")

    def __str__(self):
        return 'BOOL'

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


class DecimalType(BaseType):
    def __init__(self, precision, scale):
        super().__init__("DECIMAL", {"precision": precision, "scale": scale})
        self.precision = precision
        self.scale = scale

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'NUMBER({self.precision}, {self.scale})'
        else:
            return f'DECIMAL({self.precision}, {self.scale})'

    def __str__(self):
        return f"DECIMAL({self.precision},{self.scale})"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, float) or isinstance(value, str)
            return str(float(value))


class DoubleType(BaseType):
    def __init__(self):
        super().__init__("DOUBLE")

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

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, float) or isinstance(value, str)
            return str(value)


class DateType(BaseType):
    def __init__(self):
        super().__init__("DATE")

    def get_type_name(self, dialect: str):
        return 'DATE'

    def __str__(self):
        return "DATE"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, dict)
            if dialect == 'mysql':
                date_format = date_format_udf(value['format'])
                return f"STR_TO_DATE('{value['value']}', '{date_format}')"
            elif dialect == 'pg':
                return f"TO_DATE('{value['value']}', '{value['format']}')"
            elif dialect == 'oracle':
                return f"TO_DATE('{value['value']}', '{value['format']}')"


class TimeType(BaseType):
    def __init__(self, fraction=None):
        super().__init__("TIME", {"fraction": fraction})
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
            return 'TIME'
        else:
            return f'TIME({self.fraction})'

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
    def __init__(self):
        super().__init__("YEAR")

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'YEAR'
        elif dialect == 'pg':
            return 'SMALLINT'
        elif dialect == 'oracle':
            return 'NUMBER(4)'

    def __str__(self):
        return "YEAR"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, int) or isinstance(value, str)
            return str(value)


class TimestampType(BaseType):
    def __init__(self, fraction=None):
        super().__init__("TIMESTAMP", {"fraction": fraction})
        self.fraction = fraction

    def get_type_name(self, dialect: str):
        if self.fraction is None:
            return f'TIMESTAMP'
        else:
            return f'TIMESTAMP({self.fraction})'

    def __str__(self):
        if self.fraction is None:
            return 'TIMESTAMP'
        else:
            return f'TIMESTAMP{self.fraction}'

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, dict)
            timestamp_obj = datetime.strptime(value['value'], date_format_udf(value['format']))
            formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            if dialect == 'oracle':
                return f"TO_TIMESTAMP('{value['value']}', '{value['format']}')"
            elif dialect == 'pg':
                return f"'{formatted_timestamp_str}'::timestamp"
            elif dialect == 'mysql':
                return f"TIMESTAMP('{formatted_timestamp_str}')"


class DatetimeType(BaseType):
    def __init__(self, fraction=None):
        super().__init__("DATETIME", {"fraction": fraction})
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
            return 'DATETIME'
        else:
            return f'DATETIME({self.fraction})'

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, dict)
            date_format_udf(value['format'])
            timestamp_obj = datetime.strptime(value['value'], date_format_udf(value['format']))
            formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            if dialect == 'mysql':
                return formatted_timestamp_str
            else:
                if dialect == 'oracle':
                    return f"TO_TIMESTAMP('{formatted_timestamp_str}', 'yyyy-MM-dd HH24:mi:ss')"
                elif dialect == 'pg':
                    return f"TIMESTAMP('{formatted_timestamp_str}')"


class IntervalYearMonthType(BaseType):
    def __init__(self):
        super().__init__("INTERVAL YEAR TO MONTH")

    def get_type_name(self, dialect: str):
        if dialect == 'oracle' or dialect == 'pg':
            return 'INTERVAL YEAR TO MONTH'
        else:
            assert False

    def __str__(self):
        return "INTERVAL YEAR TO MONTH"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, dict)
            if dialect == 'pg':
                if value['sign']:
                    value['year'] = -1 * value['year']
                return f"'{value['year']} years {value['month']} months'::INTERVAL YEAR TO MONTH"
            elif dialect == 'oracle':
                if value['sign']:
                    value['year'] = -1 * value['year']
                return f"to_yminterval('{value['year']}-{value['month']}')"
            else:
                return None


class TimestamepTZType(BaseType):
    def __init__(self, fraction=None):
        super().__init__("TIMESTAMPTZ", {"fraction": fraction})
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


class VarcharType(BaseType):
    def __init__(self, length):
        super().__init__("VARCHAR", {"length": length})
        self.length = length

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f'VARCHAR2({self.length})'
        else:
            return f'VARCHAR({self.length})'

    def __str__(self):
        return f"VARCHAR({self.length})"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class EnumType(BaseType):
    def __init__(self, values: list):
        super().__init__("Enum", {"values": values})
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
        return f"VARCHAR({self.len})"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            assert value in self.values
            return f"\'{value}\'"


class NvarcharType(BaseType):
    def __init__(self, length):
        super().__init__("NVARCHAR", {"length": length})
        self.length = length

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return f"NVARCHAR({self.length})"
        elif dialect == 'pg':
            return f'VARCHAR({self.length})'
        elif dialect == 'oracle':
            return f"VARCHAR2({self.length} CHAR)"

    def __str__(self):
        return f"NVARCHAR({self.length})"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class CharType(BaseType):
    def __init__(self, length):
        super().__init__("CHAR", {"length": length})
        self.length = length

    def get_type_name(self, dialect: str):
        return f'CHAR({self.length})'

    def __str__(self):
        return f"CHAR({self.length})"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class TextType(BaseType):
    def __init__(self):
        super().__init__("TEXT")

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return f"VARCHAR2(4000)"
        else:
            return 'TEXT'

    def __str__(self):
        return f"TEXT"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            assert isinstance(value, str)
            return f"\'{value}\'"


class UuidType(BaseType):
    def __init__(self):
        super().__init__("UUID")

    def get_type_name(self, dialect: str):
        if dialect == 'mysql' or dialect == 'oracle':
            return f"CHAR(36)"
        else:
            return 'UUID'

    def __str__(self):
        return f"UUID"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql' or dialect == 'oracle':
                return f"\'{value}\'"
            else:
                return f"\'{value}\'::uuid"


class JsonType(BaseType):
    def __init__(self, json_structure):
        super().__init__("JSON", {"structure": json_structure})

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'JSON'
        elif dialect == 'pg':
            return 'JSON'
        else:
            assert False

    def __str__(self):
        return f"JSON"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql':
                return "\'" + json.dumps(value) + "\'"
            elif dialect == 'pg':
                return "\'" + json.dumps(value) + "\'::json"
            else:
                assert False


class JsonbType(BaseType):
    def __init__(self, json_structure):
        super().__init__("JSONB", {"structure": json_structure})

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'JSON'
        elif dialect == 'pg':
            return 'JSONB'
        else:
            assert False

    def __str__(self):
        return f"JSONB"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            if dialect == 'mysql':
                return "\'" + json.dumps(value) + "\'"
            elif dialect == 'pg':
                return "\'" + json.dumps(value) + "\'::jsonb"
            else:
                assert False


class PointType(BaseType):
    def __init__(self):
        super().__init__("POINT")

    def get_type_name(self, dialect: str):
        if dialect == 'oracle':
            return 'SDO_GEOMETRY'
        elif dialect == 'pg':
            return 'GEOMETRY'
        else:
            return 'POINT'

    def __str__(self):
        return f"POINT"

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
    def __init__(self):
        super().__init__("XML")

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'TEXT'
        elif dialect == 'pg':
            return 'XML'
        elif dialect == 'oracle':
            return 'XMLType'

    def __str__(self):
        return f"XML"

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
    def __init__(self):
        super().__init__("BLOB")

    def get_type_name(self, dialect: str):
        if dialect == 'mysql':
            return 'BLOB'
        elif dialect == 'pg':
            return 'BYTEA'
        elif dialect == 'oracle':
            return 'BLOB'

    def __str__(self):
        return f"BLOB"

    def gen_value(self, dialect: str, value=None) -> str | None:
        if value is None:
            assert False
        else:
            return None


class ArrayType(BaseType):
    def __init__(self, element_type: BaseType, col_name, length):
        super().__init__("ARRAY", {"element_type": element_type})
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
        return f"ARRAY({self.element_type})[{self.length}]"

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
