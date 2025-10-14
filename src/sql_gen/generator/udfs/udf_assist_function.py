# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: udf_assist_functions$
# @Author: 10379
# @Time: 2025/6/13 10:41
import re

from dateutil.relativedelta import relativedelta

from sql_gen.generator.element.Operand import Operand

time_string = "@ 1 year 2 months -3 days 4 hours 5 minutes 6 seconds ago"
pattern = r'([-+]?\d+)\s+(year|mon|month|day|hour|minute|sec|second|years|months|days|hours|minutes|seconds|mins|secs|mons)'

matches = re.findall(pattern, time_string, flags=re.IGNORECASE)

def parse_time_string(s):
    sign = 1
    if 'ago' in s:
        sign = -1
    s = re.sub(r'^@', '', s).strip()
    s = re.sub(r'ago$', '', s).strip()
    pattern = r'(-?\d+)\s+(\w+)'
    matches = re.findall(pattern, s)
    kwargs = {
        'years': 0,
        'months': 0,
        'days': 0,
        'hours': 0,
        'minutes': 0,
        'seconds': 0,
    }
    for value, unit in matches:
        value = int(value)
        unit = unit.lower()
        if unit in ['year', 'years']:
            kwargs['years'] += value
        elif unit in ['mon', 'month', 'months', 'mons']:
            kwargs['months'] += value
        elif unit in ['day', 'days']:
            kwargs['days'] += value
        elif unit in ['hour', 'hours']:
            kwargs['hours'] += value
        elif unit in ['min', 'minute', 'minutes', 'mins']:
            kwargs['minutes'] += value
        elif unit in ['sec', 'second', 'seconds', 'secs']:
            kwargs['seconds'] += value
        else:
            raise ValueError(f"Unknown time unit: {unit}")
    return relativedelta(**kwargs) * sign


def parse_ori_pg_interval_string(s):
    pattern = r"""
        ^\s*                                      
        (?: (?P<year_month> [-+]?\d+ [+-] [-+]?\d+) )? \s*
        (?: (?P<days> [+-]?\d+ ) )? \s*
        (?: (?P<time> [+-]? \d{1,2} : \d{2} (?: : \d{2} )? ) )? \s*
        $                                       
    """
    regex = re.compile(pattern, re.VERBOSE)
    match = regex.match(s.strip())
    if not match:
        return None
    return match.groupdict()


def reg_escape(op: Operand):
    ori_str = op.str_value().strip().strip('\'')
    special_chars = r".^$*+?{}[]\|()"
    escaped_string = ''.join(
        '\\' + char if char in special_chars else char
        for char in ori_str
    )
    return escaped_string


def is_valid_pg_time_string(s):
    # 匹配以 @ 开头且以 ago 结尾的字符串
    pattern = r'^@?(\s*(-?\d+)\s+(year|mon|month|day|hour|minute|sec|second|years|months|days|hours|minutes|seconds|mins|secs|mons))+\s*(ago)?$'
    return bool(re.fullmatch(pattern, s.strip(), flags=re.IGNORECASE))
