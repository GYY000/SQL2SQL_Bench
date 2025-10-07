# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: tools$
# @Author: 10379
# @Time: 2024/12/9 20:18
import configparser
import json
import os
import platform
from typing import List


def get_data_path(config_file=None):
    if config_file is None:
        config_file = os.path.join(get_proj_root_path(), 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)
    return config.get("FILE_PATH", 'data_path')


def dialect_judge(dialect: str):
    oracle_synonyms = ['oracle']
    pg_synonyms = ['pg', 'postgres', 'postgreSQL']
    mysql_synonyms = ['mysql']
    if dialect.lower() in mysql_synonyms:
        return 'mysql'
    elif dialect.lower() in pg_synonyms:
        return 'postgres'
    elif dialect.lower() in oracle_synonyms:
        return 'oracle'
    else:
        raise ValueError(f"Dialect must be one of {oracle_synonyms + pg_synonyms + mysql_synonyms}")


def get_proj_root_path():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_config(config_file=None):
    if config_file is None:
        config_file = os.path.join(get_proj_root_path(), 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'dbg': config.getboolean("MODE", 'dbg'),
        'cloud_mode': config.getboolean("MODE", 'cloud_mode'),
        "gpt_api_base": config.get("API", 'gpt_api_base'),
        "gpt_api_key": config.get("API", 'gpt_api_key'),
        "moonshot_api_base": config.get("API", 'moonshot_api_base'),
        "moonshot_api_key": config.get("API", 'moonshot_api_key'),
        "llama3.1_api_base": config.get("API", 'llama3.1_api_base'),
        "volcano_api_base": config.get("API", 'volcano_api_base'),
        "volcano_api_key": config.get("API", 'volcano_api_key'),
        "bailian_api_base": config.get("API", 'bailian_api_base'),
        "bailian_api_key": config.get("API", 'bailian_api_key'),
        "deepseek_api_base": config.get("API", 'deepseek_api_base'),
        "deepseek_api_key": config.get("API", 'deepseek_api_key'),
    }


def load_gen_param(config_file=None):
    if config_file is None:
        config_file = os.path.join(get_proj_root_path(), 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'using_new_sql_pos': config.getfloat("PARAMETER", 'using_new_sql_pos'),
    }


def load_db_config(config_file=None):
    if config_file is None:
        config_file = os.path.join(get_proj_root_path(), 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'max_len_oracle_sql': config.getint("DB_LIMIT", 'max_len_oracle_sql'),
        'max_len_mysql_sql': config.getint("DB_LIMIT", 'max_len_mysql_sql'),
        "max_len_pg_sql": config.getint("DB_LIMIT", 'max_len_pg_sql')
    }


def load_oracle_config(config_file=None):
    if config_file is None:
        config_file = os.path.join(get_proj_root_path(), 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)

    return {
        'oracle_instant_path': config.get("ORACLE_CONN", 'oracle_instant_path'),
        # 'oracle_user': config.get("ORACLE_CONN", 'oracle_user'),
        'usr_default_pwd': config.get("ORACLE_CONN", 'usr_default_pwd'),
        'oracle_host': config.get("ORACLE_CONN", 'oracle_host'),
        'oracle_port': config.getint("ORACLE_CONN", 'oracle_port'),
        'oracle_sys_user': config.get("ORACLE_CONN", 'oracle_sys_user'),
        'oracle_sys_pwd': config.get("ORACLE_CONN", 'usr_default_pwd'),
        'oracle_sid': config.get("ORACLE_CONN", 'oracle_sid'),
    }


def load_mysql_config(config_file=None):
    if config_file is None:
        config_file = os.path.join(get_proj_root_path(), 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)

    return {
        'mysql_user': config.get("MYSQL_CONN", 'mysql_user'),
        'mysql_pwd': config.get("MYSQL_CONN", 'mysql_pwd'),
        'mysql_host': config.get("MYSQL_CONN", 'mysql_host'),
        'mysql_port': config.getint("MYSQL_CONN", 'mysql_port'),
    }


def load_pg_config(config_file=None):
    if config_file is None:
        config_file = os.path.join(get_proj_root_path(), 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)

    return {
        'pg_user': config.get("PG_CONN", 'pg_user'),
        'pg_pwd': config.get("PG_CONN", 'pg_pwd'),
        'pg_host': config.get("PG_CONN", 'pg_host'),
        'pg_port': config.getint("PG_CONN", 'pg_port'),
    }


def self_split(str1: str) -> List[str]:
    """
    不会将引号内的空格由于分割
    """
    res = []
    str0 = ''
    flag0 = False
    flag1 = False
    i = 0
    while i < len(str1):
        if str1[i] == '\"':
            if flag0:
                str0 = str0 + str1[i]
            else:
                flag1 = not flag1
                str0 = str0 + str1[i]
        if str1[i] == '\'':
            if flag1:
                str0 = str0 + str1[i]
            else:
                flag0 = not flag0
                str0 = str0 + str1[i]
        elif not flag0 and not flag1 and (str1[i] == ' ' or str1[i] == '\n'):
            if str0 != '':
                res.append(str0)
            str0 = ''
        else:
            if str1[i] == '\\':
                str0 = str0 + str1[i]
                i = i + 1
            str0 = str0 + str1[i]
        i = i + 1
    if str0 != '':
        res.append(str0)
    return res


def str_split(str_to_split: str, split_mark: str):
    res = []
    str0 = ''
    flag0 = False
    flag1 = False
    i = 0
    while i < len(str_to_split):
        if str_to_split[i] == '\"':
            if flag0:
                str0 = str0 + str_to_split[i]
            else:
                flag1 = not flag1
                str0 = str0 + str_to_split[i]
        elif str_to_split[i] == '\'':
            if flag1:
                str0 = str0 + str_to_split[i]
            else:
                flag0 = not flag0
                str0 = str0 + str_to_split[i]
        elif not flag0 and not flag1 and str_to_split[i] == split_mark:
            if str0 != '':
                res.append(str0)
            str0 = ''
        else:
            if str_to_split[i] == '\\':
                str0 = str0 + str_to_split[i]
                i = i + 1
            str0 = str0 + str_to_split[i]
        i = i + 1
    if str0 != '':
        res.append(str0)
    return res


def remove_all_space(ori_str: str):
    res = ''
    for ori_sql_slice in ori_str.split():
        res = res + ori_sql_slice
    return res


def get_quote(dialect: str):
    if dialect == 'mysql':
        return '`'
    elif dialect == 'oracle' or dialect == 'pg':
        return '"'
    else:
        assert False


def strip_quote(dialect: str, name: str):
    quote = get_quote(dialect)
    if name.startswith(quote):
        name = name[1:]
    if name.endswith(quote):
        name = name[:len(name) - 1]
    return name


def add_quote(dialect: str, name: str):
    quote = get_quote(dialect)
    if name.startswith(quote):
        name = name[1:]
    if name.endswith(quote):
        name = name[:len(name) - 1]
    return quote + name + quote


def is_running_on_linux():
    if os.name == 'posix':
        return 'linux' in platform.system().lower()
    return False


def extract_parameters(func_expr: str):
    i = 0
    while i < len(func_expr) and func_expr[i] != '(':
        i = i + 1
    assert func_expr[i] == '('
    i = i + 1
    res = []
    quote_stack = []
    paren_layer = 1
    cur_str = ''
    while i < len(func_expr) and paren_layer != 0:
        if func_expr[i] in ['\"', '\'', '`']:
            if len(quote_stack) > 0 and quote_stack[len(quote_stack) - 1] == func_expr[i]:
                quote_stack.pop()
            else:
                quote_stack.append(func_expr[i])
            cur_str = cur_str + func_expr[i]
        elif func_expr[i] == ',' and len(quote_stack) == 0 and paren_layer == 1:
            res.append(cur_str.strip())
            cur_str = ''
        elif func_expr[i] == '(' and len(quote_stack) == 0:
            paren_layer = paren_layer + 1
            cur_str = cur_str + func_expr[i]
        elif func_expr[i] == ')' and len(quote_stack) == 0:
            paren_layer = paren_layer - 1
            if paren_layer != 0:
                cur_str = cur_str + func_expr[i]
            else:
                res.append(cur_str.strip())
                cur_str = ''
        else:
            cur_str = cur_str + func_expr[i]
        i = i + 1
    return res


def date_format_trans(format_str: str):
    return ((format_str.replace('YYYY', '%Y').
             replace('yyyy', '%Y').replace('MM', '%m').
             replace('DD', '%d')).replace('dd', '%d').replace('MONTH', '%M').
            replace('MON', '%b').replace('HH24', '%H').replace('MI', '%M').
            replace('SS', '%S').replace('FF', '%f').replace('DY', '%a').
            replace('AM', '%p').replace('PM', '%p').replace('HH', '%I').replace('RR', '%y'))


def gen_interval(dialect, units: list, values: list, sign=False):
    year = 0
    month = 0
    day = 0
    hour = 0
    minute = 0
    second = 0
    millisecond = 0
    for idx, unit in enumerate(units):
        if unit == 'millennium':
            year = year + values[idx] * 1000
        elif unit == 'century':
            year = year + values[idx] * 100
        elif unit == 'decade':
            year = year + values[idx] * 10
        elif unit == 'year':
            year = year + values[idx]
        elif unit == 'quarter':
            month = month + values[idx] * 3
        elif unit == 'month':
            month = month + values[idx]
        elif unit == 'week':
            day = day + values[idx] * 7
        elif unit == 'day':
            day = day + values[idx]
        elif unit == 'hour':
            hour = hour + values[idx]
        elif unit == 'minute':
            minute = minute + values[idx]
        elif unit == 'second':
            second = second + values[idx]
        elif unit == 'millisecond':
            millisecond = millisecond + values[idx]
        else:
            assert False
    if sign:
        year = year * -1
        month = month * -1
        day = day * -1
        hour = hour * -1
        minute = minute * -1
        second = second * -1
        millisecond = millisecond * -1
    if dialect == 'oracle':
        "microsecond, second, minute, hour, day, week, month, quarter, year, decade, century, millennium"
        if day == 0 and hour == 0 and minute == 0 and second == 0 and millisecond == 0:
            return f"INTERVAL '{year}' YEAR TO MONTH"


def get_no_space_len(string: str):
    splits = self_split(string)
    length = 0
    for split in splits:
        length = length + len(split)
    return length


reserved_keywords = {}

with open(os.path.join(get_proj_root_path(), 'src', 'utils', 'mysql_reserved_keyword.json'), 'r') as f:
    reserved_keywords['mysql'] = json.load(f)

with open(os.path.join(get_proj_root_path(), 'src', 'utils', 'oracle_reserved_keyword.json'), 'r') as f:
    reserved_keywords['oracle'] = json.load(f)

with open(os.path.join(get_proj_root_path(), 'src', 'utils', 'pg_reserved_keyword.json'), 'r') as f:
    reserved_keywords['pg'] = json.load(f)


def get_used_reserved_keyword_list():
    return reserved_keywords


def get_table_col_name(name: str, dialect: str):
    if dialect == 'pg':
        if name.upper() in reserved_keywords['pg']:
            name = f"\"{name}\""
        elif ' ' in name or '-' in name:
            name = f"\"{name}\""
    elif dialect == 'mysql':
        if name.upper() in reserved_keywords['mysql']:
            name = f"`{name}`"
        elif ' ' in name or '-' in name:
            name = f"`{name}`"
    elif dialect == 'oracle':
        if name.upper() in reserved_keywords['oracle']:
            name = f"\"{name}\""
        elif ' ' in name or '-' in name:
            name = f"\"{name}\""
    else:
        assert False
    return name


def get_all_db_name(dialect: str):
    return 'all_db_final'


def get_db_ids():
    return [
        # 'chinook',
        'bird',
        'hr_order_entry',
        # 'sale_history',
        # 'customer_order',
        # 'snap',
        'tpch',
        'tpcds',
        # 'csail_stata_cinder',
        # 'csail_stata_glance',
        # 'csail_stata_neutron',
        # 'csail_stata_nova',
        'dw',
        # 'keystone'
    ]


def get_schema_path(db_id: str):
    if db_id not in get_db_ids():
        return None
    return os.path.join(get_data_path(), db_id)


def get_empty_db_name(db_name):
    return f'emp_{db_name}'


def no_space_and_case_insensitive_str(string_value: str):
    splits = string_value.split()
    result = ''
    for item in splits:
        result = result + item
    return result.lower()


def no_space_and_case_insensitive_str_eq(str1: str, str2: str):
    return no_space_and_case_insensitive_str(str1) == no_space_and_case_insensitive_str(str2)


def get_sql_folder_path(db_name: str):
    return os.path.join(get_proj_root_path(), 'SQL', db_name)


def scale_name_into_length(s: str, max_length: int = 30) -> str:
    parts = s.split('_')
    num_underscores = len(parts) - 1
    original_lengths = [len(p) for p in parts]
    total_chars = sum(original_lengths) + num_underscores  # 包括下划线的总长度
    if total_chars <= max_length:
        return s
    available_letters_length = max_length - num_underscores
    total_letters = sum(original_lengths)
    scaled_lengths = [
        max(1, int(round((length / total_letters) * available_letters_length)))
        for length in original_lengths
    ]
    while sum(scaled_lengths) > available_letters_length:
        idx = scaled_lengths.index(max(scaled_lengths))
        if scaled_lengths[idx] > 1:
            scaled_lengths[idx] -= 1
    result_parts = [part[:scaled_lengths[i]] for i, part in enumerate(parts)]
    return '_'.join(result_parts)
