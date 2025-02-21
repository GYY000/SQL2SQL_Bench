# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: tools$
# @Author: 10379
# @Time: 2024/12/9 20:18
import configparser
import os
import platform
from typing import List


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
        config_file = os.path.join(get_proj_root_path(), 'src', 'Config.ini')
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'dbg': config.getboolean("MODE", 'dbg'),

        "gpt_api_base": config.get("API", 'gpt_api_base'),
        "gpt_api_key": config.get("API", 'gpt_api_key'),
        "llama3.1_api_base": config.get("API", 'llama3.1_api_base'),
        "llama3.2_api_base": config.get("API", 'llama3.2_api_base'),
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

