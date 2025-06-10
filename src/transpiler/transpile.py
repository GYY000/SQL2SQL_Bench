# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: sqline_translate$
# @Author: 10379
# @Time: 2025/3/20 17:01
import json
import os
import re
import subprocess

import sqlglot

from db_builder.normalize import remove_sql_quote, remove_for_oracle
from model.model_init import parse_llm_answer, init_model
from transpiler.cracksql_driver.cracksql_driver import trans_func
from transpiler.model_prompt import LLM_REWRITE_SYS_PROMPT, LLM_REWRITE_USER_PROMPT
from utils.tools import get_proj_root_path, load_config

config = load_config()
if not config['cloud_mode']:
    sqlines_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines-3.3.133', 'sqlines-3.3.133',
                                'sqlines.exe')
    input_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines-3.3.133', 'script.sql')
    output_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines-3.3.133', 'sqlines_res.sql')
else:
    sqlines_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines', 'sqlines')
    input_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines', 'script.sql')
    output_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines_res.sql')


def normalize_whitespace(text):
    return re.sub(r'\s+', ' ', text).strip()


def transfer_sql_sqline(sql: str, src_dialect: str, tgt_dialect: str):
    sql = sql.strip(';').strip() + ';'
    if tgt_dialect == 'oracle':
        sql = remove_for_oracle(sql, src_dialect)
    else:
        sql = remove_sql_quote(sql, src_dialect)
    with open(input_file, 'w') as file:
        file.write(sql + '\n')

    sqlines_dialect_map = {
        "pg": "postgresql",
        "mysql": "mysql",
        "oracle": "oracle"
    }

    command = (f"{sqlines_path} -in={input_file} -s={sqlines_dialect_map[src_dialect]} "
               f"-out={output_file} -t={sqlines_dialect_map[tgt_dialect]}")

    os.system(command + f" > {os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines_cmd_out.txt')}")
    with open(output_file, 'r') as file:
        out_sqls = file.readlines()

    flag = True
    out_sql = out_sqls[1]
    out_sql = normalize_whitespace(out_sql)
    folder_path = os.path.join(get_proj_root_path(), 'src', 'transpiler')
    for file_name in os.listdir(folder_path):
        if not file_name.startswith('sqlines'):
            continue
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
    return flag, out_sql.strip()


def translate_sqlglot(sql: str, src_dialect: str, tgt_dialect: str):
    sqlglot_dialect_map = {
        "pg": "postgres",
        "mysql": "mysql",
        "oracle": "oracle"
    }
    src_dialect = sqlglot_dialect_map[src_dialect]
    tgt_dialect = sqlglot_dialect_map[tgt_dialect]
    sql = sqlglot.transpile(sql, src_dialect, tgt_dialect)
    return True, sql[0].strip()


def model_translate(sql: str, src_dialect: str, tgt_dialect: str):
    translator = init_model('moonshot-v1-128k')
    model_dialect_map = {
        "pg": "PostgreSQL",
        "mysql": "MySQL",
        "oracle": "Oracle"
    }
    src_dialect = model_dialect_map[src_dialect]
    tgt_dialect = model_dialect_map[tgt_dialect]
    sys_prompt = LLM_REWRITE_SYS_PROMPT.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect)
    user_prompt = LLM_REWRITE_USER_PROMPT.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect, sql=sql)
    answer_raw = translator.trans_func([], sys_prompt, user_prompt)
    print(answer_raw)
    pattern = r'"Answer":\s*(.*?)\s*,\s*"Reasoning":\s*(.*?)'
    # pattern = r'"SQL Snippet":\s*(.*?)\s*,\s*"Reasoning":\s*(.*?),\s*"Confidence":\s*(.*?)\s'
    res = parse_llm_answer(translator.model_id, answer_raw, pattern)
    folder_path = os.path.join(get_proj_root_path(), 'src', 'transpiler')
    with open(os.path.join(folder_path, 'llm_ans.json'), 'r') as file:
        data = json.load(file)
    data.append(answer_raw)
    with open(os.path.join(folder_path, 'llm_ans.json'), 'w') as file:
        json.dump(data, file)
    return True, res


def cracksql_translate(sql: str, src_dialect: str, tgt_dialect: str, db_name, db_para):
    if src_dialect == 'pg':
        db_used_name = 'Postgres Database Parameter'
    elif src_dialect == 'mysql':
        db_used_name = 'MySQL Database Parameter'
    elif src_dialect == 'oracle':
        db_used_name = 'Oracle Database Parameter'
    else:
        raise ValueError("Unsupported dialect")
    db_para = {
        db_used_name: db_para
    }
    translated_sql, model_ans_list, used_pieces, lift_histories = trans_func(sql, src_dialect, tgt_dialect, db_name,
                                                                             'moonshot-v1-128k', db_para)
    return translated_sql


def transpile_pipeline(sql: str, db_name, src_dialect: str, tgt_dialect: str):
    flag, out_sql = model_translate(sql, src_dialect, tgt_dialect)
    if not flag:
        flag, out_sql = transfer_sql_sqline(sql, src_dialect, tgt_dialect)
