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

from db_builder.schema_builder import create_table, schema_build
from model.model_init import parse_llm_answer, init_model
from transpiler.cracksql_driver.cracksql_driver import trans_func
from transpiler.model_prompt import LLM_REWRITE_SYS_PROMPT, LLM_REWRITE_USER_PROMPT, LLM_FEED_BACK_SYS_PROMPT, \
    LLM_FEED_BACK_USER_PROMPT, DB_PARAM_SYS_PROMPT, DB_PARAM_USER_PROMPT, TYPE_MAPPING_SYS_PROMPT, \
    TYPE_MAPPING_USER_PROMPT, TRAN_HISTORY_SYS_PROMPT, TRAN_HISTORY_USER_PROMPT
from utils.db_connector import sql_dependent_execute
from utils.tools import get_proj_root_path, load_config, get_all_db_name, get_db_ids
from verification.verify import post_process_for_reserved_keyword

config = load_config()


def normalize_whitespace(text):
    return re.sub(r'\s+', ' ', text).strip()


def transfer_sql_sqline(sql: str, src_dialect: str, tgt_dialect: str, tid: int = 0):
    if not config['cloud_mode']:
        sqlines_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines-3.3.133', 'sqlines-3.3.133',
                                    'sqlines.exe')
        input_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines-3.3.133', f'script_{tid}.sql')
        output_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines-3.3.133',
                                   f'sqlines_res_{tid}.sql')
    else:
        sqlines_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines', 'sqlines')
        input_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines', f'script_{tid}.sql')
        output_file = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'sqlines', f'sqlines_res_{tid}.sql')
    sql = sql.strip(';').strip() + ';'
    with open(input_file, 'w') as file:
        file.write(sql + '\n')
    sqlines_dialect_map = {
        "pg": "postgresql",
        "mysql": "mysql",
        "oracle": "oracle"
    }
    cnt = 0
    while True:
        command = (f"{sqlines_path} -in={input_file} -s={sqlines_dialect_map[src_dialect]} "
                   f"-out={output_file} -t={sqlines_dialect_map[tgt_dialect]}")

        os.system(command + f" > {os.path.join(get_proj_root_path(), 'src', 'transpiler', f'sqlines_cmd_out_{tid}.txt')}")
        if os.path.exists(output_file) or cnt >= 5:
            break
        cnt += 1
    if cnt >= 5:
        return True, 'Wrong Sqlines'
    with open(output_file, 'r') as file:
        out_sqls = file.readlines()
    flag = True
    out_sql = '\n'.join(out_sqls[1:])
    out_sql = normalize_whitespace(out_sql)
    folder_path = os.path.join(get_proj_root_path(), 'src', 'transpiler')
    for file_name in os.listdir(folder_path):
        if not file_name.startswith('sqlines'):
            continue
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
    return flag, out_sql.strip()


def make_db_para(db_para: dict):
    res = {}
    for dialect, params in db_para.items():
        if dialect == 'pg':
            db_used_name = 'Postgres Database Parameter'
        elif dialect == 'mysql':
            db_used_name = 'MySQL Database Parameter'
        elif dialect == 'oracle':
            db_used_name = 'Oracle Database Parameter'
        else:
            raise ValueError("Unsupported dialect")
        new_params = {}
        for key, value in params.items():
            assert key in para_description
            new_params[key] = {
                "value": value,
                "description": para_description[key]
            }
        if len(params) != 0:
            res[db_used_name] = new_params
    return res


def translate_sqlglot(sql: str, src_dialect: str, tgt_dialect: str):
    sqlglot_dialect_map = {
        "pg": "postgres",
        "mysql": "mysql",
        "oracle": "oracle"
    }
    src_dialect = sqlglot_dialect_map[src_dialect]
    tgt_dialect = sqlglot_dialect_map[tgt_dialect]
    try:
        sql = sqlglot.transpile(sql, src_dialect, tgt_dialect)
    except Exception as e:
        return False, str(e)
    return True, sql[0].strip()


type_mapping_table = {
    "mysql": {
        "pg": {
            "YEAR": "SMALLINT",
            "POINT": "SDO_GEOMETRY",
            "BLOB": "BYTEA"
        },
        "oracle": {
            "YEAR": "NUMBER(4)",
            "POINT": "GEOMETRY",
            "BOOL": "NUMBER(1)"
        }
    },
    "pg": {
        "mysql": {
            "UUID": "CHAR(36)",
            "GEOMETRY": "POINT",
            "JSONB": "JSON",
            "XML": "TEXT",
            "ARRAY": "JSON",
        },
        "oracle": {
            "UUID": "CHAR(36)",
            "GEOMETRY": "SDO_GEOMETRY",
            "JSONB": "JSON",
            "XML": "XMLType",
            "ARRAY": "VARRAY",
            "BOOL": "NUMBER(1)"
        }
    },
    "oracle": {
        "mysql": {
            "SDO_GEOMETRY": "POINT",
            "XMLType": "XML",
            "VARRAY": "JSON",
        },
        "pg": {
            "SDO_GEOMETRY": "GEOMETRY",
            "XMLType": "XML",
            "VARRAY": "ARRAY"
        }
    }
}

para_description = {
    "NLS_DATE_FORMAT": "NLS_DATE_FORMAT specifies the default date format to use with the TO_CHAR and TO_DATE functions. The default value of this parameter is determined by NLS_TERRITORY. The value of this parameter can be any valid date format mask, and the value must be surrounded by double quotation marks. For example: NLS_DATE_FORMAT = \"MM/DD/YYYY\"",
    "NLS_TIMESTAMP_FORMAT": "NLS_TIMESTAMP_FORMAT defines the datetime format model to use with the TO_CHAR and TO_TIMESTAMP functions.",
    "NLS_DATE_LANGUAGE": "NLS_DATE_LANGUAGE specifies the language to use for the spelling of day and month names and date abbreviations (a.m., p.m., AD, BC) returned by the TO_DATE and TO_CHAR functions.",
    "datestyle": "Sets the display format for date and time values, as well as the rules for interpreting ambiguous date input values. For historical reasons, this variable contains two independent components: the output format specification (ISO, Postgres, SQL, or German) and the input/output specification for year/month/day ordering (DMY, MDY, or YMD). These can be set separately or together. The keywords Euro and European are synonyms for DMY; the keywords US, NonEuro, and NonEuropean are synonyms for MDY. See Section 8.5 for more information. The built-in default is ISO, MDY, but initdb will initialize the configuration file with a setting that corresponds to the behavior of the chosen lc_time locale.",
    "NLS_NUMERIC_CHARACTERS": "NLS_NUMERIC_CHARACTERS specifies the characters to use as the group separator and decimal character."
}


def make_no_feedback_prompt(sql: dict, src_dialect: str, tgt_dialect: str, db_param: dict):
    model_dialect_map = {
        "pg": "PostgreSQL",
        "mysql": "MySQL",
        "oracle": "Oracle"
    }
    ddl = create_stmt_fetch(sql['tables'], src_dialect)
    ddls = '\n'.join(ddl)

    sql_input = sql[src_dialect]
    type_mapping = type_mapping_table[src_dialect][tgt_dialect]

    prompt_src_dialect = model_dialect_map[src_dialect]
    prompt_tgt_dialect = model_dialect_map[tgt_dialect]
    if len(db_param[src_dialect]) == 0 and len(db_param[tgt_dialect]) == 0:
        db_param_sys = ''
        db_param_usr = ''
    else:
        db_param_sys = DB_PARAM_SYS_PROMPT
        parameter = json.dumps(make_db_para(db_param), indent=2)
        db_param_usr = DB_PARAM_USER_PROMPT.format(parameter=parameter,
                                                   src_dialect=prompt_src_dialect, tgt_dialect=prompt_tgt_dialect)

    if len(type_mapping) == 0:
        type_mapping_sys = ""
        type_mapping_usr = ""
    else:
        type_mapping_sys = TYPE_MAPPING_SYS_PROMPT.format(src_dialect=prompt_src_dialect,
                                                          tgt_dialect=prompt_tgt_dialect)
        type_mapping_usr = TYPE_MAPPING_USER_PROMPT.format(type_mapping=json.dumps(type_mapping, indent=2),
                                                           src_dialect=prompt_src_dialect,
                                                           tgt_dialect=prompt_tgt_dialect)

    sys_prompt = LLM_REWRITE_SYS_PROMPT.format(src_dialect=prompt_src_dialect, tgt_dialect=prompt_tgt_dialect,
                                               db_param_sys=db_param_sys, type_mapping_sys=type_mapping_sys)
    user_prompt = LLM_REWRITE_USER_PROMPT.format(src_dialect=prompt_src_dialect, tgt_dialect=prompt_tgt_dialect,
                                                 sql=sql_input,
                                                 db_param_stmt=db_param_usr, type_mapping_stmt=type_mapping_usr,
                                                 ddl=ddls)
    return sys_prompt, user_prompt


def make_feedback_prompt(sql: dict, src_dialect: str, tgt_dialect: str, db_param: dict,
                         tran_history: list[dict]):
    model_dialect_map = {
        "pg": "PostgreSQL",
        "mysql": "MySQL",
        "oracle": "Oracle"
    }
    prompt_src_dialect = model_dialect_map[src_dialect]
    prompt_tgt_dialect = model_dialect_map[tgt_dialect]
    if len(db_param[src_dialect]) == 0 and len(db_param[tgt_dialect]) == 0:
        db_param_sys = ''
        db_param_usr = ''
    else:
        db_param_sys = DB_PARAM_SYS_PROMPT
        parameter = json.dumps(make_db_para(db_param), indent=2)
        db_param_usr = DB_PARAM_USER_PROMPT.format(parameter=parameter,
                                                   src_dialect=prompt_src_dialect, tgt_dialect=prompt_tgt_dialect)
    type_mapping = type_mapping_table[src_dialect][tgt_dialect]
    if len(type_mapping) == 0:
        type_mapping_sys = ""
        type_mapping_usr = ""
    else:
        type_mapping_sys = TYPE_MAPPING_SYS_PROMPT.format(src_dialect=prompt_src_dialect,
                                                          tgt_dialect=prompt_tgt_dialect)
        type_mapping_usr = TYPE_MAPPING_USER_PROMPT.format(type_mapping=json.dumps(type_mapping, indent=2),
                                                           src_dialect=prompt_src_dialect,
                                                           tgt_dialect=prompt_tgt_dialect)
    tran_history_sys_prompt = TRAN_HISTORY_SYS_PROMPT.format(src_dialect=prompt_src_dialect,
                                                             tgt_dialect=prompt_tgt_dialect)
    tran_history_usr_prompt = TRAN_HISTORY_USER_PROMPT.format(history=json.dumps(tran_history, indent=2))
    ddl = create_stmt_fetch(sql['tables'], src_dialect)
    ddls = '\n'.join(ddl)

    sql_input = sql[src_dialect]

    sys_prompt = LLM_FEED_BACK_SYS_PROMPT.format(src_dialect=prompt_src_dialect, tgt_dialect=prompt_tgt_dialect,
                                                 db_param_sys=db_param_sys, type_mapping_sys=type_mapping_sys,
                                                 history_sys=tran_history_sys_prompt)
    user_prompt = LLM_FEED_BACK_USER_PROMPT.format(src_dialect=prompt_src_dialect, tgt_dialect=prompt_tgt_dialect,
                                                   sql=sql_input, db_param_stmt=db_param_usr,
                                                   type_mapping_stmt=type_mapping_usr,
                                                   ddl=ddls, history_stmt=tran_history_usr_prompt)
    return sys_prompt, user_prompt


def model_translate(sql: dict, src_dialect: str, tgt_dialect: str, model_id: str, db_param: dict, tid: str = '0'):
    translator = init_model(model_id)
    sys_prompt, user_prompt = make_no_feedback_prompt(sql, src_dialect, tgt_dialect, db_param)
    answer_raw = translator.trans_func([], sys_prompt, user_prompt)
    pattern = r'"Answer":\s*(.*?)\s*,\s*"Reasoning":\s*(.*?)'
    # pattern = r'"SQL Snippet":\s*(.*?)\s*,\s*"Reasoning":\s*(.*?),\s*"Confidence":\s*(.*?)\s'
    res = parse_llm_answer(translator.model_id, answer_raw, pattern)
    folder_path = os.path.join(get_proj_root_path(), 'src', 'transpiler')
    if os.path.exists(os.path.join(folder_path, f'{model_id}_llm_ans_{tid}.json')):
        with open(os.path.join(folder_path, f'{model_id}_llm_ans_{tid}.json'), 'r') as file:
            data = json.load(file)
    else:
        data = []
    data.append(answer_raw)
    with open(os.path.join(folder_path, f'{model_id}_llm_ans_{tid}.json'), 'w') as file:
        json.dump(data, file)
    return res


def feed_back_model(sql: dict, src_dialect, tgt_dialect, db_para: dict, max_retry_time: int, model_id: str,
                    tid: int = 0):
    ori_sql = sql[src_dialect]
    translator = init_model(model_id)
    sys_prompt, user_prompt = make_no_feedback_prompt(sql, src_dialect, tgt_dialect, db_para)
    answer_raw = translator.trans_func([], sys_prompt, user_prompt)
    pattern = r'"Answer":\s*(.*?)\s*,\s*"Reasoning":\s*(.*?)'
    res_sql = parse_llm_answer(translator.model_id, answer_raw, pattern)
    folder_path = os.path.join(get_proj_root_path(), 'src', 'transpiler')
    if os.path.exists(os.path.join(folder_path, f'{model_id}_feed_back_llm_ans_{tid}.json')):
        with open(os.path.join(folder_path, f'{model_id}_feed_back_llm_ans_{tid}.json'), 'r') as file:
            data = json.load(file)
    else:
        data = []
    data.append(answer_raw)
    with open(os.path.join(folder_path, f'{model_id}_feed_back_llm_ans_{tid}.json'), 'w') as file:
        json.dump(data, file, indent=4)
    retry_time = 0
    history = []
    last_success_sql = None
    if res_sql != "Answer not returned in the given format!":
        last_success_sql = res_sql
    while retry_time < max_retry_time:
        if res_sql == "Answer not returned in the given format!":
            history.append({
                "OriginalSQL": ori_sql,
                "AttemptedTranslation": 'You give translation in the wrong format, Please follow the format!',
                "Issue": 'Answer not returned in the given format!',
            })
        else:
            res_sql = post_process_for_reserved_keyword(res_sql, src_dialect, tgt_dialect)
            if tgt_dialect == 'oracle':
                explain_sql = f"EXPLAIN PLAN FOR {res_sql}"
            else:
                explain_sql = f"EXPLAIN {res_sql}"
            flag, res = sql_dependent_execute(tgt_dialect, get_all_db_name(tgt_dialect),
                                              explain_sql, db_para.get(tgt_dialect, {}))
            if flag:
                break
            else:
                history.append({
                    "OriginalSQL": ori_sql,
                    "AttemptedTranslation": res_sql,
                    "Issue": res,
                })
        retry_time += 1
        sys_prompt, user_prompt = make_feedback_prompt(sql, src_dialect, tgt_dialect, db_para, history)
        answer_raw = translator.trans_func([], sys_prompt, user_prompt)
        pattern = r'"Answer":\s*(.*?)\s*,\s*"Reasoning":\s*(.*?)'
        res_sql = parse_llm_answer(translator.model_id, answer_raw, pattern)
        if res_sql != 'Answer not returned in the given format!':
            last_success_sql = res_sql
        folder_path = os.path.join(get_proj_root_path(), 'src', 'transpiler')
        with open(os.path.join(folder_path, f'{model_id}_feed_back_llm_ans_{tid}.json'), 'r') as file:
            data = json.load(file)
        data.append(answer_raw)
        with open(os.path.join(folder_path, f'{model_id}_feed_back_llm_ans_{tid}.json'), 'w') as file:
            json.dump(data, file, indent=4)
    if last_success_sql is None:
        last_success_sql = "Answer not returned in the given format!"
    return last_success_sql


def ora2pg_tran(sql: str, src_dialect: str, tgt_dialect: str, db_name, ora_id: str = '0'):
    assert tgt_dialect == 'pg'
    root_path = os.path.join(get_proj_root_path(), 'src', 'transpiler')
    input_path = os.path.join(root_path, f'input_{ora_id}.sql')
    output_path = os.path.join(root_path, f'output_{ora_id}.sql')
    with open(input_path, 'w') as file:
        file.write(sql.strip().strip(';') + ';')
    config_path = os.path.join(root_path, 'ora2pg.conf')
    ora2pg_env = os.environ.copy()
    ora2pg_env["PATH"] = ora2pg_env.get("PATH", "") + ":/home/gyy/perl5/bin"
    ora2pg_env['PERL5LIB'] = '/home/gyy/perl5/lib/perl5:/home/gyy/perl5/lib/perl5'
    ora2pg_env['PERL_MB_OPT'] = '--install_base "/home/gyy/perl5"'
    ora2pg_env['PERL_MM_OPT'] = 'INSTALL_BASE=/home/gyy/perl5'
    ora2pg_env['PERL_LOCAL_LIB_ROOT'] = '/home/gyy/perl5:/home/gyy/perl5'
    if src_dialect == 'oracle':
        cmd = ["ora2pg", "-i", input_path, "-o", output_path, "-c", config_path]
    else:
        cmd = ["ora2pg", "-m", "-i", input_path, "-o", output_path, "-c", config_path]
    result = subprocess.run(cmd, env=ora2pg_env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Command failed with exit code {result.returncode}")
    with open(output_path, 'r') as file:
        content = file.read().splitlines()
        i = 0
        for i in range(len(content)):
            if content[i] == '\\set ON_ERROR_STOP ON':
                break
        res = ''
        i = i + 1
        while i < len(content):
            res = res + content[i] + ' '
            i = i + 1
        res = res.strip()
        return res


def create_stmt_fetch(tables: list[str], dialect: str):
    # fetch the sub-graph in db
    db_ids = get_db_ids()
    create_statements = []
    for db_id in db_ids:
        schema, add_constraints, type_defs = schema_build(db_id, dialect)
        for table in tables:
            if table in schema:
                create_table_stmt = create_table(schema[table], add_constraints[table], dialect)
                create_statements.append(create_table_stmt)
                create_statements = create_statements + type_defs[table]
    return create_statements


def cracksql_translate(sql: str, src_dialect: str, tgt_dialect: str, db_name,
                       model_id: str = 'deepseek-r1-250528', out_dir='./'):
    flag, translated_sql, model_ans_list, used_pieces, lift_histories = trans_func(sql, src_dialect, tgt_dialect,
                                                                                   db_name,
                                                                                   model_id, None, out_dir)
    return flag, translated_sql, lift_histories, used_pieces
