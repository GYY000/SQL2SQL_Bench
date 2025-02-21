# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: verify$
# @Author: 10379
# @Time: 2025/2/21 13:04
import os.path
import subprocess

from utils.tools import get_proj_root_path, is_running_on_linux


def verify_sql_solver(sql1, sql2, schema):
    # verify one by one
    cur_path = os.path.join(get_proj_root_path(), 'src', 'verification')

    jar_path = os.path.join(cur_path, 'solver_dependency', 'sqlsolver-v1.1.0.jar')

    sql1_path = os.path.join(cur_path, 'sql1.sql')
    sql2_path = os.path.join(cur_path, 'sql2.sql')
    schema_path = os.path.join(cur_path, 'schema.sql')
    res_path = os.path.join(cur_path, 'res.txt')
    with open(sql1_path, 'w') as file:
        file.write(sql1)

    with open(sql2_path, 'w') as file:
        file.write(sql2)

    with open(schema_path, 'w') as file:
        file.write(schema)

    if is_running_on_linux():
        load_cmd = f"export LD_LIBRARY_PATH={os.path.join(cur_path, 'solver_dependency')}"
    else:
        load_cmd = f"set PATH={os.path.join(cur_path, 'solver_dependency')};%PATH%"
    cmd = load_cmd + f" & java -jar {jar_path} -sql1={sql1_path} -sql2={sql2_path} -schema={schema_path} -output={res_path}"

    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    verify_result = None
    with open(res_path, 'r') as file:
        contents = file.readlines()
        if contents[0].strip() == 'EQ':
            verify_result = "EQ"
        elif contents[0].strip() == 'NEQ':
            verify_result = "NEQ"
        elif contents[0].strip() == 'UNKNOWN':
            verify_result = "UNKNOWN"
        elif contents[0].strip() == 'TIMEOUT':
            verify_result = "TIMEOUT"
    os.remove(sql1_path)
    os.remove(sql2_path)
    os.remove(schema_path)
    os.remove(res_path)
    return {
        "sql1": sql1,
        "sql2": sql2,
        "res": verify_result
    }


def verify_sqls_solver(sql1s, sql2s, schema):
    # verify all
    cur_path = os.path.join(get_proj_root_path(), 'src', 'verification')
    jar_path = os.path.join(cur_path, 'solver_dependency', 'sqlsolver-v1.1.0.jar')
    sql1_path = os.path.join(cur_path, 'sql1.sql')
    sql2_path = os.path.join(cur_path, 'sql2.sql')
    schema_path = os.path.join(cur_path, 'schema.sql')
    res_path = os.path.join(cur_path, 'res.txt')

    with open(sql1_path, 'w') as file:
        flag = False
        for sql in sql1s:
            if flag:
                file.write('\n')
            flag = True
            file.write(sql)

    with open(sql2_path, 'w') as file:
        flag = False
        for sql in sql2s:
            if flag:
                file.write('\n')
            flag = True
            file.write(sql)

    with open(schema_path, 'w') as file:
        file.write(schema)

    if is_running_on_linux():
        load_cmd = f"export LD_LIBRARY_PATH={os.path.join(cur_path, 'solver_dependency')}"
    else:
        load_cmd = f"set PATH={os.path.join(cur_path, 'solver_dependency')};%PATH%"
    cmd = load_cmd + f" & java -jar {jar_path} -sql1={sql1_path} -sql2={sql2_path} -schema={schema_path} -output={res_path}"

    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    res = []
    with open(res_path, 'r') as file:
        contents = file.readlines()
        for i in range(len(sql1s)):
            verify_result = None
            if contents[0].strip() == 'EQ':
                verify_result = "EQ"
            elif contents[0].strip() == 'NEQ':
                verify_result = "NEQ"
            elif contents[0].strip() == 'UNKNOWN':
                verify_result = "UNKNOWN"
            elif contents[0].strip() == 'TIMEOUT':
                verify_result = "TIMEOUT"
            res.append({
                "sql1": sql1s[i],
                "sql2": sql2s[i],
                "res": verify_result
            })
    os.remove(sql1_path)
    os.remove(sql2_path)
    os.remove(schema_path)
    os.remove(res_path)
    return res
