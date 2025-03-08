import json
import os.path
import re
import subprocess

from typing import List

import mysql.connector

import psycopg2
from psycopg2 import Error

import cx_Oracle

from utils.tools import get_proj_root_path

mysql_conn_map = {}
mysql_cursor_map = {}

oracle_locate_open = False

database_mapping = {
    "customer_order": {
        "mysql": "cus_order",
        "pg": "cus_order",
        "oracle": "orc_sample_db",
    },
    "human_resource": {
        "mysql": "order_entry",
        "pg": "tpch",
        "oracle": "tpch"
    },
    "sale_history": {
        "mysql": "",
        "pg": "",
        "oracle": "orcl"
    },
    "order_enrty": {
        "mysql": "order_entry",
        "pg": "tpch",
        "oracle": "tpch"
    },
    "snap": {
        "mysql": "snap",
        "pg": "snap",
        "oracle": "tpch"
    },
}

def sql_execute(dialect: str, db_name: str, sql: str):
    if dialect == 'pg':
        return pg_sql_execute(db_name, sql)
    elif dialect == 'mysql':
        return mysql_sql_execute(db_name, sql)
    elif dialect == 'oracle':
        return oracle_sql_execute(db_name, sql)
    else:
        raise ValueError(f"{dialect} is not supported")


def mysql_db_connect(dbname):
    try:
        # 连接数据库
        connection = mysql.connector.connect(
            host='localhost',  # 数据库主机地址，默认为localhost
            port=3306,  # MySQL默认端口
            user='root',  # 数据库用户名
            password='021021',  # 数据库密码
            database=dbname  # 要连接的数据库名称
        )
        cursor = connection.cursor()
        mysql_conn_map[dbname] = connection
        mysql_cursor_map[dbname] = cursor
        if connection.is_connected():
            return connection, cursor
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")


def mysql_sql_execute(db_name: str, sql):
    if db_name not in mysql_conn_map:
        mysql_db_connect(db_name)
    connection = mysql_conn_map[db_name]
    cursor = mysql_cursor_map[db_name]
    try:
        cursor.execute(sql.strip(';'))
        rows = cursor.fetchall()
        connection.commit()
        return True, rows
    except mysql.connector.Error as e:
        connection.rollback()
        return False, e.args[1]


def close_mysql_connnect(dbname: str):
    connection = mysql_conn_map[dbname]
    cursor = mysql_cursor_map[dbname]
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


mysql_types = None


def get_mysql_type_by_oid(type_code):
    global mysql_types
    if mysql_types is None:
        with open(os.path.join(get_proj_root_path(), 'src', 'schema', 'mysql_types.json'), 'r') as file:
            mysql_types = json.loads(file.read())
    if str(type_code) in mysql_types:
        return mysql_types[str(type_code)]
    else:
        return "UNKNOWN"


def get_mysql_type(obj: str, db_name: str, is_table: bool) -> tuple[bool, List]:
    if db_name not in mysql_conn_map:
        mysql_db_connect(db_name)
    connection = mysql_conn_map[db_name]
    cursor = mysql_cursor_map[db_name]
    if is_table:
        sql = f"SELECT * FROM {obj} LIMIT 1"
    else:
        sql = obj

    try:
        cursor.execute(sql)
        res = []

        columns = cursor.description
        res = []

        for column in columns:
            col_name = column[0]
            col_type_code = column[1]
            col_type = get_mysql_type_by_oid(col_type_code)
            res.append({
                "col": col_name,
                "type": col_type
            })
        rows = cursor.fetchall()
        connection.commit()
        return True, res
    except mysql.connector.Error as e:
        connection.rollback()
        return False, [str(e)]


pg_conn_map = {}
pg_cursor_map = {}


def pg_db_connect(dbname):
    try:
        # 连接数据库
        connection = psycopg2.connect(
            host='localhost',  # 数据库主机地址，默认为localhost
            port='5432',  # PostgreSQL默认端口
            user='postgres',  # 数据库用户名
            password='021021',  # 数据库密码

            dbname=dbname  # 要连接的数据库名称
        )

        cursor = connection.cursor()

        pg_conn_map[dbname] = connection
        pg_cursor_map[dbname] = cursor
        cursor.execute("SET lc_messages TO 'en_US.UTF-8';")
        if connection:
            return connection, cursor

    except (Exception, Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")


def pg_sql_execute(db_name: str, sql):
    if db_name not in pg_conn_map:
        pg_db_connect(db_name)
    connection = pg_conn_map[db_name]
    cursor = pg_cursor_map[db_name]
    try:
        cursor.execute(sql.strip(';'))
        if cursor.description:
            rows = cursor.fetchall()
        else:
            rows = None
        connection.commit()
        return True, rows
    except (Exception, Error) as error:
        connection.rollback()
        return False, f"Error while executing PostgreSQL query: {error}"


def close_pg_connnect(db_name: str):
    connection = pg_conn_map[db_name]
    cursor = pg_cursor_map[db_name]
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


pg_types = None


def get_type_name_by_oid(oid):
    global pg_types
    if pg_types is None:
        with open(os.path.join(get_proj_root_path(), 'src', 'schema', 'pg_types.json'), 'r') as file:
            pg_types = json.loads(file.read())
    if str(oid) in pg_types:
        return pg_types[str(oid)]
    else:
        return "UNKNOWN"


def get_pg_type(obj: str, db_name: str, is_table: bool) -> tuple[bool, list]:
    if db_name not in pg_conn_map:
        pg_db_connect(db_name)
    connection = pg_conn_map[db_name]
    cursor = pg_cursor_map[db_name]
    if is_table:
        sql = f"SELECT * FROM {obj} LIMIT 1"
    else:
        sql = obj
    try:
        cursor.execute(sql)
        res = []
        if cursor.description:
            for column in cursor.description:
                res.append({
                    "col": column.name,
                    "type": get_type_name_by_oid(column.type_code)
                })
        connection.commit()
        return True, res
    except (Exception, Error) as error:
        connection.rollback()
        return False, [f"Error while executing PostgreSQL query: {error}"]


oracle_conn_map = {}
oracle_cursor_map = {}


def oracle_db_connect(db_name):
    if db_name != 'bird':
        connection = cx_Oracle.connect(user="system", password="021021", dsn=db_name)
    else:
        dsn = cx_Oracle.makedsn("8.131.229.55", "49161", service_name="XE")
        connection = cx_Oracle.connect("BIRD", "dmai4db2021.", dsn)
    cursor = connection.cursor()
    oracle_cursor_map[db_name] = cursor
    oracle_conn_map[db_name] = connection
    return connection, cursor


def oracle_sql_execute(db_name: str, sql: str, sql_plus_flag=False):
    if not sql_plus_flag:
        if db_name not in oracle_conn_map:
            oracle_db_connect(db_name)
        connection = oracle_conn_map[db_name]
        cursor = oracle_cursor_map[db_name]
        try:
            cursor.execute(sql)
            if "SELECT" in sql.upper():
                result = cursor.fetchall()
                return True, result
            else:
                connection.commit()
                return True, []
        except Exception as e:
            # Handle the exception, log the error, and optionally raise
            error_message = str(e)
            print(f"Error executing SQL on database {db_name}: {error_message}")
    else:
        sqlplus_path = "sqlplus"
        user = "SYSTEM"
        password = "021021"
        server_ip = 'localhost'
        server_port = 1521
        db_string = db_name
        sqlplus_command = f"{sqlplus_path} -S {user}/{password}@{server_ip}:{server_port}/{db_string}"
        process = subprocess.run(sqlplus_command, shell=True,
                                 input='alter session set nls_language=american;\n ' + sql + ';',
                                 text=True, capture_output=True)
        return False, f"Error while executing Oracle query: {process.stdout}"


def close_oracle_connnect(db_name: str):
    connection = oracle_conn_map[db_name]
    cursor = oracle_cursor_map[db_name]
    if connection:
        cursor.close()
        connection.close()


oracle_conn_local_map = {}
oracle_cursor_local_map = {}


def get_oracle_type(obj: str, db_name, is_table: bool) -> tuple[bool, list]:
    try:
        connection = cx_Oracle.connect(user="system", password="021021", dsn=db_name)
        cursor = connection.cursor()
        if is_table:
            sql = f"SELECT * FROM {obj}"
        else:
            sql = obj
        cursor.execute(sql)

        res = []
        for column in cursor.description:
            match = re.search(r'DB_TYPE_(\w+)', column[1].name)
            assert match
            type_name = match.group(1)
            res.append({
                "col": column[0],
                "type": type_name
            })
        cursor.fetchall()
        return True, res
    except Exception as e:
        raise e
