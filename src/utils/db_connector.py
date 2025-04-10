import json
import os.path
import re
import subprocess

from typing import List

import mysql.connector

import psycopg2

import oracledb

from sql_gen.generator.ele_type.type_operation import load_col_type
from utils.tools import get_proj_root_path, load_mysql_config, load_pg_config, load_oracle_config

mysql_conn_map = {}
mysql_cursor_map = {}

oracle_locate_open = False

mysql_config = load_mysql_config()
pg_config = load_pg_config()
ora_config = load_oracle_config()

oracledb.init_oracle_client(lib_dir=ora_config['oracle_instant_path'])

database_mapping = {
    "customer_order": {
        "mysql": "cus_order",
        "pg": "cus_order",
        "oracle": "orc_sample_db",
    },
    "hr_order_entry": {
        "mysql": "order_entry",
        "pg": "order_entry",
        "oracle": "order_entry"
    },
    "sale_history": {
        "mysql": "sale_his",
        "pg": "sale_his",
        "oracle": "sale_his"
    },
    "snap": {
        "mysql": "snap",
        "pg": "snap",
        "oracle": "snap"
    },
    "chinook": {
        "mysql": "chinook",
        "pg": "chinook",
        "oracle": "chinook"
    },
    "bird": {
        "mysql": "bird",
        "pg": "bird",
        "oracle": "bird"
    },
    "tpch": {
        "mysql": "tpch",
        "pg": "tpch",
        "oracle": "tpch"
    },
    "tpcds": {
        "mysql": "tpcds",
        "pg": "tpcds",
        "oracle": "tpcds"
    }
}


def get_db_name(dialect, db_name):
    if db_name not in database_mapping:
        assert False
    else:
        return database_mapping[db_name][dialect]


def sql_execute(dialect: str, db_name: str, sql: str):
    if dialect == 'pg':
        return pg_sql_execute(db_name, sql)
    elif dialect == 'mysql':
        return mysql_sql_execute(db_name, sql)
    elif dialect == 'oracle':
        return oracle_sql_execute(db_name, sql)
    else:
        raise ValueError(f"{dialect} is not supported")


def show_mysql_databases():
    try:
        connection = mysql.connector.connect(
            host=mysql_config['mysql_host'],
            port=mysql_config['mysql_port'],
            user=mysql_config['mysql_user'],
            password=mysql_config['mysql_pwd']
        )
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]
        return databases
    except mysql.connector.Error as err:
        print(f"error raised: {err}")
        return None


def mysql_drop_db(db_name: str):
    try:
        connection = mysql.connector.connect(
            host=mysql_config['mysql_host'],
            port=mysql_config['mysql_port'],
            user=mysql_config['mysql_user'],
            password=mysql_config['mysql_pwd']
        )
        db_name = get_db_name('mysql', db_name)
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE `{db_name}`")
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Database '{db_name}' dropped successfully!")
    except mysql.connector.Error as err:
        print(f"error raised: {err}")


def mysql_db_connect(dbname):
    try:
        # 建立连接
        connection = mysql.connector.connect(
            host=mysql_config['mysql_host'],
            port=mysql_config['mysql_port'],
            user=mysql_config['mysql_user'],
            password=mysql_config['mysql_pwd'],
        )
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]

        if dbname not in databases:
            print(f"Database '{dbname}' don't exist, start creating")
            cursor.execute(f"CREATE DATABASE `{dbname}`")
            print(f"Database '{dbname}' create successfully!")
            connection.commit()
        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        print(f"error raised: {err}")

    try:
        connection = mysql.connector.connect(
            host=mysql_config['mysql_host'],
            port=mysql_config['mysql_port'],
            user=mysql_config['mysql_user'],
            password=mysql_config['mysql_pwd'],
            database=dbname
        )
        cursor = connection.cursor()
        mysql_conn_map[dbname] = connection
        mysql_cursor_map[dbname] = cursor
        if connection.is_connected():
            return connection, cursor
    except mysql.connector.Error as e:
        print(f"Error while connecting to MySQL: {e}")


def mysql_sql_execute(db_name: str, sql):
    db_name = get_db_name('mysql', db_name)
    if db_name not in mysql_conn_map:
        mysql_db_connect(db_name)
    connection = mysql_conn_map[db_name]
    cursor = mysql_cursor_map[db_name]
    try:
        cursor.execute(sql.strip().strip(';'))
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
        with open(os.path.join(get_proj_root_path(), 'src', 'sql_gen', 'generator', 'ele_type', 'mysql_types.json'),
                  'r') as file:
            mysql_types = json.loads(file.read())
    if str(type_code) in mysql_types:
        return mysql_types[str(type_code)]
    else:
        return "UNKNOWN"


def get_mysql_type(obj: str, db_name: str, is_table: bool) -> tuple[bool, List]:
    if is_table:
        table_name = obj
        with open(os.path.join(get_proj_root_path(), 'data', db_name, 'schema.json'), 'r') as file:
            schema = json.loads(file.read())
        if table_name not in schema:
            raise ValueError(f"Table {table_name} not found in schema")
        else:
            res = []
            for col in schema[table_name]['cols']:
                col_name = col['col']
                col_type, _, _ = load_col_type(col['type'], col['col_name'], 'mysql')
                res.append({
                    "col": col_name,
                    "type": col_type
                })
            return True, res
    try:
        db_name = get_db_name('mysql', db_name)
        if db_name not in mysql_conn_map:
            mysql_db_connect(db_name)
        connection = mysql_conn_map[db_name]
        cursor = mysql_cursor_map[db_name]
        sql = obj
        cursor.execute(sql)
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
        print(obj)
        connection.rollback()
        return False, [str(e)]


pg_conn_map = {}
pg_cursor_map = {}


def show_pg_databases():
    try:
        connection = psycopg2.connect(
            host=pg_config['pg_host'],
            port=pg_config['pg_port'],
            user=pg_config['pg_user'],
            password=pg_config['pg_pwd'],
            database='postgres'
        )
        cursor = connection.cursor()
        cursor.execute("SELECT datname FROM pg_database;")
        databases = [db[0] for db in cursor.fetchall()]
        return databases
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None


def pg_drop_db(db_name: str):
    try:
        db_name = get_db_name('pg', db_name)
        connection = psycopg2.connect(
            host=pg_config['pg_host'],
            port=pg_config['pg_port'],
            user=pg_config['pg_user'],
            password=pg_config['pg_pwd'],
            database='postgres'
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE \"{db_name}\";")
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Database '{db_name}' dropped successfully!")
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")


def pg_db_connect(dbname):
    flag = False
    try:
        connection = psycopg2.connect(
            host=pg_config['pg_host'],
            port=pg_config['pg_port'],
            user=pg_config['pg_user'],
            password=pg_config['pg_pwd'],
            database='postgres'
        )
        connection.autocommit = True

        cursor = connection.cursor()

        cursor.execute("SELECT datname FROM pg_database;")
        databases = [db[0] for db in cursor.fetchall()]

        if dbname not in databases:
            print(f"Database '{dbname}' does not exist, creating...")
            cursor.execute(f"CREATE DATABASE \"{dbname}\";")
            print(f"Database '{dbname}' created successfully!")
            connection.commit()
            flag = True
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")

    try:
        connection = psycopg2.connect(
            host=pg_config['pg_host'],
            port=pg_config['pg_port'],
            user=pg_config['pg_user'],
            password=pg_config['pg_pwd'],
            dbname=dbname
        )

        cursor = connection.cursor()

        pg_conn_map[dbname] = connection
        pg_cursor_map[dbname] = cursor
        cursor.execute("SET lc_messages TO 'en_US.UTF-8';")
        if flag:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        connection.commit()
        if connection:
            return connection, cursor

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")


def pg_sql_execute(db_name: str, sql):
    db_name = get_db_name('pg', db_name)
    if db_name not in pg_conn_map:
        pg_db_connect(db_name)
    connection = pg_conn_map[db_name]
    cursor = pg_cursor_map[db_name]
    try:
        cursor.execute(sql.strip().strip(';'))
        if cursor.description:
            rows = cursor.fetchall()
        else:
            rows = None
        connection.commit()
        return True, rows
    except (Exception, psycopg2.Error) as error:
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
        with open(os.path.join(get_proj_root_path(), 'src', 'sql_gen', 'generator', 'ele_type', 'pg_types.json'),
                  'r') as file:
            pg_types = json.loads(file.read())
    if str(oid) in pg_types:
        return pg_types[str(oid)]
    else:
        return "UNKNOWN"


def get_pg_type(obj: str, db_name: str, is_table: bool) -> tuple[bool, list]:
    if is_table:
        table_name = obj
        with open(os.path.join(get_proj_root_path(), 'data', db_name, 'schema.json'), 'r') as file:
            schema = json.loads(file.read())
        if table_name not in schema:
            raise ValueError(f"Table {table_name} not found in schema")
        else:
            res = []
            for col in schema[table_name]['cols']:
                col_name = col['col']
                col_type, _, _ = load_col_type(col['type'], col['col_name'], 'pg')
                res.append({
                    "col": col_name,
                    "type": col_type
                })
            return True, res

    try:
        db_name = get_db_name('pg', db_name)
        if db_name not in pg_conn_map:
            pg_db_connect(db_name)
        connection = pg_conn_map[db_name]
        cursor = pg_cursor_map[db_name]
        sql = obj
        cursor.execute(sql)
        res = []
        if cursor.description:
            for column in cursor.description:
                print(column)
                res.append({
                    "col": column.name,
                    "type": get_type_name_by_oid(column.type_code)
                })
        connection.commit()
        return True, res
    except (Exception, psycopg2.Error) as error:
        connection.rollback()
        return False, [f"Error while executing PostgreSQL query: {error}"]


oracle_conn_map = {}
oracle_cursor_map = {}


def show_oracle_databases():
    try:
        connection = oracledb.connect(
            user=ora_config['oracle_sys_user'],
            password=ora_config['oracle_sys_pwd'],
            host=ora_config['oracle_host'],
            port=ora_config['oracle_port'],
            service_name=ora_config['oracle_sid']
        )
        cursor = connection.cursor()
        cursor.execute(f"SELECT USERNAME FROM DBA_USERS")
        users = [user[0] for user in cursor.fetchall()]
        return users
    except Exception as e:
        print(f"Error while connecting to Oracle: {e}")
        return None


def oracle_db_connect(db_name):
    connection = oracledb.connect(
        user=ora_config['oracle_sys_user'],
        password=ora_config['oracle_sys_pwd'],
        host=ora_config['oracle_host'],
        port=ora_config['oracle_port'],
        service_name=ora_config['oracle_sid']
    )
    cursor = connection.cursor()
    cursor.execute(f"SELECT USERNAME FROM DBA_USERS")
    users = [user[0] for user in cursor.fetchall()]
    if db_name.upper() not in users:
        try:
            print(f'CREATE usr {db_name}')
            cursor.execute(f"CREATE USER {db_name} IDENTIFIED BY {ora_config['usr_default_pwd']}")
            cursor.execute(f"GRANT CONNECT, RESOURCE TO {db_name}")
            cursor.execute(f"GRANT CREATE SESSION TO {db_name}")
            cursor.execute(f"GRANT CREATE TABLE TO {db_name}")
            connection.commit()
        except Exception as e:
            print(f"Error creating user {db_name}: {e}")
            return False, f"Error creating user {db_name}: {e}"
    cursor.close()
    connection.close()

    connection = oracledb.connect(
        user=db_name,
        password=ora_config['usr_default_pwd'],
        host=ora_config['oracle_host'],
        port=ora_config['oracle_port'],
        service_name=ora_config['oracle_sid']
    )
    cursor = connection.cursor()
    oracle_cursor_map[db_name] = cursor
    oracle_conn_map[db_name] = connection
    return connection, cursor


def oracle_drop_db(db_name):
    try:
        db_name = get_db_name('oracle', db_name)
        connection = oracledb.connect(
            user=ora_config['oracle_sys_user'],
            password=ora_config['oracle_sys_pwd'],
            host=ora_config['oracle_host'],
            port=ora_config['oracle_port'],
            service_name=ora_config['oracle_sid']
        )
        cursor = connection.cursor()
        cursor.execute(f"DROP USER {db_name} CASCADE")
        print(f"Drop {db_name} successfully!")
        connection.commit()
    except Exception as e:
        print(f"Error dropping user {db_name}: {e}")
        return False
    cursor.close()
    connection.close()
    return True


def oracle_sql_execute(db_name: str, sql: str, sql_plus_flag=False):
    sql = sql.strip().strip(';')
    db_name = get_db_name('oracle', db_name)
    if not sql_plus_flag:
        if db_name not in oracle_conn_map:
            oracle_db_connect(db_name)
        connection = oracle_conn_map[db_name]
        cursor = oracle_cursor_map[db_name]
        try:
            cursor.execute(sql)
            if sql.startswith('INSERT'):
                connection.commit()
                return True, []
            elif "SELECT" in sql.upper():
                result = cursor.fetchall()
                return True, result
            else:
                connection.commit()
                return True, []
        except Exception as e:
            # Handle the exception, log the error, and optionally raise
            error_message = str(e)
            print(sql)
            print(f"Error executing SQL on database {db_name}: {error_message}")
            return False, error_message
    else:
        sqlplus_path = "sqlplus"
        user = ora_config['oracle_user'],
        password = ora_config['oracle_pwd'],
        server_ip = ora_config['oracle_host'],
        server_port = ora_config['oracle_port']
        db_string = db_name
        sqlplus_command = f"{sqlplus_path} -S {user}/{password}@{server_ip}:{server_port}/{db_string}"
        process = subprocess.run(sqlplus_command, shell=True,
                                 input='alter session set nls_language=american;\n ' + sql + ';',
                                 text=True, capture_output=True)
        return False, f"Error while executing Oracle query: {process.stdout}"


def close_oracle_connect(db_name: str):
    connection = oracle_conn_map[db_name]
    cursor = oracle_cursor_map[db_name]
    if connection:
        cursor.close()
        connection.close()


oracle_conn_local_map = {}
oracle_cursor_local_map = {}


def get_oracle_type(obj: str, db_name, is_table: bool) -> tuple[bool, list]:
    if is_table:
        table_name = obj
        with open(os.path.join(get_proj_root_path(), 'data', db_name, 'schema.json'), 'r') as file:
            schema = json.loads(file.read())
        if table_name not in schema:
            raise ValueError(f"Table {table_name} not found in schema")
        else:
            res = []
            for col in schema[table_name]['cols']:
                col_name = col['col']
                col_type, _, _ = load_col_type(col['type'], col['col_name'], 'oracle')
                res.append({
                    "col": col_name,
                    "type": col_type
                })
            return True, res
    try:
        db_name = get_db_name('oracle', db_name)
        if db_name not in oracle_conn_map:
            oracle_db_connect(db_name)
        connection = oracle_conn_map[db_name]
        cursor = oracle_cursor_map[db_name]
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
