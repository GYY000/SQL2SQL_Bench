# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: ExecutionEnv$
# @Author: 10379
# @Time: 2025/6/15 12:29
from utils.db_connector import sql_execute, get_mysql_type, get_oracle_type, get_pg_type


class ExecutionEnv:
    def __init__(self, dialect: str, db_name: str):
        self.dialect = dialect
        self.db_name = db_name
        self.db_param = {}

    def add_param(self, dialect: str, param_values: dict):
        if dialect != self.dialect:
            return True
        for db_param, param_value in param_values.items():
            if db_param in self.db_param:
                if self.db_param[db_param] != param_value:
                    return False
        self.db_param.update(param_values)
        return True

    def execute_sql(self, sql: str):
        flag, res = sql_execute(self.dialect, self.db_name, sql, self.db_param, restart_flag=True)
        return flag, res

    def fetch_type(self, object, is_table=False):
        if self.dialect == 'mysql':
            return get_mysql_type(self.db_name, object, is_table, self.db_param)
        elif self.dialect == 'oracle':
            return get_oracle_type(self.db_name, object, is_table, self.db_param)
        elif self.dialect == 'pg':
            return get_pg_type(self.db_name, object, is_table, self.db_param)
        else:
            assert False

    def explain_execute_sql(self, sql: str):
        if self.dialect == 'mysql':
            return self.execute_sql('EXPLAIN ' + sql)
        elif self.dialect == 'oracle':
            return self.execute_sql('EXPLAIN PLAN FOR ' + sql)
        elif self.dialect == 'pg':
            return self.execute_sql('EXPLAIN ' + sql)
        else:
            assert False
