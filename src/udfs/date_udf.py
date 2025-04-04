# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: date_udf$
# @Author: 10379
# @Time: 2025/3/14 18:54

def date_format_udf(format_str: str):
    return ((format_str.replace('YYYY', '%Y').
             replace('yyyy', '%Y').replace('MM', '%m').
             replace('DD', '%d')).replace('dd', '%d').
            replace('MON', '%b').replace('HH24', '%H').replace('MI', '%M').
            replace('SS', '%S').replace('FF', '%f').
            replace('AM', '%p').replace('HH', '%I').replace('RR', '%y'))
