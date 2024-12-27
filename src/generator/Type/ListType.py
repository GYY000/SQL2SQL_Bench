# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: ListType$
# @Author: 10379
# @Time: 2024/12/26 19:53
from generator.Type.Type import Type


class ListType(Type):
    def __init__(self, ele_type: Type):
        super().__init__()
        self.ele_type = ele_type

    def __str__(self):
        return f'List[{str(self.ele_type)}]'
