# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: SemanticAttribute$
# @Author: 10379
# @Time: 2025/7/15 0:42


class SemanticAttribute:
    def __init__(self, semantic_info: dict | None):
        if semantic_info is not None:
            if 'CATEGORICAL' in semantic_info:
                self.categorical = True
            else:
                self.categorical = False
            if 'NUMBER' in semantic_info:
                self.number = True
            else:
                self.number = False
            if 'NON_ARITHMETIC' in semantic_info:
                self.non_arithmetical = True
            else:
                self.non_arithmetical = False
            if 'DATE' in semantic_info:
                self.is_date = True
                self.date_format = semantic_info['DATE']
            else:
                self.is_date = False
        else:
            self.categorical = False
            self.number = False
            self.non_arithmetical = False
            self.is_date = False
