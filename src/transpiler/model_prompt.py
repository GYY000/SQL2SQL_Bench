# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: model_prompt$
# @Author: 10379
# @Time: 2025/3/20 23:37
LLM_REWRITE_SYS_PROMPT = """
## CONTEXT ##
You are a database expert specializing in various SQL dialects, such as **{src_dialect}** and **{tgt_dialect}**, with a focus on accurately translating SQL queries between these dialects.

## OBJECTIVE ##
Your task is to translate the input SQL from **{src_dialect}** into **{tgt_dialect}**, ensuring the following criteria are met:
1. **Grammar Compliance**: The translated SQL must strictly adheres to the grammar and conventions of {tgt_dialect} (e.g., correct usage of keywords and functions);
2. **Functional Consistency**: The translated SQL should produce the same results and maintain the same functionality as the input SQL (e.g., same columns and data types).
3. **Clarity and Efficiency**: The translation should be clear and efficient, avoiding unnecessary complexity while achieving the same outcome.

## Supplementary Material ##
Some supplementary materials are also provided which you should also consider during the translation.
- `Table Definition`: The definition of the table used in the SQL statement is included to provide context and constraints that may influence translation.
{db_param_sys}{type_mapping_sys}

During your translation, please consider the following candidate translation points:
1. **Keywords and Syntax**: Ensure {tgt_dialect} supports all the keywords from the input SQL, and that the syntax is correct;
2. **Built-In Functions**: Verify that any built-in functions from {src_dialect} are available in {tgt_dialect}, paying attention to the argument types and the return types;
3. **Data Types**: Ensure that {tgt_dialect} supports the data types used in the input SQL. Address any expressions that require explicit type conversions;
4. **Incompatibilities**: Resolve any other potential incompatibility issues during translation.

This task is crucial, and your successful translation will be recognized and rewarded. 
Please start by carefully reviewing the input SQL and then proceed with the translation.
"""

LLM_REWRITE_USER_PROMPT = r"""
## INPUT ##
Please translate the input SQL from **{src_dialect}** to **{tgt_dialect}**.
The input SQL is:
```sql
{sql}
```
{db_param_stmt}
{type_mapping_stmt}
Below are table definition about tables used in the {src_dialect} SQL.
<< DDL START >>
```sql
{ddl}
```
<< DDL END >>

## OUTPUT FORMAT ##
Please return your response without any redundant information, strictly adhering to the following format:
```json
{{ 
    "Answer": "The translated SQL",
    "Reasoning": "Your detailed reasoning for the translation steps",
    "Confidence": "The confidence score about your translation (0 - 1)"
}}
```

## OUTPUT ##
"""


LLM_FEED_BACK_SYS_PROMPT = r"""
## CONTEXT ##
You are a database expert specializing in various SQL dialects, such as **{src_dialect}** and **{tgt_dialect}**, with a focus on accurately translating SQL queries between these dialects.

## OBJECTIVE ##
Your task is to translate the input SQL from **{src_dialect}** into **{tgt_dialect}**, ensuring the following criteria are met:
1. **Grammar Compliance**: The translated SQL must strictly adheres to the grammar and conventions of {tgt_dialect} (e.g., correct usage of keywords and functions);
2. **Functional Consistency**: The translated SQL should produce the same results and maintain the same functionality as the input SQL (e.g., same columns and data types).
3. **Clarity and Efficiency**: The translation should be clear and efficient, avoiding unnecessary complexity while achieving the same outcome.

During your translation, please consider the following candidate translation points:
1. **Keywords and Syntax**: Ensure {tgt_dialect} supports all the keywords from the input SQL, and that the syntax is correct;
2. **Built-In Functions**: Verify that any built-in functions from {src_dialect} are available in {tgt_dialect}, paying attention to the argument types and the return types;
3. **Data Types**: Ensure that {tgt_dialect} supports the data types used in the input SQL. Address any expressions that require explicit type conversions;
4. **Incompatibilities**: Resolve any other potential incompatibility issues during translation.
{history_sys}
## Supplementary Material ##
Some supplementary materials are also provided which you should also consider during the translation.
- `Table Definition`: The definition of the table used in the SQL statement is included to provide context and constraints that may influence translation.
{db_param_sys}{type_mapping_sys}

This task is crucial, and your successful translation will be recognized and rewarded. 
Please start by carefully reviewing the input SQL and then proceed with the translation.
"""

LLM_FEED_BACK_USER_PROMPT = r"""
## INPUT ##
Please translate the input SQL from **{src_dialect}** to **{tgt_dialect}**.
The input SQL is:
```sql
{sql}
```
{history_stmt}{db_param_stmt}{type_mapping_stmt}
Below are table definition about tables used in the {src_dialect} SQL.
<< DDL START >>
```sql
{ddl}
```
<< DDL END >>

## OUTPUT FORMAT ##
Please return your response without any redundant information, strictly adhering to the following format:
```json
{{ 
    "Answer": "The translated SQL",
    "Reasoning": "Your detailed reasoning for the translation steps",
    "Confidence": "The confidence score about your translation (0 - 1)"
}}
```

## OUTPUT ##
"""


DB_PARAM_SYS_PROMPT = r"""- `Database Parameters`: Translation-related database parameters and their values are provided. These may affect syntax, function availability, or behavior—pay close attention to their values.
"""
DB_PARAM_USER_PROMPT = r"""
Below are database parameters organized in JSON format that may influence the translation process.
1. `{src_dialect} Database Parameter`: parameters along with their values and descriptions specific to the {src_dialect} database;
2. `{tgt_dialect} Database Parameter`: parameters along with their values and descriptions specific to the {tgt_dialect} database;
Please consider these parameters when translating the SQL snippet:
<< PARAMETER START >>
```json
{parameter}
```
<< PARAMETER END >>
"""

TRAN_HISTORY_SYS_PROMPT = r"""
## TRANSLATION HISTORY ##
You are also provided with previous attempts to translate the SQL statement from **{src_dialect}** to **{tgt_dialect}**. 
Your job is to **not repeat these mistakes**. Instead, use these examples to learn what to avoid during your own translation.

Each historical entry has the form:
- `OriginalSQL`: The original SQL statement in {src_dialect}
- `AttemptedTranslation`: The previous (incorrect) translation into {tgt_dialect}
- `Issue`: Execution feedback of the attempted translation SQL from database
"""
TRAN_HISTORY_USER_PROMPT = r"""
Below are examples of previous incorrect historical entries, including the original SQL statement, the incorrectly translated SQL statement and the execution feedback from the database.
<< TRANSLATION HISTORY START >>
```json
{history}
```
<< TRANSLATION HISTORY END >>
"""

TYPE_MAPPING_SYS_PROMPT = r"""- `Type Mapping`: A mapping between data types in {src_dialect} and {tgt_dialect} is provided to help you understand which type is used in {tgt_dialect} for a given type in {src_dialect}.
"""

TYPE_MAPPING_USER_PROMPT = r"""
Please refer to the type mappings provided below, which indicate how types from {src_dialect} is translated into equivalent types in {tgt_dialect}.
<< TYPE MAPPING START >>
```json
{type_mapping}
```
<< TYPE MAPPING END >>
"""

