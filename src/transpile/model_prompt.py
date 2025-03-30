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