# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: prompt$
# @Author: 10379
# @Time: 2024/12/29 10:36


sys_prompt = """
## CONTEXT ##
You are a database expert specializing in various SQL dialects, such as **{src_dialect}** and **{tgt_dialect}**, with a focus on accurately summarize the transformation points of the functions or operators before and after conversion between dialects.
You will be provided with the following material to assist the translation process:
1. **Dialect Documents**: The information about functions and their usage descriptions to guide your translation.

## OBJECTIVE ##
Your task is to translate the input Function or Operator from **{src_dialect}** to **{tgt_dialect}**, using the provided dialect specifications as needed. 
For each parameter of the MySQL function or Operator, create a placeholder in the format [var_name: varType], where `var_name` is a descriptive name for the variable and `varType` is one of the following types: BOOL, INT, DOUBLE, DATE, TIMESTAMP, TEXT, JSON, POINT. If the type does not match any of these categories, use `other: [type]` and specify the appropriate type.
Ensure you meet the following criteria:
1. **Grammar Compliance**: The translated SQL must strictly adheres to the grammar and conventions of {tgt_dialect} (e.g., correct usage of keywords and functions);
2. **Functional Consistency**: The translated SQL should produce the same results and maintain the same functionality as the input SQL (e.g., same columns and data types).
3. **Clarity and Efficiency**: The translation should be clear and efficient, avoiding unnecessary complexity while achieving the same outcome.

During your translation, please consider the following candidate translation points:
1. **Keywords and Syntax**: Ensure {tgt_dialect} supports all the keywords from the input SQL, and that the syntax is correct;
2. **Built-In Functions**: Verify that any built-in functions from {src_dialect} are available in {tgt_dialect}, paying attention to the argument types and the return types;
3. **Data Types**: Ensure that {tgt_dialect} supports the data types used in the input SQL. Address any expressions that require explicit type conversions;
4. **Incompatibilities**: Resolve any other potential incompatibility issues during translation.

This task is crucial, and your successful translation will be recognized and rewarded. 
Please start by carefully reviewing the input SQL, along with the provided specifications, and then proceed with the translation.
"""

user_prompt = """
## INPUT ##
Please summarize the transformation point of {function_name} from {src_dialect} to {tgt_dialect} with its description.

Below are specifications (might be redundant or irrelevant) for `{function_name}`. 
<< DOCUMENT START >>
{description}
<< DOCUMENT END >>

Note that these specifications may contain redundant or irrelevant information, so please carefully identify what is necessary for the translation.

```json
{{
    "mysql": "function([var_name: varType])",
    "postgres": "equivalent_expression([var_name: varType])"
}}
```

If there is no direct equivalent in PostgreSQL, return `"unknown"` for the `postgres` field.

Example input:
- MySQL function: LAST_DAY
- Description: Returns the last day of the month for the given date.

Example output:
```json
{{
    "mysql": "LAST_DAY([value: DATE])",
    "postgres": "DATE_TRUNC('month', [value: DATE]) + interval '1 month' - interval '1 day'"
}}
```

## OUTPUT FORMAT ##
Please return your response without any redundant information, strictly adhering to the following format:
```json
{{ 
    "{src_dialect}": "The original SQL snippet",
    "{tgt_dialect}": "Your detailed reasoning for the translation steps",
    "Confidence": "The confidence score about your translation (0 - 1)"
}}
```

## OUTPUT ##
"""


sys_prompt_bat = """
### System Prompt  
You are an expert in SQL dialect conversion, specializing in translating {src_dialect} functions and operators into {tgt_dialect} equivalents. Your role is to analyze the given {src_dialect} function or operator, formalize its structure using specific data types, and identify its {tgt_dialect} equivalent if available. You must ensure consistency and accuracy in representing variable names and data types between the two dialects.  

"""

user_prompt_bat = """
I will provide you with a **{src_dialect} function or operator** and its **description**. Your task is to convert it into its {tgt_dialect} equivalent by following these steps:  

1. **Input**:  
   - **{src_dialect} Function or Operator**: `{function_name}`  
   - **Description**: `{description}`  

2. **Formalize {src_dialect} Function**: Represent the given {src_dialect} function or operator in a structured format with variable placeholders indicating their data types. Use the following syntax: `[variable_name: variable_type]`, where `variable_type` is one of:  
   - BOOL  
   - INT  
   - DOUBLE  
   - DATE  
   - TIMESTAMP  
   - TEXT  
   - JSON  
   - POINT  
   If a data type doesn't match these categories, return `other: [type]` and specify the appropriate type.  

3. **Find {tgt_dialect} Equivalent**: Identify the {tgt_dialect} function or operator that matches the behavior of the {src_dialect} function or operator. Use the same variable placeholders to maintain consistency.  

4. **Output the Result**: Provide the output in JSON format. If no equivalent function exists in {tgt_dialect}, specify `"unknown"` for the {tgt_dialect} field.  

### Example Input  
- **{src_dialect} Function**: `LAST_DAY`  
- **Description**: Returns the last day of the month for a given date.  

### Example Output  
```json  
{{  
    "{src_dialect}": "LAST_DAY([value: DATE])",  
    "{tgt_dialect}": "DATE_TRUNC('month', [value: DATE]) + interval '1 month - 1 day'"  
}}  
```  

If no equivalent exists:  
```json  
{{  
    "{src_dialect}": "FUNCTION_NAME([parameter: TYPE])",  
    "{tgt_dialect}": "unknown"  
}}  
```  
"""
