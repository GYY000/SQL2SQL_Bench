# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: translate_model$
# @Author: 10379
# @Time: 2024/12/13 10:23
import os
import re
import traceback

from model.llm_model import LLMModel
from utils.tools import load_config

config = load_config()
dbg = config['dbg']

gpt_api_base = config['gpt_api_base']
gpt_api_key = config['gpt_api_key']
llama_3_1_api_base = config['llama3.1_api_base']
moonshot_api_base = config['moonshot_api_base']
moonshot_api_key = config['moonshot_api_key']
volcano_api_base = config['volcano_api_base']
volcano_api_key = config['volcano_api_key']
bailian_api_base = config['bailian_api_base']
bailian_api_key = config['bailian_api_key']
print(bailian_api_key)
deepseek_api_base = config['deepseek_api_base']
deepseek_api_key = config['deepseek_api_key']


def init_model(model_id):
    model = None

    if "gpt-" in model_id:  # gpt-4-turbo, gpt-4o, gpt-4o-mini, gpt-4.1
        openai_conf = {"temperature": 0}
        api_base = gpt_api_base
        api_key = gpt_api_key
        model = LLMModel(model_id, openai_conf)
        model.load_model(api_base, api_key)

    elif "moonshot" in model_id:
        api_base = moonshot_api_base
        api_key = moonshot_api_key
        model = LLMModel(model_id)
        model.load_model(api_base, api_key)

    elif model_id == "llama3.1":
        api_base = llama_3_1_api_base
        model = LLMModel(model_id)
        model.load_model(api_base)

    elif model_id == "deepseek-r1-distill-llama-70b":
        api_base = bailian_api_base
        api_key = bailian_api_key
        model = LLMModel(model_id)
        model.load_model(api_base, api_key)

    elif model_id == 'deepseek-r1':
        api_base = volcano_api_base
        api_key = volcano_api_key
        model = LLMModel('deepseek-r1-250528')
        model.load_model(api_base, api_key)

    elif model_id == 'deepseek-v3':
        api_base = volcano_api_base
        api_key = volcano_api_key
        model = LLMModel('deepseek-v3-250324')
        model.load_model(api_base, api_key)

    else:
        raise ValueError(f"Model {model_id} not supported!")
    return model


def parse_llm_answer(model_id, answer_raw, pattern):
    if "gpt-" in model_id:
        answer = answer_raw
    elif "llama" in model_id:
        answer = answer_raw
    elif "moonshot" in model_id:
        answer = answer_raw
    elif 'deepseek' in model_id:
        answer = answer_raw
    else:
        assert False
    try:
        match = re.search(pattern, answer, re.DOTALL)
        if match:
            answer_ori = match.group(1)
            if answer_ori[0] == '"':
                answer_ori = answer_ori[1:]
            if answer_ori[-1] == '"':
                answer_ori = answer_ori[:-1]
            answer_extract = answer_ori.replace('\\\"', '\"').replace('\\n', '\n')
            reasoning = match.group(2).strip('"').replace('\\\"', '\"').replace('\\n', '\n')
            json_content_reflect = {
                "Answer": answer_extract,
                "Reasoning": reasoning
            }
            res = json_content_reflect["Answer"]
        else:
            res = "Answer not returned in the given format!"
        return res
    except Exception as e:
        traceback.print_exc()
        return str(e)

# model = init_model('deepseek-r1-distill-llama-70b')
# print(model.trans_func([], '', 'Hello'))
