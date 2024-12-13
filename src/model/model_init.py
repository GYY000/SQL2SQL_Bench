# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: translate_model$
# @Author: 10379
# @Time: 2024/12/13 10:23
import os

from model.llm_model import LLMModel
from util.tools import load_config

config = load_config()
dbg = config['dbg']

gpt_api_base = config['gpt_api_base']
gpt_api_key = config['gpt_api_key']
llama_3_1_api_base = config['llama3.1_api_base']
llama_3_2_api_base = config['llama3.2_api_base']


def init_model(model_id):
    model = None

    if "gpt-" in model_id:  # gpt-4-turbo, gpt-4o, gpt-4o-mini
        openai_conf = {"temperature": 0}
        api_base = gpt_api_base
        api_key = gpt_api_key

        model = LLMModel(model_id, openai_conf)
        model.load_model(api_base, api_key)

    elif model_id == "llama3.1":
        api_base = llama_3_1_api_base
        model = LLMModel(model_id)
        model.load_model(api_base)

    elif model_id == "llama3.2":
        api_base = llama_3_2_api_base
        model = LLMModel(model_id)
        model.load_model(api_base)

    return model
