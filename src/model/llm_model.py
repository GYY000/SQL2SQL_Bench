import os
import json
import openai
import requests
import http.client


class LLMModel:
    def __init__(self, model_id, model_conf=None):
        self.model_id = model_id
        self.model_conf = model_conf

        self.model = None
        self.api_key = None
        self.api_base = None
        self.tokenizer = None

        self.trans_func = None

    def load_model(self, api_base=None, api_key=None):
        if "gpt-" in self.model_id:
            self.api_key = api_key
            self.api_base = api_base
            openai.api_base = self.api_base
            self.model = openai.OpenAI(api_key=self.api_key,
                                       base_url=self.api_base)
            self.trans_func = self.openai_gpt

        if "moonshot" in self.model_id:
            self.api_key = api_key
            self.api_base = api_base
            openai.api_base = self.api_base
            self.model = openai.OpenAI(api_key=self.api_key,
                                       base_url=self.api_base)
            self.trans_func = self.openai_moonshot

        if "deepseek" in self.model_id:
            self.api_key = api_key
            self.api_base = api_base
            openai.api_base = self.api_base
            self.model = openai.OpenAI(api_key=self.api_key,
                                       base_url=self.api_base)
            self.trans_func = self.openai_deepseek

        elif self.model_id == "llama3.1":
            self.model = api_base
            self.trans_func = self.llama3

        elif self.model_id == "llama3.2":
            self.model = api_base
            self.trans_func = self.llama3

    def openai_gpt(self, history: [], sys_prompt, user_prompt):
        if sys_prompt is not None:
            messages = [{"role": "system", "content": sys_prompt}]
        else:
            messages = []
        for message in history:
            messages.append(message)
        messages.append({"role": "user", "content": user_prompt})

        completion = self.model.chat.completions.create(
            model=self.model_id,
            messages=messages,
            temperature=1
        )

        return completion.choices[0].message.content

    def openai_moonshot(self, history: [], sys_prompt, user_prompt):
        if sys_prompt is not None:
            messages = [{"role": "system", "content": sys_prompt}]
        else:
            messages = []
        for message in history:
            messages.append(message)
        messages.append({"role": "user", "content": user_prompt})
        completion = self.model.chat.completions.create(
            model=self.model_id,
            messages=messages,
            temperature=1
        )

        return completion.choices[0].message.content

    def openai_deepseek(self, history: [], sys_prompt, user_prompt):
        if sys_prompt is not None:
            messages = [{"role": "system", "content": sys_prompt}]
        else:
            messages = []
        for message in history:
            messages.append(message)
        messages.append({"role": "user", "content": user_prompt})
        completion = self.model.chat.completions.create(
            model=self.model_id,
            messages=messages,
            temperature=0
        )
        return completion.choices[0].message.content

    def openai_gpt_v1(self, history: [], sys_prompt, user_prompt):
        # https://platform.openai.com/docs/models
        if sys_prompt is not None:
            messages = [{"role": "system", "content": sys_prompt}]
        else:
            messages = []
        for message in history:
            messages.append(message)

        messages.append({"role": "user", "content": user_prompt})
        completion = self.model.chat.completions.create(
            model=self.model_id,
            messages=messages,
            **self.model_conf
        )
        # self.model.close()

        return completion.choices[0].message.content

    def llama3(self, history: [], sys_prompt, user_prompt):
        messages = list()
        if sys_prompt is not None:
            messages = [{"role": "system", "content": sys_prompt}]

        for message in history:
            messages.append(message)

        messages.append({"role": "user", "content": user_prompt})

        client = openai.OpenAI(
            base_url=self.model,
            api_key="EMPTY"  # 因为我们不使用认证，所以设为 "EMPTY"
        )
        # 发送一个文本生成请求
        completion = client.chat.completions.create(
            model="llama-3-8B",  # 这个名称是你通过 --served-model-name 设置的
            messages=messages,
            max_tokens=10000,
            temperature=0
        )

        response = completion.choices[0].message.content

        return response

    def llama3_2(self, history: [], sys_prompt, user_prompt):
        messages = list()
        if sys_prompt is not None:
            messages = [{"role": "system", "content": sys_prompt}]
        for message in history:
            messages.append(message)
        messages.append({"role": "user", "content": user_prompt})

        response = requests.request(method="POST", url=self.model,
                                    data=json.dumps({"messages": messages}))

        return response.text
