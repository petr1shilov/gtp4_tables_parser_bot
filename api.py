import pandas as pd
import logging
import os
import json
from sqlite3 import connect
from openai import AzureOpenAI


class TableParser():
    def __init__(self,
        system_prompt,
        table_path,
        user_default_cols,
        user_info_cols,
        tool_description,
        sql_shema):
        OPENAI_API_KEY_AZURE = ''
        OPENAI_API_VERSION = '2023-07-01-preview'
        OPENAI_API_BASE = 'https://studio-gpt-eastus-2.openai.azure.com'

        self.client = AzureOpenAI(
            api_key=OPENAI_API_KEY_AZURE,
            api_version=OPENAI_API_VERSION,
            azure_endpoint=OPENAI_API_BASE
        )

        self.kwargs = {
            'ai_model': 'gpt4o',
            'temperature': 0.2,
            'max_tokens': 200,
            'top_p': 0.0,
            'frequency_penalty': 0.0,
            'presence_penalty': -0.5
        }
        self.system_prompt = system_prompt
        self.table_path = table_path
        self.user_default_cols = user_default_cols
        self.user_info_cols = user_info_cols
        self.tool_description = tool_description
        self.sql_shema = sql_shema

    def read_table(self, file_path):
        ext = os.path.splitext(file_path)[-1].lower()
        if ext == '.xml':
            data = pd.read_xml(file_path)
        elif ext == '.xlsx':
            data = pd.read_excel(file_path)
        elif ext == '.csv':
            data = pd.read_csv(file_path)   
        conn = connect(':memory:')
        data.to_sql(name='test_table', con=conn)
        return conn


    def search(self, conn, sql_query):
        return json.dumps(
            {"search_res": pd.read_sql(sql_query, conn).to_string(index=False)},
            ensure_ascii=False
    )

    def get_tools(self):
        default_cols = self.user_default_cols
        info_cols = self.user_info_cols
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_info_from_database",
                    "description": self.tool_description,
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql_query": {
                                "type": "string",
                                "description": (
                                        'Get a sql-query to the table test_table returning table which needs to '
                                        'answer to users question. '
                                        'Do not use SELECT *.\n'
                                        'Add columns from WHERE clause in SELECT. '
                                        'If it does not conflict with the request, use DISTINCT. \n'
                                        f'Always use: {default_cols}. '
                                        f'If you need to describe car, use columns: {info_cols}\n'
                                        'Columns described below:\n'
                                        f'{self.sql_shema}\n'
                                ),
                            }
                        },
                        "required": ["sql_query"],
                    },
                },
            }
        ]
        return tools

    def get_answer(
        self,
        messages,
        print_sql_queries=False
    ):
        conn = self.read_table(self.table_path)

        messages = [
            {
                'role': 'system',
                'content': self.system_prompt
            }
        ] + messages

        response = self.client.chat.completions.create(
            model=self.kwargs['ai_model'],
            messages=messages,
            tools=self.get_tools(),
            tool_choice="auto"
        )

        response_message = response.choices[0].message
        tool_calls = response_message.tool_calls

        if not tool_calls:
            return response_message.content
        else:
            messages.append(response_message)

        for tool_call in tool_calls:
            sql_query = json.loads(tool_call.function.arguments)['sql_query']
            if print_sql_queries:
                print('-------SQL QUERY')
                print(sql_query)
                print('-------')
            logging.info(f'sql_query: {sql_query}')
            messages.append(
                {
                    "tool_call_id": tool_call.id,
                    "role": "tool",
                    "name": "get_info_from_database",
                    "content": self.search(conn, sql_query)
                }
            )
        second_response = self.client.chat.completions.create(
            model=self.kwargs['ai_model'],
            messages=messages,
        )
        return second_response.choices[0].message.content