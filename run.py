import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.state import default_state
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (Message, FSInputFile)

from bot.texts import *
from bot.states import UserStates

from api import TableParser

import json

TOKEN = '7253845178:AAFpriODIUVv6GF04zegB_5nqnjvt3EYxcE'

storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot = Bot(TOKEN)

@dp.message(CommandStart())
async def command_start_hendler(message: Message, state: FSMContext) -> None:
    await message.answer(start_message)
    message_xlsx = await message.answer(xlsx_message_text)
    user_id = message.from_user.id
    await state.update_data(delete_messege=[message_xlsx.message_id], user_id=user_id, conversation=[])
    await state.set_state(UserStates.get_xlsx)


@dp.message(Command('restart'))
async def commant_restart_hendler(message: Message, state: FSMContext):
    await state.clear()
    await command_start_hendler(message, state)


def get_parametrs(file_path):
    with open(file_path) as json_file:
        data = json.load(json_file)
    return data['tool_description'], data['system_prompt'],\
          data['user_info_cols'], data['user_default_cols'], data['sql_shema']


@dp.message(UserStates.get_xlsx, F.content_type == "document")
async def get_xlxs_file(message: Message, state: FSMContext):
    user_data = await state.get_data()
    message_id = user_data["delete_messege"]
    user_id = user_data["user_id"]
    await bot.delete_messages(chat_id=message.chat.id, message_ids=message_id)

    file_id = message.document.file_id
    xlsx_file_name = f"{str(user_id)}_{message.document.file_name}"
    await state.update_data(xlsx_file_name=f"files/{xlsx_file_name}")

    file = await bot.get_file(file_id)
    file_path = file.file_path
    await bot.download_file(file_path, f"files/{xlsx_file_name}")
    messege_id = message.message_id
    await state.update_data(delete_messege=[messege_id + 1])

    message_json = await message.answer(json_message_text)
    await state.update_data(delete_messege=[message_json.message_id])
    await state.set_state(UserStates.get_json)


@dp.message(StateFilter(UserStates.get_json), F.content_type == "document")
async def get_json_file(message: Message, state: FSMContext):
    global api
    user_data = await state.get_data()
    message_id = user_data["delete_messege"]
    user_id = user_data["user_id"]
    await bot.delete_messages(chat_id=message.chat.id, message_ids=message_id)

    file_id = message.document.file_id
    json_file_name = f"{str(user_id)}_{message.document.file_name}"

    file = await bot.get_file(file_id)
    file_path = file.file_path
    await bot.download_file(file_path, f"files/{json_file_name}")

    tool_description, system_prompt, user_info_cols, user_default_cols, sql_shema = get_parametrs(f"files/{json_file_name}")
    api = TableParser(tool_description, user_data['xlsx_file_name'], system_prompt, user_info_cols, user_default_cols, sql_shema)

    messege_id = message.message_id
    await state.update_data(delete_messege=[messege_id + 1])
    message_conversation = await message.answer(conversation_message_text)
    await state.update_data(conversation_messege=[message_conversation.message_id])

    
@dp.message(F.content_type == "text")
async def conversation(message: Message, state: FSMContext):
    user_data = await state.get_data()

    conversation_list = user_data['conversation']
    conversation_list.append({'role': 'user', 'content' : message.text})
    
    answer = api.get_answer(conversation_list)
    await message.answer(answer)
    conversation_list.append({'role': 'user', 'content' : answer})

    await state.update_data(conversation=conversation_list)


if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))
