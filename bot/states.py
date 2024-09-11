from aiogram.fsm.state import State, StatesGroup

class UserStates(StatesGroup):
    get_xlsx = State()
    get_json = State()