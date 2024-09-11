from aiogram.fsm.state import State, StatesGroup

class UserStates(StatesGroup):
    get_system = State()
    get_xlsx = State()
    get_json = State()
