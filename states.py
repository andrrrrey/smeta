from aiogram.fsm.state import State, StatesGroup


class MainMenu(StatesGroup):
    start = State()


class Calculation(StatesGroup):
    awaiting_pdf = State()
    awaiting_page_numbers = State()
    awaiting_image_prompt = State()
    viewing_calculation = State()
    awaiting_edit_command = State()
    awaiting_delete_confirm = State()


class Admin(StatesGroup):
    menu = State()

    awaiting_price_list = State()
    awaiting_price_list_clear_confirm = State()

    stop_words_menu = State()
    awaiting_stop_word_add = State()

    consumables_menu = State()
    awaiting_consumable_add = State()

    section_titles_menu = State()
    awaiting_section_title_add = State()

    settings_menu = State()
    awaiting_api_key = State()
    awaiting_model = State()
    awaiting_system_prompt = State()
    viewing_system_prompt = State()

    docs_menu = State()
    awaiting_doc_upload = State()
    awaiting_doc_delete_confirm = State()

    users_menu = State()
    user_management = State()