from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton
import hashlib


def main_menu_keyboard(is_admin: bool = False):
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Рассчитать спецификацию", callback_data="calc_start")
    builder.button(text="📚 Мои расчеты", callback_data="calc_history")

    if is_admin:
        builder.button(text="⚙️ Админ-панель", callback_data="admin_menu")

    builder.adjust(1)
    return builder.as_markup()


def admin_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 Загрузить прайс", callback_data="admin_upload_price")
    builder.button(text="👥 Пользователи", callback_data="admin_users_list")
    builder.button(text="🚫 Стоп-слова (Поиск)", callback_data="admin_stopwords")
    builder.button(text="🔴 Расходники (Excel)", callback_data="admin_consumables")
    builder.button(text="📑 Разделы (Заголовки)", callback_data="admin_section_titles")
    builder.button(text="🤖 Настройки ИИ", callback_data="admin_ai_settings")
    builder.button(text="🧠 Упр. базой знаний (RAG)", callback_data="admin_rag_docs")
    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()


def ai_settings_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🔑 API Ключ OpenAI", callback_data="ai_set_key")
    builder.button(text="🧠 Модель (gpt-4o, gpt-4-turbo)", callback_data="ai_set_model")
    builder.button(text="📜 Редактировать Промпт (Текст / .txt)", callback_data="ai_set_prompt")
    builder.button(text="📋 Показать/Скопировать Промпт", callback_data="ai_show_full_prompt")
    builder.button(text="📥 Скачать Промпт (.txt)", callback_data="ai_download_prompt")
    builder.button(text="⬅️ Назад", callback_data="admin_menu")
    builder.adjust(1, 1, 1, 2, 1)
    return builder.as_markup()


def stopwords_menu_keyboard(stopwords: list, page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить стоп-слово", callback_data=f"stopword_add_page_{page}")

    for word in stopwords:
        builder.button(text=f"❌ {word.word}", callback_data=f"stopword_delete_{word.id}_page_{page}")

    nav_buttons = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"stopword_page_{page - 1}")
        )
    if total_pages > 1:
        nav_buttons.append(
            InlineKeyboardButton(text=f"{page} / {total_pages}", callback_data="stopword_page_info")
        )
    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперед ➡️", callback_data=f"stopword_page_{page + 1}")
        )

    if nav_buttons:
        builder.row(*nav_buttons)

    builder.button(text="⬅️ Назад", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()


def rag_docs_menu_keyboard(docs: list):
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Загрузить (.txt, .pdf, .xls)", callback_data="rag_upload")

    for doc_name in docs:
        display_name = (doc_name[:25] + '...') if len(doc_name) > 28 else doc_name
        doc_hash = hashlib.md5(doc_name.encode()).hexdigest()
        builder.button(text=f"❌ {display_name}", callback_data=f"rag_confirm_delete_{doc_hash}")

    builder.button(text="⬅️ Назад", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()


def confirm_delete_keyboard(doc_hash: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"rag_delete_confirm_{doc_hash}")
    builder.button(text="⬅️ Отмена", callback_data="admin_rag_docs")
    builder.adjust(1)
    return builder.as_markup()


def calculation_view_keyboard(calc_id: int, status: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Редактировать", callback_data=f"calc_edit_{calc_id}")
    builder.button(text="📥 Скачать Excel", callback_data=f"calc_download_{calc_id}")

    if status == "approved":
        builder.button(text="✅ Утвержден", callback_data="calc_approved_info")
    else:
        builder.button(text="✅ Утвердить (дообучить)", callback_data=f"calc_approve_{calc_id}")

    builder.button(text="❌ Удалить", callback_data=f"calc_delete_{calc_id}")
    builder.button(text="⬅️ К списку расчетов", callback_data="calc_history")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def calculation_edit_keyboard(calc_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="💬 Команда (текст/голос)", callback_data=f"calc_edit_cmd_{calc_id}")
    builder.button(text="📥 Загрузить Excel", callback_data=f"calc_edit_excel_{calc_id}")
    builder.button(text="⬅️ Назад", callback_data=f"calc_view_{calc_id}")
    builder.adjust(1)
    return builder.as_markup()


def confirm_calc_delete_keyboard(calc_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить расчет", callback_data=f"calc_delete_confirm_{calc_id}")
    builder.button(text="⬅️ Отмена", callback_data=f"calc_view_{calc_id}")
    builder.adjust(1)
    return builder.as_markup()


def back_button(callback_data: str):
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=callback_data)
    return builder.as_markup()


def ai_models_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🤖 gpt-4o (OpenAI Vision)", callback_data="ai_set_model_gpt-4o")
    builder.button(text="🤖 gpt-4o-mini (OpenAI Vision)", callback_data="ai_set_model_gpt-4o-mini")
    builder.button(text="🤖 gpt-4-turbo (OpenAI Vision)", callback_data="ai_set_model_gpt-4-turbo")
    builder.button(text="⌨️ Ввести вручную", callback_data="ai_set_model_custom")
    builder.button(text="⬅️ Назад", callback_data="admin_ai_settings")
    builder.adjust(1)
    return builder.as_markup()


def price_list_menu_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 Скачать прайс-лист", callback_data="admin_download_price")
    builder.button(text="🗑️ Очистить прайс-лист", callback_data="price_clear_confirm")
    builder.button(text="⬅️ Назад", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()


def confirm_clear_price_list_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, очистить", callback_data="price_clear_execute")
    builder.button(text="⬅️ Отмена", callback_data="admin_upload_price")
    builder.adjust(1)
    return builder.as_markup()


def calc_history_keyboard(page: int, total_pages: int):
    builder = InlineKeyboardBuilder()

    nav_buttons = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(text="⬅️ Пред.", callback_data=f"calc_history_page_{page - 1}")
        )

    if total_pages > 1:
        nav_buttons.append(
            InlineKeyboardButton(text=f"{page} / {total_pages}", callback_data="calc_page_info")
        )

    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(text="След. ➡️", callback_data=f"calc_history_page_{page + 1}")
        )

    if nav_buttons:
        builder.row(*nav_buttons)

    builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()


def admin_users_list_keyboard(users: list, page: int, total_pages: int):
    builder = InlineKeyboardBuilder()

    for user in users:
        status_emoji = "🚫" if user.is_blocked else "✅"
        user_display = f"{status_emoji} {user.first_name}"
        if user.username:
            user_display += f" (@{user.username})"
        builder.button(text=user_display, callback_data=f"admin_manage_user_{user.user_id}")

    nav_buttons = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_users_page_{page - 1}")
        )
    if total_pages > 1:
        nav_buttons.append(
            InlineKeyboardButton(text=f"{page} / {total_pages}", callback_data="admin_users_page_info")
        )
    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперед ➡️", callback_data=f"admin_users_page_{page + 1}")
        )

    if nav_buttons:
        builder.row(*nav_buttons)

    builder.button(text="⬅️ Назад", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()


def admin_user_manage_keyboard(user_id: int, is_blocked: bool):
    builder = InlineKeyboardBuilder()
    if is_blocked:
        builder.button(text="✅ Разблокировать", callback_data=f"admin_unblock_user_{user_id}")
    else:
        builder.button(text="🚫 Заблокировать", callback_data=f"admin_block_user_{user_id}")

    builder.button(text="⬅️ Назад к списку", callback_data="admin_users_list")
    builder.adjust(1)
    return builder.as_markup()


def consumables_menu_keyboard(words: list, page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить слово", callback_data=f"consumable_add_page_{page}")

    for word in words:
        builder.button(text=f"❌ {word.word}", callback_data=f"consumable_delete_{word.id}_page_{page}")

    nav_buttons = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"consumable_page_{page - 1}")
        )
    if total_pages > 1:
        nav_buttons.append(
            InlineKeyboardButton(text=f"{page} / {total_pages}", callback_data="consumable_page_info")
        )
    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперед ➡️", callback_data=f"consumable_page_{page + 1}")
        )

    if nav_buttons:
        builder.row(*nav_buttons)

    builder.button(text="⬅️ Назад", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()


def section_titles_menu_keyboard(titles: list, page: int, total_pages: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить раздел", callback_data=f"section_title_add_page_{page}")

    for title in titles:
        builder.button(text=f"❌ {title.title}", callback_data=f"section_title_delete_{title.id}_page_{page}")

    nav_buttons = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"section_title_page_{page - 1}")
        )
    if total_pages > 1:
        nav_buttons.append(
            InlineKeyboardButton(text=f"{page} / {total_pages}", callback_data="section_title_page_info")
        )
    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперед ➡️", callback_data=f"section_title_page_{page + 1}")
        )

    if nav_buttons:
        builder.row(*nav_buttons)

    builder.button(text="⬅️ Назад", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()