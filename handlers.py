import os
import io
import asyncio
import pandas as pd
import pytz
import base64
import pdfplumber
import html
from decimal import Decimal
import hashlib
import re
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, FSInputFile, BufferedInputFile, Voice
from aiogram.filters import CommandStart, StateFilter, Command
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import func
from sqlalchemy import delete
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from typing import Optional, List

from config import load_config
from db import async_session_factory, User, Calculation, CalculationItem, PriceListItem, StopWord, ConsumableWord, BotSettings, SectionTitle
from states import MainMenu, Calculation as CalcState, Admin
from keyboards import (
    main_menu_keyboard, admin_menu_keyboard, ai_settings_keyboard,
    stopwords_menu_keyboard, consumables_menu_keyboard, rag_docs_menu_keyboard, back_button,
    calculation_view_keyboard, calculation_edit_keyboard,
    confirm_delete_keyboard, confirm_calc_delete_keyboard,
    price_list_menu_keyboard, confirm_clear_price_list_keyboard,
    ai_models_keyboard, calc_history_keyboard,
    admin_users_list_keyboard, admin_user_manage_keyboard,
    section_titles_menu_keyboard
)
from utils import (
    delete_user_message, delete_previous_menu, set_menu_message,
    send_temp_notification, add_message_to_history, is_owner,
    extract_specification_tables, create_calculation_excel,
    parse_excel_for_update, create_pricelist_excel,
    parse_spec_excel_for_creation
)
from services import price_logic_instance, ai_service_instance, vector_db_instance


def md_esc(text: str) -> str:
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in str(text))

router = Router()
config = load_config()
PAGE_SIZE = 10


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)

    async with async_session_factory() as session:
        user = await session.get(User, message.from_user.id)
        if not user:
            user = User(
                user_id=message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                is_blocked=False
            )
            session.add(user)
            await session.commit()

        if user.is_blocked:
            await message.answer("⛔ Доступ к боту заблокирован администратором.")
            return

    is_admin = message.from_user.id in config.bot.owner_ids

    text = f"Здравствуйте, {message.from_user.first_name}! Я бот для расчета смет."
    menu_msg = await message.answer(text, reply_markup=main_menu_keyboard(is_admin=is_admin))
    await set_menu_message(menu_msg, state)
    await state.set_state(MainMenu.start)


@router.message(Command("help"))
async def cmd_help(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)

    text = """
<b>Инструкция по работе с ботом</b>

<b>1. Как работает расчет (Для Пользователя)</b>

1.  <b>Начало:</b> Нажми "📊 Рассчитать спецификацию" в главном меню.
2.  <b>Загрузка PDF:</b> Бот попросит PDF. Отправь ему проект.
3.  <b>Парсинг:</b> Бот откроет PDF, найдет <i>в конце</i> документа таблицы спецификации (он ищет заголовки <code>'наименование'</code>, <code>'кол'</code>, <code>'ед'</code>).
4.  <b>Поиск цен (Логика):</b>
    • Сначала бот ищет позицию в твоем прайс-листе (который загружен в админке).
    • Если не нашел, он ищет в Базе Знаний (RAG) (файлы, загруженные в админке).
    • Если не нашел, он ищет в интернете (OpenAI/ChatGPT).
    • <b>Важно:</b> Если позиция — расходник (из <code>'Стоп-слов'</code>), бот НЕ БУДЕТ искать ее в интернете, чтобы не тратить деньги.
5.  <b>Результат:</b> Ты получишь Excel-файл.
    • <b>Желтый:</b> Цена найдена в интернете (OpenAI).
    • <b>Красный:</b> Цена не найдена нигде.
    • <b>Белый:</b> Цена взята из твоего прайс-листа.

<b>2. Как редактировать расчет</b>

У тебя два способа:

<b>Способ 1: Команды (Текст или Голос)</b>
Находясь в меню "Редактирование", просто отправь команду:

• <code>"увеличь все на 10%"</code> (или "повысь на 10")
• <code>"уменьши все на 5%"</code> (или "понизь на 5")
• <code>"подогнать сумму к 500000"</code> (или "сделай итого 500к")
• <code>"увеличь на 10% кроме 5, 23, 40"</code> (номера строк)

<b>Способ 2: Excel</b>
1.  Скачай Excel-файл (<code>📥 Скачать Excel</code>).
2.  Открой, исправь цифры в колонках <code>"Кол-во"</code> или <code>"Цена за ед."</code>.
3.  Отправь исправленный Excel-файл <i>обратно</i> боту (прямо в меню редактирования). Бот сам найдет изменения по названиям позиций.

<b>3. Дообучение (Утверждение)</b>

1.  Когда расчет готов, нажми "✅ Утвердить (дообучить)".
2.  Бот возьмет <i>все</i> цены, которые ты исправил (<code>'manual'</code>) или которые нашел ИИ (<code>'internet'</code>), и <b>добавит их в твой основной прайс-лист</b>.
3.  В следующий раз он уже будет знать эти цены и не пойдет в интернет.
4.  Расчет станет "Утвержденным" (✅), его больше нельзя редактировать.

<b>4. Админка </b>

<i>Это для настройки. Доступ только у <code>owner_id</code> из <code>config.ini</code>.</i>

• <b>Загрузка прайса (CSV):</b>
    Сюда кидай свой основной прайс-лист в формате <code>.csv</code>. Обязательные колонки: <code>Наименование</code> (или похожее) и <code>Цена</code> (или похожее).

• <b>Упр. стоп-словами:</b>
    Добавь сюда расходники (<code>кабель</code>, <code>гофра</code>, <code>лоток</code>, <code>дюбель</code> и т.д.). Бот <b>не будет</b> искать эти слова в интернете (OpenAI). Поиск по <i>твоему</i> прайсу (CSV) будет работать.

• <b>Настройки ИИ:</b>
    • <b>API Ключ:</b> Самое важное. Иди на <code>platform.openai.com</code>. Создай API ключ. <b>ОБЯЗАТЕЛЬНО</b> зайди в <code>Billing</code> (Оплата) и привяжи карту. Без этого RAG (векторная база) и поиск цен работать не будут.
    • <b>Модель:</b> <code>gpt-4o</code> (по умолчанию).
    • <b>Системный промпт:</b> Это главная инструкция для ИИ, <i>как</i> ему искать цены. Можешь отредактировать ее (я дал хороший вариант).

• <b>Упр. базой знаний (RAG):</b>
    Это твоя "векторная память". Сюда кидай старые сметы (PDF, TXT), прайсы конкурентов, инструкции. Когда бот ищет цену, он <i>сначала</i> смотрит здесь, и только потом в интернете. Это делает поиск точнее и дешевле.
    """
    menu_msg = await message.answer(text, reply_markup=back_button("back_to_main_menu"), parse_mode="HTML")
    await set_menu_message(menu_msg, state)


@router.callback_query(F.data == "back_to_main_menu")
async def back_to_main_menu(callback: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    pdf_path = data.get("current_pdf_path")
    if pdf_path and os.path.exists(pdf_path):
        try:
            os.remove(pdf_path)
        except OSError as e:
            print(f"Error deleting PDF on 'back_to_main_menu': {e}")

    await callback.message.delete()
    await state.clear()

    async with async_session_factory() as session:
        user = await session.get(User, callback.from_user.id)

    is_admin = callback.from_user.id in config.bot.owner_ids

    text = f"Здравствуйте, {user.first_name}! Я бот для расчета смет."
    menu_msg = await callback.message.answer(text, reply_markup=main_menu_keyboard(is_admin=is_admin))
    await set_menu_message(menu_msg, state)
    await state.set_state(MainMenu.start)


@router.message(
    F.text & ~F.text.startswith('/'),
    StateFilter(
        None,
        MainMenu.start,
        CalcState.viewing_calculation,
        CalcState.awaiting_delete_confirm,
        Admin.menu,
        Admin.stop_words_menu,
        Admin.settings_menu,
        Admin.docs_menu,
        Admin.awaiting_doc_delete_confirm
    )
)
async def delete_stray_text_messages(message: Message, state: FSMContext):
    await delete_user_message(message)


def parse_page_numbers(text: str, max_pages: int) -> (Optional[List[int]], Optional[str]):
    indices = set()
    text = text.lower().strip().replace(" ", "")

    if text == "авто":
        return None, None

    parts = text.split(',')
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            page_num = int(part)
            if 1 <= page_num <= max_pages:
                indices.add(page_num - 1)
            else:
                return None, f"Ошибка: Номер страницы {page_num} вне диапазона (1-{max_pages})."
        elif '-' in part:
            range_parts = part.split('-')
            if len(range_parts) != 2 or not range_parts[0].isdigit() or not range_parts[1].isdigit():
                return None, f"Ошибка: Неверный диапазон '{part}'. Используйте '10-12'."

            start = int(range_parts[0])
            end = int(range_parts[1])

            if start > end:
                start, end = end, start

            if not (1 <= start <= max_pages and 1 <= end <= max_pages):
                return None, f"Ошибка: Диапазон '{part}' выходит за пределы (1-{max_pages})."

            for i in range(start, end + 1):
                indices.add(i - 1)
        else:
            return None, f"Ошибка: Не распознан ввод '{part}'. Введите числа, '10-12' или 'авто'."

    if not indices:
        return None, "Ошибка: Не найдено ни одного номера страницы."

    return sorted(list(indices)), None


@router.message(CalcState.awaiting_page_numbers, F.text)
async def process_page_numbers(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)

    data = await state.get_data()
    pdf_path = data.get("current_pdf_path")
    pdf_filename = data.get("current_pdf_filename")
    total_pages = data.get("current_pdf_total_pages", 0)

    if not pdf_path or not os.path.exists(pdf_path):
        await delete_previous_menu(state, bot, message.chat.id)
        await send_temp_notification(message, "Ошибка: PDF-файл не найден. Начните заново.", delay=7)
        await state.clear()

        async with async_session_factory() as session:
            user = await session.get(User, message.from_user.id)
        text = f"Здравствуйте, {user.first_name}! Я бот для расчета смет."
        menu_msg = await message.answer(text, reply_markup=main_menu_keyboard())
        await set_menu_message(menu_msg, state)
        await state.set_state(MainMenu.start)
        return

    page_indices, error = parse_page_numbers(message.text, total_pages)

    if error:
        await send_temp_notification(message, error, delay=7)
        return

    await delete_previous_menu(state, bot, message.chat.id)
    processing_msg = await message.answer(
        f"Начинаю обработку... (Страницы: {message.text})" if page_indices else "Начинаю обработку... (Авто-поиск)"
    )

    spec_items = []
    calculation_created = False
    try:
        spec_items = await extract_specification_tables(
            pdf_path,
            processing_msg,
            page_indices
        )

        if not spec_items:
            menu_msg = await processing_msg.edit_text(
                "Не смог найти таблицу спецификации на указанных страницах.\n\n"
                "Попробуйте ввести другие номера (<code>10-12</code>) или <code>авто</code>.",
                reply_markup=back_button("back_to_main_menu"),
                parse_mode="HTML"
            )
            await set_menu_message(menu_msg, state)
            await state.set_state(CalcState.awaiting_page_numbers)
            return

        await processing_msg.edit_text("Спецификация найдена. Ищу цены... 💰")

        async with async_session_factory() as session:
            calculation = await price_logic_instance.process_specification(
                session,
                message.from_user.id,
                spec_items,
                pdf_filename,
                processing_msg,
                bot,
                config
            )
            await session.refresh(calculation, ["items"])

        calculation_created = True
        await processing_msg.delete()
        await send_calculation_view(message, state, calculation)

    except (ValueError, Exception) as e:
        error_str = str(e).lower()
        if "billing" in error_str or "quota" in error_str or "429" in error_str:
            try:
                await processing_msg.delete()
            except TelegramBadRequest:
                pass

            await message.answer(
                f"🚨 <b>Ошибка OPENAI:</b>\n\n{e}\n\n"
                "Проверьте баланс (Billing) или лимиты (Quota) на platform.openai.com.\n"
                "Бот не может работать без оплаты API.",
                parse_mode="HTML"
            )

            await state.clear()
            async with async_session_factory() as session:
                user = await session.get(User, message.from_user.id)
            text = f"Здравствуйте, {user.first_name}! Я бот для расчета смет."
            menu_msg = await message.answer(text, reply_markup=main_menu_keyboard())
            await set_menu_message(menu_msg, state)
            await state.set_state(MainMenu.start)
            return

        try:
            await processing_msg.edit_text(
                f"Ошибка: {e}\nПопробуйте другой файл или страницы.",
                reply_markup=back_button("back_to_main_menu")
            )
        except TelegramBadRequest:
            pass

        await set_menu_message(processing_msg, state)
        await state.set_state(CalcState.awaiting_page_numbers)
        return

    finally:
        if calculation_created:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
            await state.update_data(current_pdf_path=None, current_pdf_filename=None, current_pdf_total_pages=0)


async def send_calculation_view(message_or_callback: Message | CallbackQuery, state: FSMContext,
                                calculation: Calculation):
    items = calculation.items

    msk_tz = pytz.timezone('Europe/Moscow')

    date_str = "???"
    if calculation.created_at:
        utc_dt = calculation.created_at.replace(tzinfo=pytz.utc)
        msk_dt = utc_dt.astimezone(msk_tz)
        date_str = html.escape(msk_dt.strftime('%d-%m-%Y %H:%M'))

    status_icon = " (✅ Утвержден)" if calculation.status == "approved" else ""
    total_cost_str = html.escape(f"{calculation.total_cost:,.2f}")

    file_name_str = ""
    if calculation.pdf_filename:
        file_name_str = f"Файл: <i>{html.escape(calculation.pdf_filename)}</i>\n"

    text = f"<b>Расчет №{calculation.id}</b> от {date_str}{status_icon}\n"
    text += file_name_str
    text += f"<b>Общая сумма: {total_cost_str} руб.</b>\n\n"

    text += html.escape("Полный список доступен в Excel файле (кнопка ниже).")

    if isinstance(message_or_callback, Message):
        target_message = message_or_callback
    else:
        target_message = message_or_callback.message

    if isinstance(message_or_callback, CallbackQuery):
        try:
            await target_message.delete()
        except TelegramBadRequest:
            pass

    menu_msg = await target_message.answer(
        text,
        reply_markup=calculation_view_keyboard(calculation.id, calculation.status),
        parse_mode="HTML"
    )
    await set_menu_message(menu_msg, state)
    await state.set_state(CalcState.viewing_calculation)


async def show_calc_history_page(event: Message | CallbackQuery, state: FSMContext, bot: Bot, page: int = 1):
    if isinstance(event, Message):
        target_message = event
    else:
        target_message = event.message

    await delete_previous_menu(state, bot, target_message.chat.id)

    msk_tz = pytz.timezone('Europe/Moscow')

    async with async_session_factory() as session:
        total_count_result = await session.execute(
            select(func.count(Calculation.id)).where(Calculation.user_id == event.from_user.id)
        )
        total_count = total_count_result.scalar_one()

        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        if total_pages == 0:
            total_pages = 1

        if page < 1:
            page = 1
        elif page > total_pages:
            page = total_pages

        offset = (page - 1) * PAGE_SIZE

        result = await session.execute(
            select(Calculation)
            .where(Calculation.user_id == event.from_user.id)
            .order_by(Calculation.created_at.desc())
            .offset(offset)
            .limit(PAGE_SIZE)
        )
        calculations = result.scalars().all()

    if not calculations:
        text = "У вас пока нет сохраненных расчетов."
    else:
        text = f"Ваши расчеты (Страница {page}/{total_pages}):\n\n"
        for calc in calculations:
            status_icon = "✅" if calc.status == "approved" else "🔹"
            price_formatted = html.escape(f"{calc.total_cost:,.2f}")

            date_formatted = "???"
            if calc.created_at:
                utc_dt = calc.created_at.replace(tzinfo=pytz.utc)
                msk_dt = utc_dt.astimezone(msk_tz)
                date_formatted = html.escape(msk_dt.strftime('%d-%m-%Y'))

            file_name = html.escape(calc.pdf_filename or "Без имени")
            file_name_short = (file_name[:20] + '..') if len(file_name) > 22 else file_name

            text += f"{status_icon} /view_calc_{calc.id} от {date_formatted}\n"
            text += f"     <i>{file_name_short}</i> - <b>{price_formatted} руб.</b>\n"

    menu_msg = None
    reply_markup = calc_history_keyboard(page, total_pages)

    if isinstance(event, Message):
        menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        try:
            await target_message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
            menu_msg = target_message
        except TelegramBadRequest:
            menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")

    await set_menu_message(menu_msg, state)
    await state.set_state(MainMenu.start)


@router.callback_query(F.data.startswith("calc_history_page_"))
async def handle_calc_history_page(callback: CallbackQuery, state: FSMContext, bot: Bot):
    try:
        page = int(callback.data.split("_")[3])
    except (IndexError, ValueError):
        page = 1
    await show_calc_history_page(callback, state, bot, page)
    await callback.answer()


@router.callback_query(F.data == "calc_page_info")
async def handle_calc_page_info(callback: CallbackQuery):
    await callback.answer("Вы на этой странице.")


@router.callback_query(F.data == "calc_start", MainMenu.start)
async def start_calculation(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.message.delete()
    text = (
        "Пожалуйста, загрузите исходные данные:\n\n"
        "1. 📄 <b>PDF-файл</b> (бот попытается найти таблицы)\n"
        "2. 🖼️ <b>Изображение</b> (JPG/PNG) со спецификацией\n"
        "3. 📊 <b>Excel-файл</b> (.xlsx) с колонками <i>Наименование</i>, <i>Кол-во</i>"
    )
    menu_msg = await callback.message.answer(text, reply_markup=back_button("back_to_main_menu"), parse_mode="HTML")
    await set_menu_message(menu_msg, state)
    await state.set_state(CalcState.awaiting_pdf)


@router.message(CalcState.awaiting_pdf, F.document.mime_type == "application/pdf")
async def process_pdf(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    await delete_previous_menu(state, bot, message.chat.id)

    data = await state.get_data()
    old_pdf_path = data.get("current_pdf_path")
    if old_pdf_path and os.path.exists(old_pdf_path):
        try:
            os.remove(old_pdf_path)
        except OSError as e:
            print(f"Error deleting old PDF: {e}")

    temp_msg = await message.answer("Получил PDF. Скачиваю... ⏳")

    pdf_path = f"temp_{message.document.file_id}.pdf"
    pdf_filename = message.document.file_name or f"{message.document.file_unique_id}.pdf"

    try:
        await bot.download(message.document, destination=pdf_path)

        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        if not pdf_bytes:
            raise ValueError("PDF-файл пуст (0 bytes).")

        await temp_msg.edit_text("Парсинг PDF через OpenAI... ⏳")

        spec_items = await ai_service_instance.parse_specification_from_pdf_bytes(
            pdf_bytes=pdf_bytes,
            filename=pdf_filename,
            user_hint=""
        )

        if not spec_items:
            menu_msg = await temp_msg.edit_text(
                "Не удалось распознать спецификацию в PDF.",
                reply_markup=back_button("back_to_main_menu")
            )
            await set_menu_message(menu_msg, state)
            await state.set_state(CalcState.awaiting_pdf)
            return

        await temp_msg.edit_text("Рассчет сметы... ⏳")

        async with async_session_factory() as session:
            calculation = await price_logic_instance.process_specification(
                session,
                message.from_user.id,
                spec_items,
                pdf_filename,
                temp_msg,
                bot,
                config
            )
            await session.refresh(calculation, ["items"])

        await temp_msg.delete()
        await send_calculation_view(message, state, calculation)

    except Exception as e:
        err = str(e).lower()
        if "billing" in err or "quota" in err or "429" in err:
            try:
                await temp_msg.delete()
            except TelegramBadRequest:
                pass

            await message.answer(
                f"🚨 <b>Ошибка OPENAI:</b>\n\nПроверьте Billing (оплату) или Quota (лимиты)!\nОшибка: {e}",
                parse_mode="HTML"
            )
            await state.clear()
            return

        menu_msg = await temp_msg.edit_text(
            f"Ошибка при обработке PDF: {e}",
            reply_markup=back_button("back_to_main_menu")
        )
        await set_menu_message(menu_msg, state)
        await state.set_state(CalcState.awaiting_pdf)

    finally:
        if os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except OSError:
                pass

        await state.update_data(
            current_pdf_path=None,
            current_pdf_filename=None,
            current_pdf_total_pages=0
        )


@router.message(CalcState.awaiting_pdf, F.document.mime_type.in_([
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
]))
async def process_spec_excel(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    await delete_previous_menu(state, bot, message.chat.id)

    processing_msg = await message.answer("Получил Excel. Читаю данные... 📑")

    excel_path = f"temp_spec_{message.document.file_id}.xlsx"
    excel_filename = message.document.file_name or f"{message.document.file_unique_id}.xlsx"

    await bot.download(message.document, destination=excel_path)

    spec_items = []
    try:
        spec_items = parse_spec_excel_for_creation(excel_path)

        if not spec_items:
            await processing_msg.edit_text(
                "Не смог найти данные в Excel. Убедитесь, что файл не пуст и колонки названы правильно.",
                reply_markup=back_button("back_to_main_menu")
            )
            await state.set_state(CalcState.awaiting_pdf)
            return

        await processing_msg.edit_text("Спецификация из Excel получена. Ищу цены... 💰")

        async with async_session_factory() as session:
            calculation = await price_logic_instance.process_specification(
                session,
                message.from_user.id,
                spec_items,
                excel_filename,
                processing_msg,
                bot,
                config
            )
            await session.refresh(calculation, ["items"])

        await processing_msg.delete()
        await send_calculation_view(message, state, calculation)

    except ValueError as e:
        await processing_msg.edit_text(
            f"{e}",
            reply_markup=back_button("back_to_main_menu")
        )
        await state.set_state(CalcState.awaiting_pdf)
        return
    except Exception as e:
        await processing_msg.edit_text(
            f"Произошла неизвестная ошибка при чтении Excel: {e}",
            reply_markup=back_button("back_to_main_menu")
        )
        await state.set_state(CalcState.awaiting_pdf)
        return
    finally:
        if os.path.exists(excel_path):
            os.remove(excel_path)


@router.message(CalcState.awaiting_pdf, F.photo)
async def process_image_upload(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    await delete_previous_menu(state, bot, message.chat.id)

    photo_file = message.photo[-1]
    file_path = f"temp_img_{photo_file.file_unique_id}.jpg"

    await bot.download(photo_file, destination=file_path)

    await state.update_data(current_image_path=file_path)

    menu_msg = await message.answer(
        "Изображение получено. 🖼️\n\n"
        "Напишите подсказку (промпт) для ИИ: что именно нужно извлечь? "
        "Есть ли особенности в таблице?\n\n"
        "<i>Например: 'Возьми данные только из таблицы Оборудование, колонку Код считай артикулом.'</i>\n"
        "Или отправьте 'ок', если подсказка не нужна.",
        reply_markup=back_button("back_to_main_menu"),
        parse_mode="HTML"
    )

    await set_menu_message(menu_msg, state)
    await state.set_state(CalcState.awaiting_image_prompt)


@router.message(CalcState.awaiting_image_prompt, F.text)
async def process_image_prompt_and_calculate(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    await delete_previous_menu(state, bot, message.chat.id)

    data = await state.get_data()
    image_path = data.get("current_image_path")
    user_prompt = message.text.strip()
    if user_prompt.lower() in ["ок", "ok", "-", "нет"]:
        user_prompt = ""

    if not image_path or not os.path.exists(image_path):
        await send_temp_notification(message, "Ошибка: Изображение потеряно. Начните заново.")
        await state.set_state(CalcState.awaiting_pdf)
        return

    processing_msg = await message.answer("Анализирую изображение... 🤖")

    try:
        with open(image_path, "rb") as image_file:
            base64_image = base64.b64encode(image_file.read()).decode('utf-8')

        spec_items = await ai_service_instance.parse_specification_from_image(base64_image, user_hint=user_prompt)

        if not spec_items:
            await processing_msg.edit_text(
                "Не удалось извлечь данные из изображения. Попробуйте сделать более четкое фото или измените промпт.",
                reply_markup=back_button("back_to_main_menu")
            )
            await state.set_state(CalcState.awaiting_pdf)
            return

        await processing_msg.edit_text("Данные извлечены. Ищу цены... 💰")

        async with async_session_factory() as session:
            calculation = await price_logic_instance.process_specification(
                session,
                message.from_user.id,
                spec_items,
                "image_upload.jpg",
                processing_msg,
                bot,
                config
            )
            await session.refresh(calculation, ["items"])

        await processing_msg.delete()
        await send_calculation_view(message, state, calculation)

    except Exception as e:
        await processing_msg.edit_text(
            f"Произошла ошибка при обработке изображения: {e}",
            reply_markup=back_button("back_to_main_menu")
        )
        await state.set_state(CalcState.awaiting_pdf)

    finally:
        if os.path.exists(image_path):
            os.remove(image_path)
        await state.update_data(current_image_path=None)


@router.message(CalcState.awaiting_pdf, F.text & ~F.text.startswith('/'))
async def wrong_pdf_input(message: Message, state: FSMContext):
    await delete_user_message(message)
    await send_temp_notification(message, "Пожалуйста, отправьте PDF или .xlsx файл.")


@router.message(F.text.regexp(r"^/view_calc_(\d+)$"))
async def view_calc_from_command(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)

    match = re.match(r"^/view_calc_(\d+)$", message.text)
    calc_id = int(match.group(1))

    async with async_session_factory() as session:
        calc = await session.get(Calculation, calc_id, options=[selectinload(Calculation.items)])
        if not calc or calc.user_id != message.from_user.id:
            asyncio.create_task(send_temp_notification(message, "Расчет не найден."))
            await state.clear()
            return

    await send_calculation_view(message, state, calc)


@router.callback_query(F.data == "calc_history")
async def get_calc_history(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await show_calc_history_page(callback, state, bot, page=1)
    await callback.answer()


@router.callback_query(F.data.startswith("calc_download_"))
async def download_calculation_excel(callback: CallbackQuery, state: FSMContext):
    calc_id = int(callback.data.split("_")[2])

    async with async_session_factory() as session:
        calc = await session.get(Calculation, calc_id, options=[selectinload(Calculation.items)])
        if not calc or calc.user_id != callback.from_user.id:
            await send_temp_notification(callback.message, "Расчет не найден.")
            return

        consumables_result = await session.execute(select(ConsumableWord.word))
        consumable_words = list(consumables_result.scalars().all())

        excel_file = create_calculation_excel(calc.items, calc.total_cost, consumable_words)

        file_input = BufferedInputFile(excel_file.getvalue(), filename=f"calculation_{calc_id}.xlsx")
        try:
            await callback.message.answer_document(file_input)
        except TelegramBadRequest as e:
            await send_temp_notification(callback.message, f"Ошибка отправки файла: {e}")
    await callback.answer()


@router.callback_query(F.data.startswith("calc_edit_"), CalcState.viewing_calculation)
async def start_editing(callback: CallbackQuery, state: FSMContext):
    calc_id = int(callback.data.split("_")[2])
    await callback.message.delete()

    text = (
        f"Редактирование расчета №{calc_id}.\n"
        "Отправьте команду (текст/голос) или загрузите исправленный Excel."
    )
    menu_msg = await callback.message.answer(
        text,
        reply_markup=calculation_edit_keyboard(calc_id)
    )
    await set_menu_message(menu_msg, state)
    await state.set_state(CalcState.awaiting_edit_command)
    await state.update_data(current_calc_id=calc_id)


@router.message(CalcState.awaiting_edit_command, (F.text | F.voice))
async def process_edit_command(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    data = await state.get_data()
    calc_id = data.get("current_calc_id")
    command_text = ""

    if message.voice:
        processing_msg = await message.answer("Распознаю голосовую команду... 🎙️")
        voice_path = f"temp_voice_{message.voice.file_unique_id}.ogg"
        await bot.download(message.voice, destination=voice_path)

        try:
            command_text = await ai_service_instance.transcribe_voice_command(voice_path)
        except ValueError as e:
            if os.path.exists(voice_path): os.remove(voice_path)
            await processing_msg.delete()
            if "Billing/Quota" in str(e):
                await message.answer(f"🚨 <b>Ошибка оплаты OpenAI (Whisper):</b>\n{e}", parse_mode="HTML")
            else:
                await send_temp_notification(message, f"Ошибка распознавания: {e}")
            return

        if os.path.exists(voice_path): os.remove(voice_path)
        await processing_msg.delete()

        if not command_text:
            await send_temp_notification(message, "Не удалось распознать команду. Попробуйте еще раз.")
            return
    else:
        command_text = message.text

    async with async_session_factory() as session:
        calc = await session.get(Calculation, calc_id, options=[selectinload(Calculation.items)])
        if not calc:
            await send_temp_notification(message, "Ошибка: Расчет не найден.")
            return

        if calc.status == "approved":
            await send_temp_notification(message, "Нельзя редактировать утвержденный расчет.")
            return

        def to_dec(val):
            if val is None: return Decimal('0.0')
            s = str(val).strip().lower()
            if s == 'nan' or s == 'none' or not s: return Decimal('0.0')
            try:
                return Decimal(s)
            except:
                return Decimal('0.0')

        current_total_safe = to_dec(calc.total_cost)

        parsed_command = await ai_service_instance.parse_edit_command(command_text, float(current_total_safe))

        api_error = parsed_command.get("error")
        if api_error:
            error_text = "Ошибка API при обработке команды."
            if "quota" in api_error.lower() or "billing" in api_error.lower() or "429" in api_error:
                await message.answer(f"🚨 <b>Ошибка оплаты OpenAI:</b>\n{api_error}", parse_mode="HTML")
                return
            await send_temp_notification(message, error_text, delay=10)
            return

        new_total_accum = Decimal('0.0')
        cmd_type = parsed_command.get("type", "unknown")

        if cmd_type == "unknown":
            await send_temp_notification(message, "Не распознал команду. Попробуйте еще раз.")
            return

        if cmd_type == "set_total":
            new_total_target = to_dec(parsed_command.get("new_total", current_total_safe))

            if current_total_safe == 0:
                await send_temp_notification(message, "Нельзя подогнать сумму для расчета с нулевым итогом.")
                return

            multiplier = new_total_target / current_total_safe

            for item in calc.items:
                c_work = to_dec(item.cost_per_unit)
                c_mat = to_dec(item.cost_material_per_unit)
                q = to_dec(item.quantity)

                item.cost_per_unit = c_work * multiplier
                item.cost_material_per_unit = c_mat * multiplier
                item.total_cost = (item.cost_per_unit + item.cost_material_per_unit) * q

                new_total_accum += item.total_cost

        elif cmd_type == "set_quantity":
            item_name = parsed_command.get("item_name", "").lower()
            item_row = parsed_command.get("item_row")
            new_quantity = to_dec(parsed_command.get("new_quantity", 0))

            item_found = False

            if not item_name and not item_row:
                await send_temp_notification(message, "Не распознал, для какой позиции менять кол-во.")
                return

            for i, item in enumerate(calc.items, 1):
                match = False
                if item_row and i == item_row:
                    match = True
                elif item_name and item_name in str(item.name).lower():
                    match = True

                c_work = to_dec(item.cost_per_unit)
                c_mat = to_dec(item.cost_material_per_unit)

                if match:
                    item.quantity = new_quantity
                    item.total_cost = new_quantity * (c_work + c_mat)
                    item.source = "manual"
                    item_found = True
                    new_total_accum += item.total_cost
                else:
                    q = to_dec(item.quantity)
                    new_total_accum += q * (c_work + c_mat)

            if not item_found:
                await send_temp_notification(message, f"Не нашел позицию: {item_name or f'строка {item_row}'}")
                return

        elif cmd_type == "set_cost":
            item_name = parsed_command.get("item_name", "").lower()
            item_row = parsed_command.get("item_row")
            new_cost = to_dec(parsed_command.get("new_cost", 0))

            item_found = False

            if not item_name and not item_row:
                await send_temp_notification(message, "Не распознал, для какой позиции менять цену.")
                return

            for i, item in enumerate(calc.items, 1):
                match = False
                if item_row and i == item_row:
                    match = True
                elif item_name and item_name in str(item.name).lower():
                    match = True

                q = to_dec(item.quantity)
                c_mat = to_dec(item.cost_material_per_unit)

                if match:
                    # set_cost sets the work cost; material cost unchanged
                    item.cost_per_unit = new_cost
                    item.total_cost = q * (new_cost + c_mat)
                    item.source = "manual"
                    item_found = True
                    new_total_accum += item.total_cost
                else:
                    c_work = to_dec(item.cost_per_unit)
                    new_total_accum += q * (c_work + c_mat)

            if not item_found:
                await send_temp_notification(message, f"Не нашел позицию: {item_name or f'строка {item_row}'}")
                return

        else:
            percent = to_dec(parsed_command.get("percent", 0))
            multiplier = Decimal('1.0')
            except_rows = set(parsed_command.get("except_rows", []))

            if cmd_type in ["percent_all_increase", "percent_except_increase"]:
                multiplier = Decimal('1.0') + (percent / Decimal('100.0'))
            elif cmd_type in ["percent_all_decrease", "percent_except_decrease"]:
                multiplier = Decimal('1.0') - (percent / Decimal('100.0'))

            for i, item in enumerate(calc.items, 1):
                apply_change = False
                if cmd_type in ["percent_all_increase", "percent_all_decrease"]:
                    apply_change = True
                elif cmd_type in ["percent_except_increase", "percent_except_decrease"] and i not in except_rows:
                    apply_change = True

                c_work = to_dec(item.cost_per_unit)
                c_mat = to_dec(item.cost_material_per_unit)
                q = to_dec(item.quantity)

                if apply_change:
                    item.cost_per_unit = c_work * multiplier
                    item.cost_material_per_unit = c_mat * multiplier
                    item.total_cost = (item.cost_per_unit + item.cost_material_per_unit) * q
                else:
                    item.total_cost = (c_work + c_mat) * q

                new_total_accum += item.total_cost

        calc.total_cost = new_total_accum
        await session.commit()
        await session.refresh(calc, ["items"])

    await delete_previous_menu(state, bot, message.chat.id)
    await send_calculation_view(message, state, calc)


@router.callback_query(F.data.startswith("calc_approve_"), CalcState.viewing_calculation)
async def approve_calculation(callback: CallbackQuery, state: FSMContext, bot: Bot):
    calc_id = int(callback.data.split("_")[2])

    async with async_session_factory() as session:
        calc = await session.get(Calculation, calc_id, options=[selectinload(Calculation.items)])
        if not calc or calc.user_id != callback.from_user.id:
            await callback.answer("Расчет не найден.", show_alert=True)
            return

        if calc.status == "approved":
            await callback.answer("Расчет уже утвержден.", show_alert=True)
            return

        items_learned = 0
        for item in calc.items:
            if item.source in ["manual", "internet"] and (item.cost_per_unit > 0 or item.cost_material_per_unit > 0):
                full_name = item.name
                if item.code and item.code.lower() not in item.name.lower() and "зип" not in item.name.lower():
                    full_name = f"{item.name} {item.code}"

                stmt = select(PriceListItem).where(PriceListItem.name == full_name)
                result = await session.execute(stmt)
                existing_price_item = result.scalar_one_or_none()

                if existing_price_item:
                    existing_price_item.price = item.cost_per_unit
                    existing_price_item.price_material = item.cost_material_per_unit
                else:
                    session.add(PriceListItem(
                        name=full_name,
                        price=item.cost_per_unit,
                        price_material=item.cost_material_per_unit
                    ))
                items_learned += 1

        calc.status = "approved"
        await session.commit()

        if items_learned > 0:
            await price_logic_instance.load_pricelist_cache(session)
            asyncio.create_task(send_temp_notification(callback.message,
                                                       f"Расчет утвержден. База знаний обновлена ({items_learned} поз.)"))
        else:
            asyncio.create_task(send_temp_notification(callback.message, "Расчет утвержден."))

    await get_calc_history(callback, state, bot)


@router.callback_query(F.data.startswith("calc_edit_cmd_"), CalcState.awaiting_edit_command)
async def info_edit_command(callback: CallbackQuery):
    await callback.answer("Пожалуйста, просто отправьте текстовую или голосовую команду в этот чат.", show_alert=True)


@router.callback_query(F.data.startswith("calc_edit_excel_"), CalcState.awaiting_edit_command)
async def info_edit_excel(callback: CallbackQuery):
    await callback.answer("Пожалуйста, просто отправьте .xlsx файл с исправлениями в этот чат.", show_alert=True)


@router.callback_query(F.data == "calc_approved_info", CalcState.viewing_calculation)
async def calc_approved_info(callback: CallbackQuery):
    await callback.answer("Этот расчет уже утвержден. Цены из него (manual, internet) добавлены в базу.", show_alert=True)


@router.message(CalcState.awaiting_edit_command, F.document.mime_type.in_([
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
]))
async def process_excel_update(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    data = await state.get_data()
    calc_id = data.get("current_calc_id")

    excel_path = f"temp_update_{message.document.file_id}.xlsx"
    await bot.download(message.document, destination=excel_path)

    try:
        updates = parse_excel_for_update(excel_path)
    except Exception as e:
        await send_temp_notification(message, f"Ошибка чтения Excel: {e}")
        if os.path.exists(excel_path):
            os.remove(excel_path)
        return
    finally:
        if os.path.exists(excel_path):
            os.remove(excel_path)

    if not updates:
        await send_temp_notification(message, "Не удалось найти данные. Проверьте заголовки (Наименование, Кол-во).")
        return

    async with async_session_factory() as session:
        calc = await session.get(Calculation, calc_id, options=[selectinload(Calculation.items)])
        if not calc:
            await send_temp_notification(message, "Ошибка: Расчет не найден.")
            return

        if calc.status == "approved":
            await send_temp_notification(message, "Нельзя редактировать утвержденный расчет.")
            return

        updates_by_pos = {u['position']: u for u in updates if u.get('position')}
        updates_by_name = {str(u['name']).strip().lower(): u for u in updates}

        new_total = Decimal('0.0')
        updated_count = 0
        items_to_delete = []

        def to_decimal(val):
            if val is None: return Decimal('0.0')
            s = str(val).strip().lower()
            if s == 'nan' or s == 'none' or not s: return Decimal('0.0')
            try:
                return Decimal(s)
            except:
                return Decimal('0.0')

        for i, item in enumerate(calc.items, 1):
            update_data = updates_by_pos.get(i)
            if not update_data:
                key_name = str(item.name).strip().lower()
                update_data = updates_by_name.get(key_name)

            if update_data:
                try:
                    new_full_name = str(update_data.get('name')).strip()
                    new_qty = to_decimal(update_data.get('quantity'))
                    new_price_work = to_decimal(update_data.get('cost_per_unit'))
                    new_price_mat = to_decimal(update_data.get('cost_material_per_unit', 0))
                    new_unit = update_data.get('unit')

                    if item.code and item.code.lower() not in new_full_name.lower():
                        item.code = ""

                    item.name = new_full_name
                    item.quantity = new_qty
                    item.cost_per_unit = new_price_work
                    item.cost_material_per_unit = new_price_mat
                    if new_unit and new_unit != '-' and new_unit != item.unit:
                        item.unit = new_unit

                    item.source = "manual"
                    item.total_cost = item.quantity * (item.cost_per_unit + item.cost_material_per_unit)
                    updated_count += 1
                except Exception as e:
                    print(f"Error updating item {i}: {e}")
            else:
                items_to_delete.append(item)

            if item not in items_to_delete:
                new_total += item.total_cost

        for item in items_to_delete:
            await session.delete(item)

        calc.total_cost = new_total
        await session.commit()
        await session.refresh(calc, ["items"])

    msg = f"✅ Расчет обновлен!\nОбновлено строк: {updated_count}\nНовая сумма: {calc.total_cost:,.2f}"
    await delete_previous_menu(state, bot, message.chat.id)
    await send_temp_notification(message, msg, delay=7)
    await send_calculation_view(message, state, calc)


@router.callback_query(F.data.startswith("calc_view_"), StateFilter(
    CalcState.awaiting_edit_command,
    CalcState.awaiting_delete_confirm
))
async def back_to_calculation_view(callback: CallbackQuery, state: FSMContext):
    calc_id = int(callback.data.split("_")[2])

    async with async_session_factory() as session:
        calc = await session.get(Calculation, calc_id, options=[selectinload(Calculation.items)])
        if not calc or calc.user_id != callback.from_user.id:
            await send_temp_notification(callback.message, "Расчет не найден.")
            await state.clear()
            await show_calc_history_page(callback, state, await callback.message.bot.me(), page=1)
            return

    await send_calculation_view(callback, state, calc)


@router.callback_query(F.data.startswith("calc_delete_"), CalcState.viewing_calculation)
async def ask_delete_calculation(callback: CallbackQuery, state: FSMContext):
    calc_id = int(callback.data.split("_")[2])

    await callback.message.edit_text(
        f"Вы уверены, что хотите удалить Расчет №{calc_id}?\n\nЭто действие необратимо.",
        reply_markup=confirm_calc_delete_keyboard(calc_id)
    )
    await state.set_state(CalcState.awaiting_delete_confirm)


@router.callback_query(F.data.startswith("calc_delete_confirm_"), CalcState.awaiting_delete_confirm)
async def execute_delete_calculation(callback: CallbackQuery, state: FSMContext, bot: Bot):
    calc_id = int(callback.data.split("_")[3])

    async with async_session_factory() as session:
        calc = await session.get(Calculation, calc_id)
        if calc and calc.user_id == callback.from_user.id:
            await session.delete(calc)
            await session.commit()
            asyncio.create_task(send_temp_notification(callback.message, f"Расчет №{calc_id} удален."))
        else:
            asyncio.create_task(send_temp_notification(callback.message, "Расчет не найден."))

    await callback.message.delete()
    await show_calc_history_page(callback, state, bot, page=1)


@router.callback_query(F.data == "admin_menu")
@router.message(F.text == "/admin")
async def show_admin_menu(event: Message | CallbackQuery, state: FSMContext, bot: Bot):
    if not await is_owner(event, config):
        if isinstance(event, CallbackQuery):
            await event.answer("Доступ запрещен.", show_alert=False)
        return

    if isinstance(event, Message):
        await delete_user_message(event)
        target_message = event
    else:
        await event.message.delete()
        target_message = event.message

    await state.clear()
    text = "Админ-панель"
    menu_msg = await target_message.answer(text, reply_markup=admin_menu_keyboard())
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.menu)


@router.callback_query(F.data == "admin_upload_price", Admin.menu)
async def request_price_list(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    text = (
        "<b>Загрузка прайс-листа</b>\n\n"
        "Отправьте <code>.csv</code> или <code>.xlsx</code> файл для добавления/обновления данных.\n\n"
        "<b>Очистка:</b>\n"
        "Если вы хотите <b>заменить</b> старый прайс-лист, сначала нажмите «Очистить прайс-лист», "
        "а затем загрузите новый файл."
    )
    menu_msg = await callback.message.answer(
        text,
        reply_markup=price_list_menu_keyboard(),
        parse_mode="HTML"
    )
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_price_list)


@router.callback_query(F.data == "price_clear_confirm", Admin.awaiting_price_list)
async def ask_clear_price_list(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Вы уверены, что хотите <b>ПОЛНОСТЬЮ</b> очистить прайс-лист?\n\n"
        "Все цены, загруженные из файлов, будут удалены. "
        "Цены, полученные через «Утвердить», останутся.",
        reply_markup=confirm_clear_price_list_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(Admin.awaiting_price_list_clear_confirm)


@router.callback_query(F.data == "price_clear_execute", Admin.awaiting_price_list_clear_confirm)
async def execute_clear_price_list(callback: CallbackQuery, state: FSMContext, bot: Bot):
    async with async_session_factory() as session:
        await session.execute(delete(PriceListItem))
        await session.commit()
        await price_logic_instance.load_pricelist_cache(session)

    asyncio.create_task(send_temp_notification(callback.message, "Прайс-лист полностью очищен."))
    await request_price_list(callback, state)


@router.callback_query(F.data == "admin_upload_price", Admin.awaiting_price_list_clear_confirm)
async def back_to_price_list_menu_from_confirm(callback: CallbackQuery, state: FSMContext):
    await request_price_list(callback, state)


@router.message(Admin.awaiting_price_list, F.document.mime_type.in_([
    "text/csv",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
]))
async def process_price_list(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    await delete_previous_menu(state, bot, message.chat.id)

    file_info = await bot.get_file(message.document.file_id)
    file_content = await bot.download_file(file_info.file_path)

    items_to_upsert = {}
    total_sheets = 0
    processed_sheets = 0

    try:
        excel_sheets = {}
        if message.document.mime_type == "text/csv":
            excel_sheets['Sheet1'] = pd.read_csv(file_content, header=None)
        else:
            excel_sheets = pd.read_excel(file_content, sheet_name=None, header=None)

        if not excel_sheets:
            asyncio.create_task(send_temp_notification(message, "Ошибка: Excel-файл пуст."))
            await show_admin_menu(message, state, bot)
            return

        total_sheets = len(excel_sheets)
        first_sheet_name = list(excel_sheets.keys())[0]
        df_first_sheet = excel_sheets[first_sheet_name]

        if df_first_sheet.empty:
            asyncio.create_task(send_temp_notification(message, "Ошибка: Первый лист Excel-файла пуст."))
            await show_admin_menu(message, state, bot)
            return

        header_row_index = -1
        name_col_idx = -1
        price_material_col_idx = -1
        price_work_col_idx = -1

        for i, row in df_first_sheet.head(20).iterrows():
            row_values = [str(cell).lower() for cell in row.tolist()]
            has_name = any('наимен' in cell or 'вид работ' in cell for cell in row_values)
            has_material = any('материал' in cell for cell in row_values)
            has_work = any('работ' in cell for cell in row_values)
            # Also accept single price column for backward compat
            has_price_single = any(
                ('цена' in cell or 'стоимост' in cell) and 'материал' not in cell and 'работ' not in cell
                for cell in row_values
            )

            if has_name and (has_material or has_work or has_price_single):
                header_row_index = i
                temp_header = df_first_sheet.iloc[header_row_index].tolist()
                try:
                    name_col_idx = next(
                        j for j, h in enumerate(temp_header)
                        if 'наимен' in str(h).lower() or 'вид работ' in str(h).lower()
                    )
                except StopIteration:
                    header_row_index = -1
                    continue

                # Find material price column
                for j, h in enumerate(temp_header):
                    h_str = str(h).lower()
                    if 'материал' in h_str and ('цена' in h_str or 'стоимост' in h_str or j > name_col_idx):
                        price_material_col_idx = j
                        break
                if price_material_col_idx == -1:
                    # Check next row for sub-header "МАТЕРИАЛОВ"
                    if i + 1 < len(df_first_sheet):
                        next_row = df_first_sheet.iloc[i + 1].tolist()
                        for j, h in enumerate(next_row):
                            if 'материал' in str(h).lower() and j > name_col_idx:
                                price_material_col_idx = j
                                break

                # Find work price column
                for j, h in enumerate(temp_header):
                    h_str = str(h).lower()
                    if 'работ' in h_str and ('цена' in h_str or 'стоимост' in h_str or j > name_col_idx):
                        price_work_col_idx = j
                        break
                if price_work_col_idx == -1:
                    # Check next row for sub-header "РАБОТ"
                    if i + 1 < len(df_first_sheet):
                        next_row = df_first_sheet.iloc[i + 1].tolist()
                        for j, h in enumerate(next_row):
                            if 'работ' in str(h).lower() and j > name_col_idx:
                                price_work_col_idx = j
                                break

                # Fallback: single price column (backward compat)
                if price_work_col_idx == -1 and price_material_col_idx == -1:
                    for j, h in enumerate(temp_header):
                        h_str = str(h).lower()
                        if ('цена' in h_str or 'стоимост' in h_str) and j > name_col_idx:
                            price_work_col_idx = j
                            break

                if name_col_idx >= 0 and (price_material_col_idx >= 0 or price_work_col_idx >= 0):
                    break
                else:
                    header_row_index = -1

        if header_row_index == -1:
            msg = (
                "Ошибка: Не найдены заголовки прайс-листа.\n"
                "Ожидаемые колонки: 'Наименование'/'Вид работ' + 'Материалов' + 'Работ'.\n"
                "Или: 'Наименование' + 'Цена'/'Стоимость'."
            )
            asyncio.create_task(send_temp_notification(message, msg, delay=10))
            await show_admin_menu(message, state, bot)
            return

        def _parse_price(val) -> float:
            if pd.isna(val):
                return 0.0
            try:
                return float(str(val).replace(" ", "").replace("\xa0", "").replace(",", "."))
            except (ValueError, TypeError):
                return 0.0

        for sheet_name, df_no_header in excel_sheets.items():
            start_row = 0
            if sheet_name == first_sheet_name:
                start_row = header_row_index + 1

            for i, row in df_no_header.iloc[start_row:].iterrows():
                max_needed_idx = max(
                    name_col_idx,
                    price_material_col_idx if price_material_col_idx >= 0 else 0,
                    price_work_col_idx if price_work_col_idx >= 0 else 0
                )
                if len(row) <= max_needed_idx:
                    continue

                name_val = row.get(name_col_idx)
                if pd.isna(name_val):
                    continue

                name = str(name_val).strip()
                if not name or name.lower() in ['nan', 'none']:
                    continue

                price_material = _parse_price(row.get(price_material_col_idx) if price_material_col_idx >= 0 else None)
                price_work = _parse_price(row.get(price_work_col_idx) if price_work_col_idx >= 0 else None)

                if name and (price_material > 0 or price_work > 0):
                    items_to_upsert[name] = {"price": price_work, "price_material": price_material}

            processed_sheets += 1

        if not items_to_upsert:
            msg = f"Не найдено данных для обновления. Всего листов: {total_sheets}, обработано: {processed_sheets}."
            asyncio.create_task(send_temp_notification(message, msg, delay=10))
            await show_admin_menu(message, state, bot)
            return

        mappings = [
            {"name": name, "price": vals["price"], "price_material": vals["price_material"]}
            for name, vals in items_to_upsert.items()
        ]

        async with async_session_factory() as session:
            dialect = session.bind.dialect.name
            stmt = None

            if dialect == "sqlite":
                stmt = sqlite_insert(PriceListItem).values(mappings)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['name'],
                    set_={'price': stmt.excluded.price, 'price_material': stmt.excluded.price_material}
                )
            elif dialect == "postgresql":
                stmt = postgresql_insert(PriceListItem).values(mappings)
                stmt = stmt.on_conflict_do_update(
                    constraint='price_list_item_name_key',
                    set_={'price': stmt.excluded.price, 'price_material': stmt.excluded.price_material}
                )
            else:
                asyncio.create_task(send_temp_notification(message, f"Ошибка: Upsert не настроен для {dialect}"))
                await show_admin_menu(message, state, bot)
                return

            await session.execute(stmt)
            await session.commit()
            await price_logic_instance.load_pricelist_cache(session)

        msg = f"Прайс-лист обновлен. Добавлено/Обновлено {len(mappings)} позиций. (Обработано {processed_sheets}/{total_sheets} листов)."
        asyncio.create_task(send_temp_notification(message, msg, delay=10))

    except Exception as e:
        error_msg = f"Ошибка обработки файла: {str(e)}"
        if "'NoneType' object is not iterable" in str(e):
            error_msg = "Ошибка обработки файла: возможно, файл пуст или имеет неверный формат."
        elif not str(e):
            error_msg = "Ошибка обработки файла: не удалось найти колонки."
        asyncio.create_task(send_temp_notification(message, error_msg))

    await show_admin_menu(message, state, bot)


@router.callback_query(F.data == "admin_download_price", Admin.awaiting_price_list)
async def download_price_list(callback: CallbackQuery, state: FSMContext):
    async with async_session_factory() as session:
        result = await session.execute(
            select(PriceListItem).order_by(PriceListItem.name)
        )
        items = result.scalars().all()

        if not items:
            await callback.answer("Прайс-лист пуст.", show_alert=True)
            return

        excel_file = create_pricelist_excel(items)
        file_input = BufferedInputFile(excel_file.getvalue(), filename="current_pricelist.xlsx")
        try:
            await callback.message.answer_document(file_input)
        except TelegramBadRequest as e:
             await send_temp_notification(callback.message, f"Ошибка отправки файла: {e}")

    await callback.answer()


@router.message(Admin.awaiting_price_list)
async def wrong_price_list_input(message: Message):
    await delete_user_message(message)
    await send_temp_notification(message, "Пожалуйста, отправьте CSV или Excel (.xlsx) файл.")


async def show_ai_settings(event: Message | CallbackQuery, state: FSMContext):
    if isinstance(event, Message):
        target_message = event
    else:
        target_message = event.message

    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)

    key_status_openai = f"<code>...{settings.openai_api_key[-4:]}</code>" if settings.openai_api_key else "⚠️ Не задан"

    prompt_text = (settings.system_prompt or "⚠️ Не задан")
    prompt_short = (prompt_text[:70] + '...') if len(prompt_text) > 73 else prompt_text
    prompt_display = html.escape(prompt_short)

    text = (
        "<b>🤖 Настройки ИИ</b>\n\n"
        f"<b>Активная Модель:</b> <code>{settings.ai_model}</code>\n\n"
        f"<b>OpenAI Ключ (GPT, RAG, Whisper):</b> {key_status_openai}\n\n"
        f"<b>Системный промпт (кратко):</b>\n"
        f"<i>{prompt_display}</i>"
    )

    menu_msg = None
    if isinstance(event, Message):
        menu_msg = await target_message.answer(
            text,
            reply_markup=ai_settings_keyboard(),
            parse_mode="HTML"
        )
    else:
        try:
            await target_message.edit_text(
                text,
                reply_markup=ai_settings_keyboard(),
                parse_mode="HTML"
            )
            menu_msg = target_message
        except TelegramBadRequest:
            try:
                await target_message.delete()
            except TelegramBadRequest:
                pass
            menu_msg = await target_message.answer(
                text,
                reply_markup=ai_settings_keyboard(),
                parse_mode="HTML"
            )

    if menu_msg:
        await set_menu_message(menu_msg, state)
    await state.set_state(Admin.settings_menu)


@router.callback_query(F.data == "ai_set_key", Admin.settings_menu)
async def request_api_key(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    text = "Введите новый API ключ OpenAI (sk-...):"
    menu_msg = await callback.message.answer(text, reply_markup=back_button("admin_ai_settings"))
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_api_key)


@router.message(Admin.awaiting_api_key, F.text.startswith("sk-"))
async def save_api_key(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)

    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)
        settings.openai_api_key = message.text.strip()
        await session.commit()
        await session.refresh(settings)
        await ai_service_instance.update_settings(settings)

    asyncio.create_task(send_temp_notification(message, "API ключ обновлен."))
    await show_ai_settings(message, state)


@router.callback_query(F.data == "ai_set_model", Admin.settings_menu)
async def request_model(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    text = "Выберите модель OpenAI.\n\n" \
           "<b>Важно:</b> Для парсинга PDF (чтения спецификаций) необходима модель с 'Vision' (gpt-4o, gpt-4-turbo)."
    menu_msg = await callback.message.answer(
        text,
        reply_markup=ai_models_keyboard(),
        parse_mode="HTML"
    )
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_model)


@router.callback_query(F.data.startswith("ai_set_model_"), Admin.awaiting_model)
async def save_selected_model(callback: CallbackQuery, state: FSMContext, bot: Bot):
    model_name = callback.data.split("ai_set_model_")[1]

    if model_name == "custom":
        await callback.message.delete()
        text = "Введите название модели OpenAI (например, gpt-4o или gpt-3.5-turbo):"
        menu_msg = await callback.message.answer(text, reply_markup=back_button("admin_ai_settings"))
        await set_menu_message(menu_msg, state)
        await state.set_state(Admin.awaiting_model)
        return

    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)
        settings.ai_model = model_name
        await session.commit()
        await session.refresh(settings)
        await ai_service_instance.update_settings(settings)

    await callback.message.delete()
    asyncio.create_task(send_temp_notification(callback.message, f"Модель обновлена: {model_name}"))
    await show_ai_settings(callback, state)


@router.message(Admin.awaiting_model, F.text)
async def save_custom_model(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)
    model_name = message.text.strip()

    if not model_name:
        asyncio.create_task(send_temp_notification(message, "Ошибка: Название модели не может быть пустым."))
        await show_ai_settings(message, state)
        return

    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)
        settings.ai_model = model_name
        await session.commit()
        await session.refresh(settings)
        await ai_service_instance.update_settings(settings)

    asyncio.create_task(send_temp_notification(message, f"Модель обновлена: {model_name}"))
    await show_ai_settings(message, state)


@router.callback_query(F.data == "ai_set_prompt", Admin.settings_menu)
async def request_system_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    text = (
        "Отправьте новый системный промпт текстом (до 4096 симв.)\n"
        "ИЛИ\n"
        "Загрузите .txt файл с полным текстом промпта."
    )
    menu_msg = await callback.message.answer(text, reply_markup=back_button("admin_ai_settings"))
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_system_prompt)


@router.message(Admin.awaiting_system_prompt, F.text)
async def save_system_prompt(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)

    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)
        settings.system_prompt = message.text.strip()
        await session.commit()
        await session.refresh(settings)
        await ai_service_instance.update_settings(settings)

    asyncio.create_task(send_temp_notification(message, "Системный промпт обновлен."))
    await show_ai_settings(message, state)


@router.message(Admin.awaiting_system_prompt, F.document.mime_type == "text/plain")
async def save_system_prompt_from_file(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    await delete_previous_menu(state, bot, message.chat.id)

    file_path = f"temp_prompt_{message.document.file_unique_id}.txt"
    try:
        await bot.download(message.document, destination=file_path)

        with open(file_path, 'r', encoding='utf-8') as f:
            prompt_text = f.read()

        if not prompt_text.strip():
            asyncio.create_task(send_temp_notification(message, "Ошибка: Файл .txt пуст."))
            await show_ai_settings(message, state)
            return

        async with async_session_factory() as session:
            settings = await session.get(BotSettings, 1)
            settings.system_prompt = prompt_text.strip()
            await session.commit()
            await session.refresh(settings)
            await ai_service_instance.update_settings(settings)

        asyncio.create_task(send_temp_notification(message, "Системный промпт обновлен из файла."))

    except Exception as e:
        asyncio.create_task(send_temp_notification(message, f"Ошибка чтения файла: {e}"))
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    await show_ai_settings(message, state)


@router.callback_query(F.data == "ai_download_prompt", Admin.settings_menu)
async def download_system_prompt(callback: CallbackQuery, state: FSMContext):
    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)
        prompt_text = (settings.system_prompt or "Промпт не задан.")

    try:
        prompt_bytes = prompt_text.encode('utf-8')
        file_input = BufferedInputFile(prompt_bytes, filename="system_prompt.txt")
        await callback.message.answer_document(file_input)
        await callback.answer()

    except Exception as e:
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data == "ai_show_full_prompt", Admin.settings_menu)
async def show_full_system_prompt(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)
        prompt_text = (settings.system_prompt or "Промпт не задан.")

    prompt_display = html.escape(prompt_text)

    max_len = 3900
    truncated_message = ""

    if len(prompt_display) > max_len:
        prompt_display = prompt_display[:max_len]
        truncated_message = "\n\n<i>[...текст обрезан, скачайте файл для полной версии...]</i>"

    text = f"<b>Текущий системный промпт</b> (нажми, чтобы скопировать):\n<pre>{prompt_display}</pre>{truncated_message}"

    menu_msg = await callback.message.answer(
        text,
        reply_markup=back_button("admin_ai_settings"),
        parse_mode="HTML"
    )
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.viewing_system_prompt)


@router.callback_query(F.data == "admin_ai_settings", StateFilter(
    Admin.awaiting_api_key,
    Admin.awaiting_model,
    Admin.awaiting_system_prompt,
    Admin.viewing_system_prompt
))
async def back_to_ai_settings(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await show_ai_settings(callback, state)


@router.callback_query(F.data == "admin_stopwords", Admin.menu)
async def show_stopwords_menu(callback: CallbackQuery, state: FSMContext):
    await show_stopwords_menu_helper(callback, state, page=1)


@router.callback_query(F.data.startswith("stopword_page_"), Admin.stop_words_menu)
async def handle_stopword_page(callback: CallbackQuery, state: FSMContext):
    if callback.data == "stopword_page_info":
        await callback.answer(f"Текущая страница.")
        return

    page = int(callback.data.split("_")[2])
    await show_stopwords_menu_helper(callback, state, page=page, edit=True)
    await callback.answer()


@router.callback_query(F.data.startswith("stopword_add_page_"), Admin.stop_words_menu)
async def request_stop_word(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[3])
    await state.update_data(stopword_page=page)

    await callback.message.delete()
    text = "Введите слово или фразу для добавления в стоп-лист:"

    menu_msg = await callback.message.answer(text, reply_markup=back_button("admin_stopwords"))
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_stop_word_add)


@router.message(Admin.awaiting_stop_word_add, F.text)
async def save_stop_word(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)
    word = message.text.lower().strip()

    if not word or len(word) < 2:
        msg = "Ошибка: Нельзя добавить пустое или слишком короткое (меньше 2 букв) стоп-слово."
        asyncio.create_task(send_temp_notification(message, msg))

        data = await state.get_data()
        page = data.get("stopword_page", 1)
        await show_stopwords_menu_helper(message, state, page=page)
        return

    data = await state.get_data()
    page = data.get("stopword_page", 1)

    async with async_session_factory() as session:
        exists = await session.execute(select(StopWord).where(StopWord.word == word))
        if not exists.scalar_one_or_none():
            session.add(StopWord(word=word))
            await session.commit()
            await price_logic_instance.load_stopwords(session)
            asyncio.create_task(send_temp_notification(message, f"Слово '{word}' добавлено."))
        else:
            asyncio.create_task(send_temp_notification(message, f"Слово '{word}' уже в списке."))

    await show_stopwords_menu_helper(message, state, page=page)


@router.callback_query(F.data.startswith("stopword_delete_"), Admin.stop_words_menu)
async def delete_stop_word(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    word_id = int(parts[2])
    page = int(parts[4])
    word_text = "Слово"

    async with async_session_factory() as session:
        word = await session.get(StopWord, word_id)
        if word:
            word_text = f"Слово '{word.word}'"
            await session.delete(word)
            await session.commit()
            await price_logic_instance.load_stopwords(session)

    asyncio.create_task(send_temp_notification(callback.message, f"{word_text} удалено."))

    await show_stopwords_menu_helper(callback, state, page=page, edit=True)
    await callback.answer()


@router.callback_query(F.data == "admin_stopwords", StateFilter(
    Admin.awaiting_stop_word_add
))
async def back_to_stopwords_menu(callback: CallbackQuery, state: FSMContext):
    await show_stopwords_menu_helper(callback, state, page=1)


@router.callback_query(F.data == "admin_rag_docs", Admin.menu)
async def show_rag_docs_menu(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()

    docs = await vector_db_instance.list_document_sources()

    text = "Управление базой знаний (RAG).\n\n<b>Загруженные файлы:</b>\n"
    text += "\n".join(f"• <code>{doc}</code>" for doc in docs) if docs else "<i>(пока пусто)</i>"

    menu_msg = await callback.message.answer(
        text,
        reply_markup=rag_docs_menu_keyboard(docs),
        parse_mode="HTML"
    )
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.docs_menu)


async def show_rag_docs_menu(event: Message | CallbackQuery, state: FSMContext):
    if isinstance(event, Message):
        target_message = event
    else:
        target_message = event.message

    docs = await vector_db_instance.list_document_sources()

    text = "Управление базой знаний (RAG).\n\n<b>Загруженные файлы:</b>\n"
    text += "\n".join(f"• <code>{doc}</code>" for doc in docs) if docs else "<i>(пока пусто)</i>"

    menu_msg = None
    if isinstance(event, Message):
        menu_msg = await target_message.answer(
            text,
            reply_markup=rag_docs_menu_keyboard(docs),
            parse_mode="HTML"
        )
    else:
        try:
            await target_message.edit_text(
                text,
                reply_markup=rag_docs_menu_keyboard(docs),
                parse_mode="HTML"
            )
            menu_msg = target_message
        except TelegramBadRequest:
            try:
                await target_message.delete()
            except TelegramBadRequest:
                pass
            menu_msg = await target_message.answer(
                text,
                reply_markup=rag_docs_menu_keyboard(docs),
                parse_mode="HTML"
            )

    if menu_msg:
        await set_menu_message(menu_msg, state)
    await state.set_state(Admin.docs_menu)


@router.callback_query(F.data == "rag_upload", Admin.docs_menu)
async def request_rag_doc(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    text = "Пожалуйста, загрузите документ (.txt, .pdf, .xlsx, .xls) для добавления в базу знаний."
    menu_msg = await callback.message.answer(text, reply_markup=back_button("admin_rag_docs"))
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_doc_upload)


@router.message(Admin.awaiting_doc_upload, F.document.mime_type.in_([
    "text/plain",
    "application/pdf",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
]))
async def process_rag_doc(message: Message, state: FSMContext, bot: Bot):
    await add_message_to_history(message, state)
    await delete_previous_menu(state, bot, message.chat.id)

    processing_msg = await message.answer("Обработка документа... ⏳")

    file_path = f"temp_rag_{message.document.file_unique_id}"
    await bot.download(message.document, destination=file_path)

    doc_text = ""
    try:
        if message.document.mime_type == "application/pdf":
            with pdfplumber.open(file_path) as pdf:
                all_pages_text = [page.extract_text() for page in pdf.pages if page.extract_text()]
                doc_text = "\n".join(all_pages_text)

        elif message.document.mime_type in [
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ]:
            xls_sheets = pd.read_excel(file_path, sheet_name=None, header=None)
            all_sheets_text = []
            if xls_sheets:
                for sheet_name, df in xls_sheets.items():
                    if not df.empty:
                        all_sheets_text.append(df.to_string(index=False, header=False))
            doc_text = "\n\n".join(all_sheets_text)

        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                doc_text = f.read()
    except Exception as e:
        await processing_msg.delete()
        asyncio.create_task(send_temp_notification(message, f"Ошибка чтения файла: {e}"))
        await show_rag_docs_menu(message, state)
        return
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    if not doc_text:
        await processing_msg.delete()
        asyncio.create_task(send_temp_notification(message, "Файл пуст или не удалось извлечь текст."))
        await show_rag_docs_menu(message, state)
        return

    initial_chunks = [chunk.strip() for chunk in doc_text.split('\n\n') if chunk.strip()]

    final_chunks = []
    MAX_CHAR_LEN = 3000

    for chunk in initial_chunks:
        if len(chunk) <= MAX_CHAR_LEN:
            final_chunks.append(chunk)
        else:
            sub_chunks = [sub.strip() for sub in chunk.split('\n') if sub.strip()]

            current_sub_chunk = ""
            for sub in sub_chunks:
                if len(current_sub_chunk) + len(sub) + 1 <= MAX_CHAR_LEN:
                    current_sub_chunk += sub + "\n"
                else:
                    if current_sub_chunk.strip():
                        final_chunks.append(current_sub_chunk.strip())

                    if len(sub) > MAX_CHAR_LEN:
                        for i in range(0, len(sub), MAX_CHAR_LEN):
                            final_chunks.append(sub[i:i + MAX_CHAR_LEN])
                        current_sub_chunk = ""
                    else:
                        current_sub_chunk = sub + "\n"

            if current_sub_chunk.strip():
                final_chunks.append(current_sub_chunk.strip())

    if not final_chunks:
        await processing_msg.delete()
        asyncio.create_task(send_temp_notification(message, "Не найдено текстовых блоков в документе."))
        await show_rag_docs_menu(message, state)
        return

    file_id_base = message.document.file_unique_id
    doc_ids = [f"{file_id_base}_{i}" for i in range(len(final_chunks))]
    metadatas = [{"source": message.document.file_name or "unknown_file"} for _ in final_chunks]

    try:
        await vector_db_instance.add_documents(docs=final_chunks, metadatas=metadatas, ids=doc_ids)
        await processing_msg.delete()
        asyncio.create_task(send_temp_notification(message, f"Документ добавлен в RAG. {len(final_chunks)} чанков."))

    except ValueError as e:
        await processing_msg.delete()
        asyncio.create_task(send_temp_notification(message, f"Ошибка RAG: {e}. Проверьте API ключ OpenAI."))
    except Exception as e:
        await processing_msg.delete()
        asyncio.create_task(send_temp_notification(message, f"Ошибка добавления в ChromaDB: {e}"))

    await show_rag_docs_menu(message, state)


@router.message(Admin.awaiting_doc_upload)
async def wrong_rag_doc_input(message: Message):
    await delete_user_message(message)
    await send_temp_notification(message, "Пожалуйста, отправьте .txt, .pdf, .xls или .xlsx файл.")


@router.callback_query(F.data.startswith("rag_confirm_delete_"), Admin.docs_menu)
async def ask_delete_rag_doc_confirm(callback: CallbackQuery, state: FSMContext):
    doc_hash = callback.data.replace("rag_confirm_delete_", "")

    docs = await vector_db_instance.list_document_sources()
    doc_name = None
    for d in docs:
        if hashlib.md5(d.encode()).hexdigest() == doc_hash:
            doc_name = d
            break

    if not doc_name:
        await callback.answer("Ошибка: Документ не найден.", show_alert=True)
        await callback.message.delete()
        await show_rag_docs_menu(callback, state)
        return

    await callback.message.edit_text(
        f"Вы уверены, что хотите удалить документ:\n<code>{doc_name}</code>\n\nЭто действие необратимо.",
        reply_markup=confirm_delete_keyboard(doc_hash),
        parse_mode="HTML"
    )
    await state.set_state(Admin.awaiting_doc_delete_confirm)


@router.callback_query(F.data.startswith("rag_delete_confirm_"), Admin.awaiting_doc_delete_confirm)
async def execute_delete_rag_doc(callback: CallbackQuery, state: FSMContext):
    doc_hash = callback.data.replace("rag_delete_confirm_", "")

    docs = await vector_db_instance.list_document_sources()
    doc_name = None
    for d in docs:
        if hashlib.md5(d.encode()).hexdigest() == doc_hash:
            doc_name = d
            break

    if not doc_name:
        await callback.answer("Ошибка: Документ не найден.", show_alert=True)
        await callback.message.delete()
        await show_rag_docs_menu(callback, state)
        return

    try:
        await vector_db_instance.delete_documents_by_source(doc_name)
        asyncio.create_task(send_temp_notification(callback.message, f"Документ '{doc_name}' удален из RAG."))
    except Exception as e:
        asyncio.create_task(send_temp_notification(callback.message, f"Ошибка удаления: {e}"))

    await callback.message.delete()
    await show_rag_docs_menu(callback, state)


@router.callback_query(F.data == "admin_rag_docs", StateFilter(
    Admin.awaiting_doc_upload,
    Admin.awaiting_doc_delete_confirm
))
async def back_to_rag_menu(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await show_rag_docs_menu(callback, state)


@router.callback_query(F.data == "admin_menu", StateFilter(
    Admin.awaiting_price_list,
    Admin.settings_menu,
    Admin.stop_words_menu,
    Admin.docs_menu,
    Admin.users_menu,
    Admin.user_management,
    Admin.consumables_menu,
    Admin.awaiting_consumable_add
))
async def back_to_admin_menu(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await callback.message.delete()
    await show_admin_menu(callback, state, bot)


@router.callback_query(F.data == "admin_users_list", Admin.menu)
@router.callback_query(F.data == "admin_users_list", Admin.user_management)
async def show_users_list(callback: CallbackQuery, state: FSMContext):
    await show_users_list_helper(callback, state, page=1)


@router.callback_query(F.data.startswith("admin_users_page_"), Admin.users_menu)
async def handle_users_page(callback: CallbackQuery, state: FSMContext):
    if callback.data == "admin_users_page_info":
        await callback.answer()
        return

    try:
        page = int(callback.data.split("_")[3])
    except (IndexError, ValueError):
        page = 1

    await show_users_list_helper(callback, state, page=page, edit=True)
    await callback.answer()


async def show_users_list_helper(event: Message | CallbackQuery, state: FSMContext, page: int = 1, edit: bool = False):
    if isinstance(event, Message):
        target_message = event
    else:
        target_message = event.message

    async with async_session_factory() as session:
        total_count_result = await session.execute(select(func.count(User.user_id)))
        total_count = total_count_result.scalar_one()

        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        if total_pages == 0:
            total_pages = 1

        if page > total_pages:
            page = total_pages
        if page < 1:
            page = 1

        offset = (page - 1) * PAGE_SIZE
        result = await session.execute(
            select(User).order_by(User.created_at.desc()).offset(offset).limit(PAGE_SIZE)
        )
        users = result.scalars().all()

    text = f"<b>Управление пользователями</b>\nВсего пользователей: {total_count}\nСтраница {page}/{total_pages}"

    reply_markup = admin_users_list_keyboard(users, page, total_pages)
    menu_msg = None

    if isinstance(event, Message):
        menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        try:
            if edit:
                await target_message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
                menu_msg = target_message
            else:
                await target_message.delete()
                menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
        except TelegramBadRequest:
            menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")

    if menu_msg:
        await set_menu_message(menu_msg, state)
    await state.set_state(Admin.users_menu)


@router.callback_query(F.data.startswith("admin_manage_user_"), Admin.users_menu)
async def manage_single_user(callback: CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split("_")[3])

    async with async_session_factory() as session:
        user = await session.get(User, user_id)
        if not user:
            await callback.answer("Пользователь не найден.", show_alert=True)
            await show_users_list_helper(callback, state, page=1, edit=True)
            return

        status_text = "⛔ ЗАБЛОКИРОВАН" if user.is_blocked else "✅ АКТИВЕН"
        reg_date = user.created_at.strftime('%d.%m.%Y %H:%M')

        text = (
            f"<b>Управление пользователем</b>\n\n"
            f"ID: <code>{user.user_id}</code>\n"
            f"Имя: {html.escape(user.first_name)}\n"
            f"Username: @{user.username if user.username else 'Нет'}\n"
            f"Дата регистрации: {reg_date}\n\n"
            f"Статус: <b>{status_text}</b>"
        )

        reply_markup = admin_user_manage_keyboard(user.user_id, user.is_blocked)

        await callback.message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
        await state.set_state(Admin.user_management)


@router.callback_query(F.data.startswith("admin_block_user_") | F.data.startswith("admin_unblock_user_"),
                       Admin.user_management)
async def toggle_user_block(callback: CallbackQuery, state: FSMContext):
    action = "block" if "admin_block_user_" in callback.data else "unblock"
    user_id = int(callback.data.split("_")[3])

    if action == "block" and user_id == callback.from_user.id:
        await callback.answer("Нельзя заблокировать самого себя!", show_alert=True)
        return

    async with async_session_factory() as session:
        user = await session.get(User, user_id)
        if user:
            user.is_blocked = (action == "block")
            await session.commit()

            status_msg = "заблокирован" if action == "block" else "разблокирован"
            await callback.answer(f"Пользователь {status_msg}.", show_alert=False)

            await manage_single_user(callback, state)
            try:
                if action == "block":
                    await callback.bot.send_message(user_id, "⛔ Ваш доступ к боту был заблокирован администратором.")
                else:
                    await callback.bot.send_message(user_id, "✅ Ваш доступ к боту восстановлен.")
            except:
                pass
        else:
            await callback.answer("Пользователь не найден.", show_alert=True)
            await show_users_list_helper(callback, state, page=1)


@router.callback_query(F.data == "admin_ai_settings", Admin.menu)
async def show_ai_settings_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await show_ai_settings(callback, state)


async def show_stopwords_menu_helper(event: Message | CallbackQuery, state: FSMContext, page: int = 1,
                                     edit: bool = False):
    if isinstance(event, Message):
        target_message = event
    else:
        target_message = event.message

    async with async_session_factory() as session:
        total_count_result = await session.execute(select(func.count(StopWord.id)))
        total_count = total_count_result.scalar_one()

        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        if total_pages == 0:
            total_pages = 1

        if page > total_pages:
            page = total_pages

        offset = (page - 1) * PAGE_SIZE
        result = await session.execute(
            select(StopWord).order_by(StopWord.word).offset(offset).limit(PAGE_SIZE)
        )
        stopwords = result.scalars().all()

    text = (
        "<b>Управление стоп-словами (расходниками)</b>\n\n"
        "<b>Как это работает:</b>\n"
        "1. Бот <b>НЕ</b> будет искать эти слова в интернете (OpenAI).\n"
        "2. Бот <b>БУДЕТ</b> искать их в твоем прайс-листе (CSV).\n\n"
        "<i>Например: Если 'лоток' тут, бот не будет гуглить 'монтаж лотка', "
        "но найдет его в твоем CSV, если он там есть.</i>\n\n"
        "<b>Удаление:</b> Нажми на слово, чтобы удалить."
    )

    reply_markup = stopwords_menu_keyboard(stopwords, page, total_pages)
    menu_msg = None

    if isinstance(event, Message):
        menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        try:
            await target_message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
            menu_msg = target_message
        except TelegramBadRequest:
            try:
                await target_message.delete()
            except TelegramBadRequest:
                pass
            menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")

    if menu_msg:
        await set_menu_message(menu_msg, state)
    await state.set_state(Admin.stop_words_menu)


@router.callback_query(F.data == "admin_consumables", Admin.menu)
async def show_consumables_menu(callback: CallbackQuery, state: FSMContext):
    await show_consumables_menu_helper(callback, state, page=1)


@router.callback_query(F.data.startswith("consumable_page_"), Admin.consumables_menu)
async def handle_consumable_page(callback: CallbackQuery, state: FSMContext):
    if callback.data == "consumable_page_info":
        await callback.answer(f"Текущая страница.")
        return

    page = int(callback.data.split("_")[2])
    await show_consumables_menu_helper(callback, state, page=page, edit=True)
    await callback.answer()


@router.callback_query(F.data.startswith("consumable_add_page_"), Admin.consumables_menu)
async def request_consumable_word(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[3])
    await state.update_data(consumable_page=page)

    await callback.message.delete()
    text = "Введите слово или фразу для добавления в список расходников (будут выделены красным в Excel):"

    menu_msg = await callback.message.answer(text, reply_markup=back_button("admin_consumables"))
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_consumable_add)


@router.message(Admin.awaiting_consumable_add, F.text)
async def save_consumable_word(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)
    word = message.text.lower().strip()

    if not word or len(word) < 2:
        msg = "Ошибка: Слишком короткое слово."
        asyncio.create_task(send_temp_notification(message, msg))

        data = await state.get_data()
        page = data.get("consumable_page", 1)
        await show_consumables_menu_helper(message, state, page=page)
        return

    data = await state.get_data()
    page = data.get("consumable_page", 1)

    async with async_session_factory() as session:
        exists = await session.execute(select(ConsumableWord).where(ConsumableWord.word == word))
        if not exists.scalar_one_or_none():
            session.add(ConsumableWord(word=word))
            await session.commit()
            asyncio.create_task(send_temp_notification(message, f"Расходник '{word}' добавлен."))
        else:
            asyncio.create_task(send_temp_notification(message, f"Слово '{word}' уже в списке."))

    await show_consumables_menu_helper(message, state, page=page)


@router.callback_query(F.data.startswith("consumable_delete_"), Admin.consumables_menu)
async def delete_consumable_word(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    word_id = int(parts[2])
    page = int(parts[4])

    async with async_session_factory() as session:
        word = await session.get(ConsumableWord, word_id)
        if word:
            await session.delete(word)
            await session.commit()
            asyncio.create_task(send_temp_notification(callback.message, f"Удалено: '{word.word}'"))

    await show_consumables_menu_helper(callback, state, page=page, edit=True)
    await callback.answer()


@router.callback_query(F.data == "admin_consumables", StateFilter(
    Admin.awaiting_consumable_add
))
async def back_to_consumables_menu(callback: CallbackQuery, state: FSMContext):
    await show_consumables_menu_helper(callback, state, page=1)


async def show_consumables_menu_helper(event: Message | CallbackQuery, state: FSMContext, page: int = 1,
                                       edit: bool = False):
    if isinstance(event, Message):
        target_message = event
    else:
        target_message = event.message

    async with async_session_factory() as session:
        total_count_result = await session.execute(select(func.count(ConsumableWord.id)))
        total_count = total_count_result.scalar_one()

        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        if total_pages == 0:
            total_pages = 1

        if page > total_pages:
            page = total_pages

        offset = (page - 1) * PAGE_SIZE
        result = await session.execute(
            select(ConsumableWord).order_by(ConsumableWord.word).offset(offset).limit(PAGE_SIZE)
        )
        words = result.scalars().all()

    text = (
        "<b>Управление расходными материалами</b>\n\n"
        "Слова из этого списка будут <b>выделены красным</b> в Excel-файле сметы.\n"
        "Бот ищет частичное совпадение (если добавить 'кабель', то 'кабель силовой' тоже станет красным)."
    )

    reply_markup = consumables_menu_keyboard(words, page, total_pages)
    menu_msg = None

    if isinstance(event, Message):
        menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        try:
            await target_message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
            menu_msg = target_message
        except TelegramBadRequest:
            try:
                await target_message.delete()
            except TelegramBadRequest:
                pass
            menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")

    if menu_msg:
        await set_menu_message(menu_msg, state)
    await state.set_state(Admin.consumables_menu)


@router.callback_query(F.data == "admin_section_titles", Admin.menu)
async def show_section_titles_menu(callback: CallbackQuery, state: FSMContext):
    await show_section_titles_menu_helper(callback, state, page=1)


@router.callback_query(F.data.startswith("section_title_page_"), Admin.section_titles_menu)
async def handle_section_title_page(callback: CallbackQuery, state: FSMContext):
    if callback.data == "section_title_page_info":
        await callback.answer(f"Текущая страница.")
        return

    page = int(callback.data.split("_")[3])
    await show_section_titles_menu_helper(callback, state, page=page, edit=True)
    await callback.answer()


@router.callback_query(F.data.startswith("section_title_add_page_"), Admin.section_titles_menu)
async def request_section_title(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[4])
    await state.update_data(section_title_page=page)

    await callback.message.delete()
    text = "Введите название раздела (заголовка), который нужно сохранять в таблице (например, 'Оборудование'):"

    menu_msg = await callback.message.answer(text, reply_markup=back_button("admin_section_titles"))
    await set_menu_message(menu_msg, state)
    await state.set_state(Admin.awaiting_section_title_add)


@router.message(Admin.awaiting_section_title_add, F.text)
async def save_section_title(message: Message, state: FSMContext, bot: Bot):
    await delete_user_message(message)
    await delete_previous_menu(state, bot, message.chat.id)
    title = message.text.strip()

    if not title or len(title) < 2:
        msg = "Ошибка: Слишком короткое название."
        asyncio.create_task(send_temp_notification(message, msg))

        data = await state.get_data()
        page = data.get("section_title_page", 1)
        await show_section_titles_menu_helper(message, state, page=page)
        return

    data = await state.get_data()
    page = data.get("section_title_page", 1)

    async with async_session_factory() as session:
        result = await session.execute(select(SectionTitle).where(func.lower(SectionTitle.title) == title.lower()))
        exists = result.scalar_one_or_none()

        if not exists:
            session.add(SectionTitle(title=title))
            await session.commit()
            await price_logic_instance.load_section_titles(session)
            asyncio.create_task(send_temp_notification(message, f"Раздел '{title}' добавлен."))
        else:
            asyncio.create_task(send_temp_notification(message, f"Раздел '{title}' уже в списке."))

    await show_section_titles_menu_helper(message, state, page=page)


@router.callback_query(F.data.startswith("section_title_delete_"), Admin.section_titles_menu)
async def delete_section_title(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    t_id = int(parts[3])
    page = int(parts[5])

    async with async_session_factory() as session:
        item = await session.get(SectionTitle, t_id)
        if item:
            await session.delete(item)
            await session.commit()
            await price_logic_instance.load_section_titles(session)
            asyncio.create_task(send_temp_notification(callback.message, f"Удалено: '{item.title}'"))

    await show_section_titles_menu_helper(callback, state, page=page, edit=True)
    await callback.answer()


@router.callback_query(F.data == "admin_section_titles", StateFilter(
    Admin.awaiting_section_title_add
))
async def back_to_section_titles_menu(callback: CallbackQuery, state: FSMContext):
    await show_section_titles_menu_helper(callback, state, page=1)


async def show_section_titles_menu_helper(event: Message | CallbackQuery, state: FSMContext, page: int = 1,
                                          edit: bool = False):
    if isinstance(event, Message):
        target_message = event
    else:
        target_message = event.message

    async with async_session_factory() as session:
        total_count_result = await session.execute(select(func.count(SectionTitle.id)))
        total_count = total_count_result.scalar_one()

        total_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        if total_pages == 0:
            total_pages = 1

        if page > total_pages:
            page = total_pages

        offset = (page - 1) * PAGE_SIZE
        result = await session.execute(
            select(SectionTitle).order_by(SectionTitle.title).offset(offset).limit(PAGE_SIZE)
        )
        titles = result.scalars().all()

    text = (
        "<b>Управление разделами (Заголовками)</b>\n\n"
        "Добавьте сюда названия разделов, которые нужно <b>сохранять</b> в смете, даже если у них нет количества и цены.\n"
        "<i>Пример: Оборудование, Материалы, Кабельная продукция.</i>\n\n"
        "В Excel они будут выделены серым цветом."
    )

    reply_markup = section_titles_menu_keyboard(titles, page, total_pages)
    menu_msg = None

    if isinstance(event, Message):
        menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        try:
            await target_message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
            menu_msg = target_message
        except TelegramBadRequest:
            try:
                await target_message.delete()
            except TelegramBadRequest:
                pass
            menu_msg = await target_message.answer(text, reply_markup=reply_markup, parse_mode="HTML")

    if menu_msg:
        await set_menu_message(menu_msg, state)
    await state.set_state(Admin.section_titles_menu)