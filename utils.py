import asyncio
import io
import re
import pdfplumber
import pandas as pd
import xlsxwriter
import base64
from typing import List, Dict, Optional
from aiogram import Bot
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest
from config import Config
from db import CalculationItem, PriceListItem
from services import ai_service_instance, price_logic_instance
import pymupdf


async def send_temp_notification(message: Message, text: str, delay: int = 5):
    try:
        notification = await message.answer(text)
        await asyncio.sleep(delay)
        await notification.delete()
    except TelegramBadRequest:
        pass


async def is_owner(message: Message, config: Config) -> bool:
    return message.from_user.id in config.bot.owner_ids


async def add_message_to_history(message: Message, state: FSMContext):
    data = await state.get_data()
    message_ids = data.get("message_ids", [])
    message_ids.append(message.message_id)
    await state.update_data(message_ids=message_ids)


async def set_menu_message(message: Message, state: FSMContext):
    await state.update_data(menu_message_id=message.message_id)


async def delete_previous_menu(state: FSMContext, bot: Bot, chat_id: int):
    data = await state.get_data()
    menu_message_id = data.get("menu_message_id")
    if menu_message_id:
        try:
            await bot.delete_message(chat_id, menu_message_id)
        except TelegramBadRequest:
            pass

    user_message_ids = data.get("message_ids", [])
    for msg_id in user_message_ids:
        try:
            await bot.delete_message(chat_id, msg_id)
        except TelegramBadRequest:
            pass

    await state.update_data(menu_message_id=None, message_ids=[])


async def delete_user_message(message: Message):
    try:
        await message.delete()
    except TelegramBadRequest:
        pass


def clean_pdf_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    return text.strip().replace("\n", " ")


async def extract_specification_tables(
        pdf_path: str,
        processing_msg: Optional[Message] = None,
        page_indices: Optional[List[int]] = None
) -> List[Dict]:
    total_pages = 0
    try:
        doc = pymupdf.open(pdf_path)
        total_pages = doc.page_count
        doc.close()
    except Exception as e:
        if processing_msg:
            try:
                await processing_msg.edit_text("Ошибка: Не могу открыть PDF. Файл поврежден или неверный формат.")
            except TelegramBadRequest:
                pass
        print(f"Failed to open PDF: {e}")
        return []

    if total_pages == 0:
        return []

    pages_to_process = page_indices if page_indices else list(range(total_pages))
    all_items = []

    for i, page_idx in enumerate(pages_to_process):
        current_page_num = page_idx + 1
        page_items = []

        if processing_msg:
            try:
                progress_percent = int(((i) / len(pages_to_process)) * 100)
                await processing_msg.edit_text(f"🤖 AI-распознавание страницы {current_page_num} ({progress_percent}%)")
            except TelegramBadRequest:
                pass

        ai_items = await _try_ocr_and_ai_extraction(pdf_path, [page_idx], None, total_pages)
        if ai_items:
            page_items.extend(ai_items)

        if page_items:
            all_items.extend(page_items)

    if not all_items and processing_msg:
        try:
            await processing_msg.edit_text("❌ Данные не найдены ни на одной из страниц")
        except TelegramBadRequest:
            pass

    return _deduplicate_items(all_items)


def create_calculation_excel(items: List[CalculationItem], total: float,
                             consumable_words: List[str] = None) -> io.BytesIO:
    consumables_set = set(w.lower() for w in (consumable_words or []))
    data = []
    position_counter = 1

    for i, item in enumerate(items):
        full_name = item.name
        if item.code and item.code.lower() not in item.name.lower():
            full_name = f"{item.name} {item.code}"

        qty_display = float(item.quantity)
        price_display = float(item.cost_per_unit)
        total_display = float(item.total_cost)

        is_section = (item.source == "section")

        if is_section:
            pos_display = ""
        else:
            pos_display = position_counter
            position_counter += 1

        data.append({
            "Позиция": pos_display,
            "Наименование и техническая характеристика": full_name,
            "Единица измерения": item.unit,
            "Количество": qty_display,
            "Стоимость ед монтажа": price_display,
            "Сумма монтажа": total_display,
            "Комментарий": "",
            "SourceRaw": item.source
        })

    df = pd.DataFrame(data)
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet('Смета')

        header_main_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#00B0F0',
            'font_size': 40,
            'border': 1
        })

        header_sub_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#BDE6F9',
            'font_size': 12,
            'border': 1
        })

        table_header_blue_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#BDE6F9',
            'border': 1
        })

        cell_format = workbook.add_format({
            'text_wrap': True,
            'valign': 'top',
            'border': 1
        })

        center_format = workbook.add_format({
            'align': 'center',
            'valign': 'top',
            'border': 1
        })

        currency_format = workbook.add_format({
            'num_format': '#,##0.00',
            'valign': 'top',
            'border': 1
        })

        yellow_format = workbook.add_format({
            'bg_color': '#FFFF00',
            'num_format': '#,##0.00',
            'valign': 'top',
            'border': 1
        })

        red_format = workbook.add_format({
            'bg_color': '#FFC7CE',
            'font_color': '#9C0006',
            'text_wrap': True,
            'valign': 'top',
            'border': 1
        })

        section_format = workbook.add_format({
            'bg_color': '#D9D9D9',
            'bold': True,
            'valign': 'center',
            'border': 1
        })

        current_row = 0

        worksheet.merge_range(current_row, 0, current_row + 4, 6, 'ООО "СИБ"', header_main_format)
        current_row += 5

        worksheet.merge_range(current_row, 0, current_row + 1, 6, 'КОММЕРЧЕСКОЕ ПРЕДЛОЖЕНИЕ', header_sub_format)
        current_row += 2

        worksheet.merge_range(current_row, 0, current_row + 2, 6, 'Контрагент', header_sub_format)
        current_row += 3

        columns = [
            "Позиция",
            "Наименование и техническая характеристика",
            "Единица измерения",
            "Количество",
            "Стоимость ед монтажа",
            "Сумма монтажа",
            "Комментарий"
        ]

        for col_num, value in enumerate(columns):
            worksheet.write(current_row, col_num, value, table_header_blue_format)

        current_row += 1

        worksheet.merge_range(current_row, 0, current_row + 1, 6, '', header_sub_format)
        current_row += 2

        worksheet.merge_range(current_row, 0, current_row + 1, 6, '', header_sub_format)
        current_row += 2

        start_data_row = current_row

        for i, row in enumerate(data):
            row_num = start_data_row + i

            price_style = currency_format
            name_style = cell_format

            source_raw = row.get("SourceRaw", "")

            name_lower = str(row["Наименование и техническая характеристика"]).lower()
            is_consumable = any(w in name_lower for w in consumables_set)

            if source_raw == "section":
                worksheet.write(row_num, 0, row["Позиция"], section_format)
                worksheet.write(row_num, 1, row["Наименование и техническая характеристика"], section_format)
                worksheet.write(row_num, 2, "", section_format)
                worksheet.write(row_num, 3, "", section_format)
                worksheet.write(row_num, 4, "", section_format)
                worksheet.write(row_num, 5, "", section_format)
                worksheet.write(row_num, 6, "", section_format)
                continue

            elif is_consumable:
                name_style = red_format
            elif row["Стоимость ед монтажа"] == 0:
                price_style = yellow_format

            worksheet.write(row_num, 0, row["Позиция"], center_format)
            worksheet.write(row_num, 1, row["Наименование и техническая характеристика"], name_style)
            worksheet.write(row_num, 2, row["Единица измерения"], center_format)
            worksheet.write(row_num, 3, row["Количество"], center_format)
            worksheet.write(row_num, 4, row["Стоимость ед монтажа"], price_style)

            formula = f'=D{row_num + 1}*E{row_num + 1}'
            worksheet.write_formula(row_num, 5, formula, currency_format)

            worksheet.write(row_num, 6, row["Комментарий"], cell_format)

        total_row = start_data_row + len(data)
        total_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'right'})
        total_val_fmt = workbook.add_format(
            {'bold': True, 'bg_color': '#D9E1F2', 'border': 1, 'num_format': '#,##0.00'})

        worksheet.merge_range(total_row, 0, total_row, 4, 'Итого:', total_fmt)
        worksheet.write_formula(total_row, 5, f'=SUM(F{start_data_row + 1}:F{total_row})', total_val_fmt)
        worksheet.write(total_row, 6, '', total_fmt)

        worksheet.set_column('A:A', 8)
        worksheet.set_column('B:B', 60)
        worksheet.set_column('C:C', 12)
        worksheet.set_column('D:D', 12)
        worksheet.set_column('E:F', 18)
        worksheet.set_column('G:G', 25)

    output.seek(0)
    return output


def parse_excel_for_update(file_path: str) -> List[Dict]:
    df_raw = pd.read_excel(file_path, sheet_name=0, header=None)

    header_row_idx = None

    target_col_name = "наименование"
    target_col_qty = "количество"

    for idx, row in df_raw.head(50).iterrows():
        row_str = " ".join([str(v).lower() for v in row.tolist()])
        if target_col_name in row_str and (target_col_qty in row_str or "кол-во" in row_str):
            header_row_idx = idx
            break

    if header_row_idx is None:
        raise ValueError("Не найдены заголовки таблицы (Наименование, Количество).")

    df = pd.read_excel(file_path, sheet_name=0, header=header_row_idx)

    df.columns = [str(c).strip().lower() for c in df.columns]

    updates = []

    col_pos = None
    col_name = None
    col_qty = None
    col_price = None
    col_unit = None

    for c in df.columns:
        if "наименование" in c:
            col_name = c
        elif "количество" in c or "кол-во" in c:
            col_qty = c
        elif "стоимость ед" in c or "цена" in c:
            col_price = c
        elif "единица" in c or "ед. изм" in c:
            col_unit = c
        elif "позиция" in c or "№" in c or "no" in c:
            col_pos = c

    if not col_name or not col_qty:
        return []

    for index, row in df.iterrows():
        try:
            name_val = row[col_name]
            if pd.isna(name_val): continue
            name = str(name_val).strip()
            if not name or name.lower() == 'nan': continue

            def safe_float(val):
                if pd.isna(val): return 0.0
                s = str(val).replace(',', '.').replace('\xa0', '').strip()
                if not s or s.lower() == 'nan': return 0.0
                try:
                    return float(s)
                except:
                    return 0.0

            quantity = safe_float(row[col_qty])
            cost_per_unit = 0.0
            if col_price:
                cost_per_unit = safe_float(row[col_price])

            unit = "-"
            if col_unit and not pd.isna(row[col_unit]):
                unit = str(row[col_unit]).strip()

            position = None
            if col_pos:
                try:
                    val = str(row[col_pos]).split('.')[0].strip()
                    if val.isdigit():
                        position = int(val)
                except:
                    position = None

            updates.append({
                "position": position,
                "name": name,
                "quantity": quantity,
                "cost_per_unit": cost_per_unit,
                "unit": unit
            })

        except Exception:
            continue

    return updates


def create_pricelist_excel(items: List[PriceListItem]) -> io.BytesIO:
    data = []
    for i, item in enumerate(items):
        data.append({
            "№": i + 1,
            "Наименование": item.name,
            "Цена": item.price,
        })

    df = pd.DataFrame(data)
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Прайс-лист', index=False, startrow=0)

        workbook = writer.book
        worksheet = writer.sheets['Прайс-лист']

        worksheet.set_column('B:B', 60)
        worksheet.set_column('C:C', 15)

    output.seek(0)
    return output


def parse_spec_excel_for_creation(file_path: str) -> List[Dict]:
    spec_items = []

    try:
        xls_dict = pd.read_excel(file_path, sheet_name=None, header=None)
    except Exception as e:
        raise ValueError(f"Не удалось прочитать Excel файл: {e}")

    saved_name_idx = None
    saved_qty_idx = None
    saved_unit_idx = None
    saved_code_idx = None
    saved_mass_idx = None

    settings = ai_service_instance

    for sheet_name, df in xls_dict.items():
        if df.empty:
            continue

        header_row_idx = _find_header_row(df)

        current_name_idx = None
        current_qty_idx = None
        current_unit_idx = None
        current_code_idx = None
        current_mass_idx = None
        start_row = 0

        if header_row_idx is not None:
            header_values = [str(x) for x in df.iloc[header_row_idx]]
            current_name_idx = _find_col_index(header_values, settings.parse_name_keys)
            current_qty_idx = _find_col_index(header_values, settings.parse_qty_keys)
            current_unit_idx = _find_col_index(header_values, settings.parse_unit_keys)
            current_code_idx = _find_col_index(header_values, settings.parse_code_keys)
            current_mass_idx = _find_col_index(header_values, settings.parse_mass_keys)

            saved_name_idx, saved_qty_idx, saved_unit_idx = current_name_idx, current_qty_idx, current_unit_idx
            saved_code_idx, saved_mass_idx = current_code_idx, current_mass_idx

            start_row = header_row_idx + 1
        elif saved_name_idx is not None:
            current_name_idx = saved_name_idx
            current_qty_idx = saved_qty_idx
            current_unit_idx = saved_unit_idx
            current_code_idx = saved_code_idx
            current_mass_idx = saved_mass_idx
            start_row = 0
        else:
            continue

        if current_name_idx is None or current_qty_idx is None:
            continue

        for i in range(start_row, len(df)):
            row = df.iloc[i]
            try:
                if current_name_idx >= len(row): continue
                name = str(row.iloc[current_name_idx]).strip()

                if not name or name.lower() in ['nan', 'none', '', '0']:
                    continue

                if name.isdigit() and len(name) < 5:
                    continue

                if current_qty_idx >= len(row): continue
                qty_raw = str(row.iloc[current_qty_idx])
                qty_clean = re.sub(r'[^\d,.-]', '', qty_raw).replace(',', '.')
                try:
                    quantity = float(qty_clean)
                except ValueError:
                    continue

                if quantity <= 0:
                    continue

                unit = "шт"
                if current_unit_idx is not None and current_unit_idx < len(row):
                    u_val = str(row.iloc[current_unit_idx]).strip()
                    if u_val and u_val.lower() not in ['nan', 'none']:
                        unit = u_val

                code = ""
                if current_code_idx is not None and current_code_idx < len(row):
                    c_val = str(row.iloc[current_code_idx]).strip()
                    if c_val and c_val.lower() not in ['nan', 'none']:
                        code = c_val

                mass = 0.0
                if current_mass_idx is not None and current_mass_idx < len(row):
                    m_raw = str(row.iloc[current_mass_idx])
                    m_clean = re.sub(r'[^\d,.]', '', m_raw).replace(',', '.')
                    try:
                        mass = float(m_clean)
                    except ValueError:
                        mass = 0.0

                spec_items.append({
                    "name": name,
                    "quantity": quantity,
                    "unit": unit,
                    "code": code,
                    "mass": mass
                })

            except Exception as e:
                print(f"Skipping excel row {i} on sheet {sheet_name}: {e}")
                continue

    if not spec_items:
        raise ValueError("Файл Excel пуст, данные не найдены или структура таблицы некорректна.")

    return _deduplicate_items(spec_items)


async def _try_ocr_and_ai_extraction(
        pdf_path: str,
        page_indices: List[int],
        processing_msg: Optional[Message],
        total_pages: int
) -> List[Dict]:
    try:
        doc = pymupdf.open(pdf_path)
        all_results = []

        for page_index in page_indices:
            try:
                page = doc.load_page(page_index)
                pix = page.get_pixmap(dpi=300)
                img_bytes = pix.tobytes("png")
                base64_image = base64.b64encode(img_bytes).decode('utf-8')

                page_results = await ai_service_instance.parse_specification_from_image(base64_image)
                if page_results:
                    all_results.extend(page_results)
            except Exception as e:
                print(f"Error processing AI page {page_index}: {e}")

        doc.close()
        return all_results
    except Exception as e:
        print(f"AI extraction failed: {e}")
        return []


def _find_col_index(search_list: List[str], keywords: List[str]) -> Optional[int]:
    for i, cell in enumerate(search_list):
        cell_str = str(cell).lower().strip()
        cell_str_nospace = cell_str.replace(" ", "")

        for kw in keywords:
            if kw in cell_str or kw.replace(" ", "") in cell_str_nospace:
                return i
    return None


def _remove_footer_garbage(text: str) -> str:
    text = re.sub(r'AC-MSR-[A-Za-z0-9\-\.\sа-яА-Я]+', '', text)
    text = re.sub(r'(?i)\s+Р\s+\d*\s*Спецификация.*', '', text)
    text = re.sub(r'(?i)(стадия|лист|листов|формат)\s*([0-9ap]+)?', '', text)
    text = re.sub(r'(?i)archi\s*comm', '', text)
    text = re.sub(r'[0-9]{2}\.[0-9]{2}\.[0-9]{2,4}', '', text)
    text = re.sub(r'\s+\d+[a-z]\s*$', '', text)
    return text.strip()


def _parse_dataframe_to_items(df: pd.DataFrame) -> List[Dict]:
    items = []
    if df.empty: return items

    settings = ai_service_instance
    df = df.fillna("")

    header_row_idx = _find_header_row(df)

    name_idx = None
    qty_idx = None
    unit_idx = None
    code_idx = None
    mass_idx = None

    data_start_idx = 0

    if header_row_idx is not None:
        data_start_idx = header_row_idx + 1
        header_vals = [str(x) for x in df.iloc[header_row_idx]]

        name_idx = _find_col_index(header_vals, settings.parse_name_keys)
        qty_idx = _find_col_index(header_vals, settings.parse_qty_keys)
        unit_idx = _find_col_index(header_vals, settings.parse_unit_keys)
        code_idx = _find_col_index(header_vals, settings.parse_code_keys)
        mass_idx = _find_col_index(header_vals, settings.parse_mass_keys)

        if header_row_idx + 1 < len(df):
            next_row_vals = [str(x) for x in df.iloc[header_row_idx + 1]]
            if _find_col_index(next_row_vals, settings.parse_qty_keys) is not None:
                data_start_idx += 1

    if name_idx is None:
        max_avg_len = 0
        best_col = None
        for c in range(len(df.columns)):
            col_vals = df.iloc[data_start_idx:data_start_idx + 20, c].astype(str)
            valid_vals = [x for x in col_vals if x.strip() and x.lower() not in ['nan', 'none']]
            if not valid_vals: continue

            has_letters = any(re.search(r'[a-zA-Zа-яА-Я]', v) for v in valid_vals)
            if not has_letters: continue

            avg_len = sum(len(x) for x in valid_vals) / len(valid_vals)
            if avg_len > max_avg_len and avg_len > 5:
                max_avg_len = avg_len
                best_col = c
        name_idx = best_col

    valid_units_anchor = ['шт', 'м', 'м.', 'уп', 'компл', 'кг', 'км', 'пог.м', 'набор', 'бухта', 'л.', 'm', 'mtr', 'pc',
                          'pcs', 'баллон', 'пач', 'кор']

    GARBAGE_PHRASES = [
        "изм.", "кол.уч", "лист", "№док", "подп.", "дата",
        "формат а", "инв. №", "взам. инв.", "стадия",
        "разработал", "проверил", "н.контр", "гип",
        "копировал", "archi comm", "согласовано",
        "заказчик", "подрядчик", "генеральный директор", "ген.директор",
        "индивидуальный предприниматель", "адрес объекта", "капитальный ремонт",
        "гбу до", "мксшор", "экспликация помещений", "ведомость рабочих",
        "общие данные", "условно-графические", "схема подключения",
        "наименование", "наименовани", "код продукции", "код продукци", "поставщик",
        "единица измерения", "единица измерени", "кол-во", "кол во", "масса", "масс единиц",
        "примечания", "примечани", "позиция", "позици", "тип, марка",
        "обозначение", "обозначени", "трасса", "способ прокладки", "кабель, провод",
        "опросног", "опросного", "спецификация оборудования",
        "коли-во", "коли", "оставщи", "единицы"
    ]

    current_item = None

    for idx in range(data_start_idx, len(df)):
        row = df.iloc[idx]
        row_values_str = [str(val).strip() for val in row if str(val).lower() not in ['nan', 'none', '']]
        full_row_text = " ".join(row_values_str)
        full_row_text_lower = full_row_text.lower()

        if not full_row_text or len(full_row_text) < 2: continue
        if re.match(r'^[\d\s\.\-]+$', full_row_text) and len(full_row_text.split()) > 3: continue

        is_garbage = False
        for bad in GARBAGE_PHRASES:
            if bad in full_row_text_lower:
                is_garbage = True
                break
        if is_garbage: continue

        pos_num_val = ""
        if len(row) > 0:
            first_col = str(row.iloc[0]).strip()
            if re.fullmatch(r'^\d+(\.\d+)*\.?$', first_col):
                pos_num_val = first_col

        name_val = ""
        if name_idx is not None and name_idx < len(row):
            name_val = str(row.iloc[name_idx]).strip()
            if name_val.lower() in ['nan', 'none']: name_val = ""

        code_val = ""
        if code_idx is not None and code_idx < len(row):
            code_val = str(row.iloc[code_idx]).strip()
            if code_val.lower() in ['nan', 'none']: code_val = ""

        qty_val = 0.0
        if qty_idx is not None and qty_idx < len(row):
            q_str = str(row.iloc[qty_idx])
            qty_val = extract_qty_val(q_str)

        if qty_val > 0 and pos_num_val:
            try:
                if abs(qty_val - float(pos_num_val)) < 0.001:
                    qty_val = 0.0
            except ValueError:
                pass

        unit_val = ""
        if unit_idx is not None and unit_idx < len(row):
            u_cand = str(row.iloc[unit_idx]).strip()
            if u_cand and not re.match(r'^[\d\.\,]+$', u_cand) and len(u_cand) < 10:
                unit_val = u_cand

        if not unit_val:
            for c in range(len(row)):
                if c == name_idx or c == code_idx: continue
                val = str(row.iloc[c]).lower().strip().replace(' ', '').rstrip('.')
                if val in valid_units_anchor:
                    unit_val = str(row.iloc[c]).strip()
                    break

        candidate_name = name_val if name_val else full_row_text

        candidate_name = re.sub(r'^\d+(\.\d+)*\.?\s+', '', candidate_name)
        candidate_name = _remove_footer_garbage(candidate_name)

        if re.fullmatch(r'^\d+(\.\d+)*\.?$', candidate_name.strip()):
            candidate_name = ""

        if qty_val == 0:
            units_regex = r'|'.join(re.escape(u) for u in valid_units_anchor)
            tail_match = re.search(r'(?i)(\s|^)(' + units_regex + r')[\.\s]+(\d+([\.,]\d+)?)\s*$', full_row_text)

            if tail_match:
                try:
                    num_str = tail_match.group(3).replace(',', '.')
                    found_qty = float(num_str)
                    found_unit = tail_match.group(2)

                    is_pos_match = False
                    if pos_num_val:
                        try:
                            if abs(found_qty - float(pos_num_val)) < 0.001:
                                is_pos_match = True
                        except:
                            pass

                    if 0 < found_qty < 100000 and not is_pos_match:
                        qty_val = found_qty
                        if found_unit and not unit_val:
                            unit_val = found_unit

                        match_start = tail_match.start()
                        if not name_val:
                            candidate_name = full_row_text[:match_start].strip()
                            candidate_name = re.sub(r'^\d+(\.\d+)*\.?\s+', '', candidate_name)
                            candidate_name = _remove_footer_garbage(candidate_name)
                except:
                    pass

        is_section = False
        section_name = ""

        clean_text_no_num = re.sub(r'^\d+\.\s*', '', full_row_text)

        if price_logic_instance.is_section_title(full_row_text) or \
                price_logic_instance.is_section_title(candidate_name) or \
                price_logic_instance.is_section_title(clean_text_no_num):
            is_section = True
            section_name = candidate_name if candidate_name else full_row_text
            qty_val = 0.0

        if unit_val or qty_val > 0:
            is_section = False

        if is_section:
            if current_item:
                items.append(current_item)
                current_item = None
            items.append({
                'name': section_name,
                'quantity': 0.0,
                'unit': "",
                'code': "",
                'mass': 0.0,
                'source': 'section'
            })
            continue

        is_new_item = False

        is_code_like = False
        if len(candidate_name) > 3:
            ru_chars = len(re.findall(r'[а-яА-Я]', candidate_name))
            total_chars = len(candidate_name)
            if ru_chars == 0 or (ru_chars / total_chars < 0.2):
                if re.search(r'[a-zA-Z0-9]', candidate_name):
                    is_code_like = True

        if qty_val > 0:
            if is_code_like and current_item and current_item['quantity'] == qty_val and current_item[
                'source'] != 'section':
                if not current_item['code']:
                    current_item['code'] = candidate_name
                else:
                    current_item['code'] += " " + candidate_name
            else:
                is_new_item = True

        elif pos_num_val:
            is_new_item = True

        elif current_item is None:
            if candidate_name and len(candidate_name) > 2 and not is_code_like:
                is_new_item = True

        if is_new_item:
            if current_item:
                items.append(current_item)

            current_item = {
                'name': candidate_name,
                'quantity': qty_val,
                'unit': unit_val if unit_val else "шт",
                'code': code_val,
                'mass': 0.0
            }
        else:
            if current_item and current_item.get('source') != 'section':
                if candidate_name:
                    is_part_garbage = False
                    for bad in GARBAGE_PHRASES:
                        if bad in candidate_name.lower():
                            is_part_garbage = True
                            break

                    if not is_part_garbage:
                        if qty_val == 0:
                            if is_code_like:
                                if not current_item['code']:
                                    current_item['code'] = candidate_name
                                else:
                                    current_item['code'] += " " + candidate_name
                            else:
                                current_item['name'] += " " + candidate_name

                        if unit_val and not current_item['unit']:
                            current_item['unit'] = unit_val

    if current_item:
        items.append(current_item)

    for item in items:
        item['name'] = re.sub(r'\s+', ' ', item['name']).strip()
        item['code'] = re.sub(r'\s+', ' ', item['code']).strip()

    return items


def extract_qty_val(val: str) -> float:
    try:
        val_clean = str(val).strip().replace('\xa0', '').replace(',', '.')
        if not val_clean: return 0.0

        val_lower = val_clean.lower()
        allowed_words = ['шт', 'м', 'уп', 'компл', 'ед', 'x', 'х', 'm', 'бухта', 'баллон', 'пач', 'кор']

        letter_count = sum(1 for c in val_lower if c.isalpha())
        if letter_count > 3:
            is_pure_unit = any(w in val_lower for w in allowed_words)
            if not is_pure_unit:
                return 0.0

        if '\n' in val_clean:
            parts = val_clean.split()
            for part in parts:
                if re.match(r'^\d+(\.\d+)?$', part):
                    return float(part)

        nums = re.findall(r"[-+]?\d*\.?\d+", val_clean)
        if nums:
            if len(nums) == 1:
                return float(nums[0])

            for n in reversed(nums):
                try:
                    f = float(n)
                    if f > 0: return f
                except:
                    continue

            return float(nums[-1])

        return 0.0
    except:
        return 0.0


def _find_header_row(df: pd.DataFrame) -> Optional[int]:
    settings = ai_service_instance
    all_keywords = (
            settings.parse_name_keys +
            settings.parse_qty_keys +
            settings.parse_unit_keys +
            settings.parse_code_keys
    )

    limit = min(30, len(df))

    best_idx = None
    max_matches = 0

    for idx in range(limit):
        row = df.iloc[idx]
        row_values = [str(cell).lower().strip() for cell in row]
        row_str = ' '.join(row_values)

        matches = 0
        for kw in all_keywords:
            if kw in row_str:
                matches += 1

        if matches > max_matches and matches >= 2:
            max_matches = matches
            best_idx = idx

    if best_idx is None:
        required = settings.parse_name_keys
        for idx in range(limit):
            row = df.iloc[idx]
            row_str = ' '.join(str(cell).lower() for cell in row)
            if any(kw in row_str for kw in required):
                if len(str(row_str)) > 10:
                    return idx

    return best_idx


def _find_column(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    for col in df.columns:
        col_str = str(col).lower().strip()
        for kw in keywords:
            if kw in col_str:
                return col
    return None


def _deduplicate_items(items: List[Dict]) -> List[Dict]:
    merged_list = []
    product_indices = []
    for item in items:
        source = item.get('source', '')
        if source == 'section':
            merged_list.append(item)
            continue
        name_clean = re.sub(r'[\W_]+', '', str(item.get('name', ''))).lower()
        code_clean = re.sub(r'[\W_]+', '', str(item.get('code', ''))).lower()
        qty = float(item.get('quantity', 0.0))
        is_duplicate = False
        for idx in reversed(product_indices):
            existing = merged_list[idx]
            ex_name_clean = re.sub(r'[\W_]+', '', str(existing.get('name', ''))).lower()
            ex_code_clean = re.sub(r'[\W_]+', '', str(existing.get('code', ''))).lower()
            match = False
            if code_clean and ex_code_clean:
                if code_clean == ex_code_clean:
                    match = True
            elif not code_clean and not ex_code_clean:
                if name_clean == ex_name_clean:
                    match = True
            if match:
                existing['quantity'] = float(existing.get('quantity', 0.0)) + qty
                if len(item['name']) > len(existing['name']):
                    existing['name'] = item['name']
                if len(item['code']) > len(existing['code']):
                    existing['code'] = item['code']
                is_duplicate = True
                break
        if not is_duplicate:
            merged_list.append(item)
            product_indices.append(len(merged_list) - 1)
    final_list = []
    pending_section = None
    for item in merged_list:
        if item.get('source') == 'section':
            pending_section = item
        else:
            if pending_section:
                should_add = True
                if final_list and final_list[-1].get('source') == 'section':
                    last_title = re.sub(r'[\W_]+', '', final_list[-1]['name']).lower()
                    curr_title = re.sub(r'[\W_]+', '', pending_section['name']).lower()
                    if last_title == curr_title:
                        should_add = False
                if should_add:
                    final_list.append(pending_section)
                pending_section = None
            final_list.append(item)
    return final_list