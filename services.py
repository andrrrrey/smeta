import json
import chromadb
import asyncio
import re
import io
import pdfplumber
import google.generativeai as genai
from google.api_core.exceptions import GoogleAPIError
from chromadb.utils import embedding_functions
from openai import AsyncOpenAI, OpenAIError
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from fuzzywuzzy import process, fuzz
from typing import List, Dict, Optional, Tuple
from aiogram import Bot
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest
import base64

from config import load_config, Config
from db import BotSettings, StopWord, PriceListItem, Calculation, CalculationItem


async def _notify_owners_internal(bot: Bot, config: Config, text: str):
    if not bot or not config:
        return
    for owner_id in config.bot.owner_ids:
        try:
            await bot.send_message(owner_id, f"🚨 <b>Ошибка:</b>\n{text}", parse_mode="HTML")
        except Exception:
            pass

class VectorDB:
    def __init__(self, config: Config, api_key: Optional[str] = None):
        self.client = chromadb.PersistentClient(path=config.vector_db.path)
        self.collection_name = config.vector_db.collection_name
        self.openai_ef = None
        if api_key:
            self.update_api_key(api_key)

    def update_api_key(self, api_key: str):
        self.openai_ef = embedding_functions.OpenAIEmbeddingFunction(
            api_key=api_key, model_name="text-embedding-ada-002"
        )
        self.collection = self.client.get_or_create_collection(
            name=self.collection_name, embedding_function=self.openai_ef
        )

    async def add_documents(self, docs: List[str], metadatas: List[dict], ids: List[str]):
        if not self.openai_ef:
            raise ValueError("OpenAI API key not set for vector DB embeddings.")
        self.collection.add(documents=docs, metadatas=metadatas, ids=ids)

    async def query(self, query_text: str, n_results: int = 3, bot: Optional[Bot] = None, config: Optional[Config] = None) -> List[dict]:
        if not self.openai_ef:
            return []
        try:
            results = self.collection.query(query_texts=[query_text], n_results=n_results)
            documents = results.get('documents', [[]])[0]
            return documents if documents else []
        except Exception as e:
            err_str = str(e)
            print(f"Error querying Chroma: {e}")
            if "429" in err_str or "quota" in err_str.lower() or "billing" in err_str.lower():
                if bot and config:
                    await _notify_owners_internal(bot, config, f"OpenAI API Error (RAG Query):\n{err_str}")
                raise ValueError("ChromaDB query failed. Check OpenAI API key or billing.")
            return []

    async def list_document_sources(self) -> List[str]:
        if not self.openai_ef:
            return []
        try:
            metadatas_result = self.collection.get(include=["metadatas"])
            all_metadatas = metadatas_result.get("metadatas", [])
            sources = {meta.get("source") for meta in all_metadatas if meta and meta.get("source")}
            return sorted(list(sources))
        except Exception as e:
            print(f"Error listing RAG docs: {e}")
            return []

    async def delete_documents(self, ids: List[str]):
        if not self.openai_ef:
            return
        if not ids:
            return
        try:
            self.collection.delete(ids=ids)
        except Exception as e:
            print(f"Error deleting by ids: {e}")

    async def delete_documents_by_source(self, source_filename: str):
        if not self.openai_ef:
            return
        try:
            results = self.collection.get(where={"source": source_filename}, include=["metadatas"])
            ids_to_delete = results.get('ids')
            if ids_to_delete:
                self.collection.delete(ids=ids_to_delete)
        except Exception as e:
            print(f"Error deleting by source '{source_filename}': {e}")


class AIService:
    def __init__(self, vdb_instance: VectorDB):
        self.openai_client: Optional[AsyncOpenAI] = None
        self.model: str = "gpt-4o"
        self.system_prompt: str = "Ты — ИИ-ассистент для расчета смет."
        self.vector_db = vdb_instance

        self.parse_name_keys: List[str] = ['наименование', 'название', 'name', 'описание']
        self.parse_qty_keys: List[str] = ['кол-во', 'количество', 'кол', 'qty', 'quantity', 'к-во']
        self.parse_unit_keys: List[str] = ['ед.', 'ед. изм', 'единица', 'unit', 'изм']
        self.parse_code_keys: List[str] = ['марка', 'код', 'тип', 'обозначение', 'code', 'артикул']
        self.parse_mass_keys: List[str] = ['масса', 'вес', 'mass', 'weight']
        self.parse_note_keys: List[str] = ['примечание', 'примечания', 'note']

    async def update_settings(self, settings: BotSettings):
        if settings.openai_api_key:
            self.openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
            try:
                self.vector_db.update_api_key(settings.openai_api_key)
            except Exception as e:
                print(f"Failed to update VectorDB API key: {e}")
        else:
            self.openai_client = None
            print("Warning: OpenAI API key is not set. RAG (VectorDB) and Whisper will not work.")

        self.model = settings.ai_model or "gpt-4o"
        self.system_prompt = settings.system_prompt or self.system_prompt

        def _load_keys(db_field: Optional[str], default: List[str]) -> List[str]:
            if db_field:
                keys = [k.strip().lower() for k in db_field.split(',') if k.strip()]
                return keys if keys else default
            return default

        self.parse_name_keys = _load_keys(getattr(settings, 'parse_name_keys', None), self.parse_name_keys)
        self.parse_qty_keys = _load_keys(getattr(settings, 'parse_qty_keys', None), self.parse_qty_keys)
        self.parse_unit_keys = _load_keys(getattr(settings, 'parse_unit_keys', None), self.parse_unit_keys)
        self.parse_code_keys = _load_keys(getattr(settings, 'parse_code_keys', None), self.parse_code_keys)
        self.parse_mass_keys = _load_keys(getattr(settings, 'parse_mass_keys', None), self.parse_mass_keys)
        self.parse_note_keys = _load_keys(getattr(settings, 'parse_note_keys', None), self.parse_note_keys)

        if not self.model:
            self.model = "gpt-4o"

        print(f"AIService updated. Active Model: {self.model}")

    def _get_client(self) -> Optional[AsyncOpenAI]:
        return self.openai_client

    async def get_internet_price_and_rag(self, item_name: str) -> Dict:
        client = self._get_client()
        if not client:
            return {"price": 0.0, "source": "not_found"}

        rag_info = []
        try:
            rag_query_prefix = "Стоимость оборудования"
            if "монтаж" in self.system_prompt.lower() or "работ" in self.system_prompt.lower():
                rag_query_prefix = "Стоимость монтажа"

            rag_info = await self.vector_db.query(f"{rag_query_prefix} {item_name}")
        except ValueError as e:
            print(e)
            if "billing" in str(e).lower():
                return {"price": 0.0, "source": "not_found"}

        context = "\n".join(rag_info) if rag_info else "Нет данных из RAG."

        user_prompt = f"""
        [РОЛЬ] Ты — ИИ-ассистент, эксперт по составлению смет на монтажные работы.
        [ЗАДАЧА] Определи среднерыночную цену *монтажных работ* для ОДНОЙ позиции.

        [ВХОДНЫЕ ДАННЫЕ]
        1. [ITEM_TO_PRICE]: "{item_name}"
        2. [RAG_CONTEXT]: {context} (Контекст из базы знаний)

        [ПРАВИЛА ОПРЕДЕЛЕНИЯ ЦЕНЫ]
        1.  **ПОИСК В БАЗЕ ЗНАНИЙ (RAG):**
            Сначала ищи релевантную цену в [RAG_CONTEXT].
            Если нашел: верни {{"price": <найденная_цена_из_RAG>, "source": "rag"}}

        2.  **ПОИСK В ИНТЕРНЕТЕ:**
            Если в RAG нет, используй свои знания (интернет), чтобы дать *среднерыночную цену* на *МОНТАЖ* [ITEM_TO_PRICE] в России 2025.
            Если нашел: верни {{"price": <среднерыночная_цена_из_интернета>, "source": "internet"}}

        3.  **ПРОВАЛ:**
            Если цена не найдена нигде, верни: {{"price": 0.0, "source": "not_found"}}

        [ФОРМАТ ОТВЕТА]
        * Ответь **ТОЛЬКО** валидным JSON-объектом.
        * Никакого текста до или после JSON.
        * Формат: {{"price": (float), "source": "(string)"}}
        """

        content = None
        try:
            model_name_only = self.model.split('@')[0]
            response = await client.chat.completions.create(
                model=model_name_only,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            content = response.choices[0].message.content
            data = json.loads(content)

            price = float(data.get("price", 0.0))
            source = str(data.get("source", "not_found"))
            return {"price": price, "source": source}

        except OpenAIError as e:
            print(f"API error ({model_name_only}): {e}")
            return {"price": 0.0, "source": "not_found"}
        except json.JSONDecodeError as e:
            print(f"AI JSON decode error: {e}. Response was: {content}")
            return {"price": 0.0, "source": "not_found"}
        except Exception as e:
            print(f"Unexpected error in get_internet_price_and_rag: {e}")
            return {"price": 0.0, "source": "not_found"}

    async def get_price_with_analog_search(self, item_name: str, item_code: Optional[str],
                                           pricelist_cache: Dict[str, float]) -> Dict:
        client = self._get_client()
        if not client:
            return {"price": 0.0, "source": "not_found"}

        all_rag_results = set()
        try:
            prefixes = ["Стоимость монтажа", "Стоимость оборудования"]
            queries_to_run = set()

            queries_to_run.add(f"{prefixes[0]} {item_name}")
            queries_to_run.add(f"{prefixes[1]} {item_name}")
            queries_to_run.add(f"{item_name}")

            if item_code and item_code.strip() and item_code.lower() != item_name.lower():
                queries_to_run.add(f"{prefixes[0]} {item_code}")
                queries_to_run.add(f"{prefixes[1]} {item_code}")
                queries_to_run.add(f"{item_code}")

            if item_code and item_code.strip() and item_code.lower() in item_name.lower():
                name_part = item_name.lower().replace(item_code.lower(), "").strip()
                if len(name_part) > 3:
                    queries_to_run.add(f"{prefixes[0]} {name_part}")
                    queries_to_run.add(f"{prefixes[1]} {name_part}")

            name_parts = item_name.split()
            if len(name_parts) > 1:
                short_name_1 = name_parts[0]
                queries_to_run.add(f"{prefixes[0]} {short_name_1}")
                queries_to_run.add(f"{prefixes[1]} {short_name_1}")
                queries_to_run.add(f"{short_name_1}")

            if len(name_parts) > 2:
                short_name_2 = ' '.join(name_parts[:2])
                queries_to_run.add(f"{prefixes[0]} {short_name_2}")
                queries_to_run.add(f"{prefixes[1]} {short_name_2}")
                queries_to_run.add(f"{short_name_2}")

            for query_text in queries_to_run:
                rag_docs = await self.vector_db.query(query_text, n_results=10)
                all_rag_results.update(rag_docs)

        except ValueError as e:
            print(e)
            if "billing" in str(e).lower():
                return {"price": 0.0, "source": "not_found"}

        rag_context = "\n".join(list(all_rag_results)) if all_rag_results else "Нет данных из RAG."

        candidates = process.extractBests(
            item_name,
            pricelist_cache.keys(),
            scorer=fuzz.token_set_ratio,
            score_cutoff=70,
            limit=10
        )
        # Pass work prices to AI (it determines installation/labor costs)
        pricelist_context = {
            name: pricelist_cache[name]["work"] if isinstance(pricelist_cache[name], dict) else pricelist_cache[name]
            for name, score in candidates
        }
        pricelist_context_str = json.dumps(pricelist_context,
                                           ensure_ascii=False) if pricelist_context else "Нет аналогов в прайс-листе."

        item_code_str = item_code if item_code and item_code.strip() else "Не указан"

        user_prompt = f"""
        [ЗАДАЧА] Определи цену для ОДНОЙ позиции, следуя инструкциям из СИСТЕМНОГО промпта.

        [КРИТИЧЕСКОЕ ПРАВИЛО АНАЛОГИЙ]
        * Твой СИСТЕМНЫЙ промпт требует, чтобы ты находил аналоги. Делай это!
        * Пример 1: "IP-камера купольная" и "Видеокамера IP цилиндрическая" — это ОДНА категория ("Видеокамера").
        * Пример 2: "NVR-50" — это "Видеорегистратор".
        * Используй [RAG_CONTEXT] и [PRICELIST_ANALOGS] **агрессивно**. Если ты видишь аналог по категории (даже если модель/тип (купольная/цилиндрическая) немного отличается) — **ИСПОЛЬЗУЙ ЕГО ЦЕНУ**.
        * Не отвечай "not_found", если в контексте есть позиция той же *категории*.

        [ПРАВИЛО ОБЩИХ ЗНАНИЙ (ИНТЕРНЕТ)]
        * Если [RAG_CONTEXT] и [PRICELIST_ANALOGS] не помогли, **ИСПОЛЬЗУЙ СВОИ ОБЩИЕ ЗНАНИЯ (Интернет)**, чтобы дать среднерыночную цену.
        * Нельзя возвращать 0.0 для **обычных** позиций, таких как 'Видеокамера' или 'Коммутатор'. Ты *знаешь* примерную цену их монтажа.
        * Если используешь "internet", ты **ОБЯЗАН** заполнить поле "explanation".

        [ВХОДНЫЕ ДАННЫЕ]
        1. [ITEM_TO_PRICE_NAME]: "{item_name}"
        2. [ITEM_TO_PRICE_CODE]: "{item_code_str}"

        [КОНТЕКСТ ДЛЯ ПОИСКА]
        1. [RAG_CONTEXT]: {rag_context}
        2. [PRICELIST_ANALOGS]: {pricelist_context_str}

        [ФОРМАТ ОТВЕТА]
        * Ответь **ТОЛЬКО** валидным JSON-объектом: {{"price": (float), "source": "(string)", "explanation": "(string)"}}
        * "source": "rag", "internal", "internet" или "not_found".
        * "explanation": (string) **ОБЯЗАТЕЛЬНО** заполни, если source="internet". (Пример: "Среднерыночная цена монтажа видеокамеры"). Если source не "internet", оставь "".
        """

        content = None
        try:
            model_name_only = self.model.split('@')[0]
            response = await client.chat.completions.create(
                model=model_name_only,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            content = response.choices[0].message.content
            data = json.loads(content)

            price = float(data.get("price", 0.0))
            source = str(data.get("source", "not_found"))

            if source == "internal" and price == 0.0:
                source = "not_found"

            return {"price": price, "source": source}

        except OpenAIError as e:
            print(f"API error ({model_name_only}): {e}")
            return {"price": 0.0, "source": "not_found"}
        except json.JSONDecodeError as e:
            print(f"AI JSON decode error: {e}. Response was: {content}")
            return {"price": 0.0, "source": "not_found"}
        except Exception as e:
            print(f"Unexpected error in get_price_with_analog_search: {e}")
            return {"price": 0.0, "source": "not_found"}

    async def parse_edit_command(self, command: str, current_total: float) -> Dict:
        client = self._get_client()
        if not client:
            return {"type": "unknown", "error": "AI-клиент не настроен."}

        prompt = f"""
        [РОЛЬ] Ты — парсер команд для редактирования сметы.
        [ЗАДАЧА] Проанализируй команду пользователя и преобразуй ее в JSON-объект.
        [ВХОДНЫЕ ДАННЫЕ]
        Команда: "{command}"
        Текущая сумма: {current_total}

        [ТИПЫ ОПЕРАЦИЙ]
        1. "percent_all_increase": Увеличить ВСЮ сумму на X процентов.
        2. "percent_all_decrease": Уменьшить ВСЮ сумму на X процентов.
        3. "set_total": Подогнать итоговую сумму под X.
        4. "percent_except_increase": Увеличить на X процентов ВСЕ, КРОМЕ строк Y, Z.
        5. "percent_except_decrease": Уменьшить на X процентов ВСЕ, КРОМЕ строк Y, Z.
        6. "set_quantity": Изменить количество для конкретной позиции (по названию или номеру строки).
        7. "set_cost": Изменить цену за единицу (стоимость) для конкретной позиции.
        8. "unknown": Команда не распознана или не относится к редактированию сметы.

        [ПРАВИЛА ИЗВЛЕЧЕНИЯ]
        - "percent": (float) Число процентов.
        - "new_total": (float) Новая итоговая сумма. Должна быть больше 0.
        - "except_rows": (list[int]) Список номеров строк для исключения (номера строк начинаются с 1).
        - "item_name": (string) Название позиции для изменения (e.g., "Труба гофрированная").
        - "item_row": (int) Номер строки для изменения (альтернатива названию).
        - "new_quantity": (float) Новое количество для "set_quantity".
        - "new_cost": (float) Новая цена за единицу для "set_cost".
        - *Всегда* отвечай *только* JSON. Без текста до или после.

        [ПРИМЕРЫ]
        Команда: "увеличь все на 10%" -> {{"type": "percent_all_increase", "percent": 10}}
        Команда: "понизь на 5" -> {{"type": "percent_all_decrease", "percent": 5}}
        Команда: "сделай итоговую 500000" -> {{"type": "set_total", "new_total": 500000}}
        Команда: "подними все на 15% кроме 5 и 23" -> {{"type": "percent_except_increase", "percent": 15, "except_rows": [5, 23]}}
        Команда: "измени количество Труба гофрированная на 800" -> {{"type": "set_quantity", "item_name": "Труба гофрированная", "new_quantity": 800}}
        Команда: "Уменьши количество Труба гофрированная с 8000 до 800" -> {{"type": "set_quantity", "item_name": "Труба гофрированная", "new_quantity": 800}}
        Команда: "в строке 8 поставь 800" -> {{"type": "set_quantity", "item_row": 8, "new_quantity": 800}}
        Команда: "измени стоимость позиции 5 на 500" -> {{"type": "set_cost", "item_row": 5, "new_cost": 500}}
        Команда: "поставь цену 500 на Труба гофрированная" -> {{"type": "set_cost", "item_name": "Труба гофрированная", "new_cost": 500}}
        Команда: "цена для строки 8 - 500 рублей" -> {{"type": "set_cost", "item_row": 8, "new_cost": 500}}
        Команда: "привет как дела" -> {{"type": "unknown"}}
        """
        try:
            model_name_only = self.model.split('@')[0]
            response = await client.chat.completions.create(
                model=model_name_only,
                messages=[
                    {"role": "system", "content": "Ты — парсер команд. Отвечаешь только в JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            content = response.choices[0].message.content
            return json.loads(content)
        except Exception as e:
            print(f"Error parsing edit command: {e}")
            return {"type": "unknown", "error": str(e)}

    async def transcribe_voice_command(self, ogg_file_path: str) -> Optional[str]:
        if not self.openai_client:
            print("OpenAI client not configured for transcription (Whisper).")
            return None

        try:
            with open(ogg_file_path, "rb") as audio_file:
                transcript = await self.openai_client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file
                )
            return transcript.text
        except OpenAIError as e:
            print(f"OpenAI Whisper error: {e}")
            if "billing" in str(e).lower() or "quota" in str(e).lower():
                raise ValueError(f"Whisper Billing/Quota Error: {e}")
            return None
        except Exception as e:
            print(f"Failed to transcribe audio: {e}")
            return None

    def _parse_items_from_json(self, content: str) -> List[Dict]:
        """Parse JSON string into list of specification items."""
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                items = data.get("items", [])
                if not isinstance(items, list):
                    # try other common keys
                    for key in ("specification", "data", "result"):
                        if key in data and isinstance(data[key], list):
                            items = data[key]
                            break
            elif isinstance(data, list):
                items = data
            else:
                return []

            out: List[Dict] = []
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip()
                code = str(item.get("code", "")).strip()
                unit = str(item.get("unit", "")).strip()
                qty_raw = item.get("quantity", 0)
                try:
                    quantity = float(qty_raw)
                except Exception:
                    quantity = 0.0
                display_name = name or code
                if not display_name:
                    continue
                if not re.search(r"[a-zA-Zа-яА-Я]", display_name):
                    continue
                out.append({"name": display_name, "quantity": quantity, "unit": unit, "code": code, "mass": 0.0})
            return out
        except Exception as e:
            print(f"JSON parse error: {e}")
            return []

    async def parse_specification_from_image(self, base64_image: str, user_hint: str = "", retry_attempt: int = 0) -> \
    List[Dict]:
        client = self.openai_client
        if not client:
            return []

        prompt = (
            "Ты OCR-ассистент. Извлеки из изображения таблицу спецификации.\n"
            "Нужно вернуть JSON-массив объектов. Каждый объект:\n"
            "{\"name\": string, \"quantity\": float, \"unit\": string, \"code\": string, \"mass\": float}\n"
            "Правила:\n"
            "1) Разделы/заголовки (например: \"1. Оборудование\", \"2. Материалы\") тоже возвращай отдельной строкой: "
            "\"name\"=текст раздела, \"quantity\"=0, \"unit\"=\"\", \"code\"=\"\", \"mass\"=0.\n"
            "2) Если количества нет — ставь 0.\n"
            "3) Никакого текста кроме JSON.\n"
        )

        if user_hint:
            prompt = f"{prompt}\nПояснение пользователя: {user_hint}\n"

        content = None
        try:
            image_payload = {"url": f"data:image/png;base64,{base64_image}", "detail": "high"}
            model_name_only = self.model.split('@')[0]
            response = await client.chat.completions.create(
                model=model_name_only,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": image_payload}
                        ]
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
                max_completion_tokens=4000
            )
            content = response.choices[0].message.content
            data = json.loads(content)

            if isinstance(data, dict):
                for key in ("items", "specification", "data"):
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break

            if not isinstance(data, list):
                return []

            validated_results = []
            for item in data:
                if not isinstance(item, dict):
                    continue

                try:
                    name_raw = str(item.get("name", "")).strip()
                    code_raw = str(item.get("code", "")).strip()
                    unit_raw = str(item.get("unit", "")).strip()

                    qty_raw = item.get("quantity", 0.0)
                    try:
                        quantity = float(qty_raw)
                    except Exception:
                        quantity = 0.0

                    mass_raw = item.get("mass", 0.0)
                    try:
                        mass = float(mass_raw)
                    except Exception:
                        mass = 0.0

                    display_name = name_raw or code_raw
                    if not display_name:
                        continue

                    if not re.search(r"[a-zA-Zа-яА-Я]", display_name):
                        continue

                    validated_results.append(
                        {
                            "name": display_name,
                            "quantity": quantity,
                            "unit": unit_raw,
                            "code": code_raw,
                            "mass": mass
                        }
                    )
                except Exception:
                    continue

            return validated_results
        except OpenAIError as e:
            print(f"OpenAI Vision error: {e}")
            if retry_attempt == 0:
                await asyncio.sleep(1)
                return await self.parse_specification_from_image(base64_image, user_hint, retry_attempt=1)
            return []
        except json.JSONDecodeError as e:
            print(f"Vision JSON parse error: {e} | raw content: {content!r:.300}")
            if retry_attempt == 0:
                await asyncio.sleep(1)
                return await self.parse_specification_from_image(base64_image, user_hint, retry_attempt=1)
            return []
        except Exception as e:
            print(f"Vision unexpected error: {e}")
            if retry_attempt == 0:
                await asyncio.sleep(1)
                return await self.parse_specification_from_image(base64_image, user_hint, retry_attempt=1)
            return []

    async def parse_specification_from_text(self, text: str) -> List[Dict]:
        """Parse specification from plain text extracted from PDF pages (faster than Vision API)."""
        client = self.openai_client
        if not client:
            return []

        prompt = (
            "Извлеки спецификацию из текста таблицы.\n"
            "Таблица содержит колонки: Позиция, Наименование, Тип/марка, Код, Завод, Ед.изм., Кол-во, Масса, Примечания.\n"
            "Верни только JSON {\"items\": [...]} где каждый item:\n"
            "{\"name\": string, \"quantity\": number, \"unit\": string, \"code\": string}\n"
            "Правила:\n"
            "1) Разделы/заголовки (строки без кол-ва, напр. 'Система П2') — quantity=0, unit='', code=''.\n"
            "2) Если кол-ва нет — quantity=0.\n"
            "3) Игнорируй строки штампа/колонтитула: 'Изм. Кол.уч.', 'Лист', 'Подпись', 'Позиция', "
            "'Наименование и техническая характеристика', 'Ед. изм.', 'Кол-во', 'Масса', 'Примечания' и т.п.\n"
            "4) Только поля: name, quantity, unit, code.\n"
        )

        model_name_only = self.model.split('@')[0]
        is_o_series = bool(re.match(r'^o\d', model_name_only))

        try:
            messages = [{"role": "user", "content": f"{prompt}\n\nТекст из PDF:\n{text}"}]
            kwargs = dict(model=model_name_only, messages=messages, max_completion_tokens=4000)
            if not is_o_series:
                kwargs["response_format"] = {"type": "json_object"}
                kwargs["temperature"] = 0.0
            resp = await client.chat.completions.create(**kwargs)
            content = resp.choices[0].message.content or ""
            if not content:
                return []
            if "```" in content:
                m = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
                if m:
                    content = m.group(1).strip()
            result = self._parse_items_from_json(content)
            print(f"parse_specification_from_text: {len(result)} items")
            return result
        except Exception as e:
            print(f"parse_specification_from_text error: {e}")
            return []

    async def parse_specification_from_pdf_bytes(self, pdf_bytes: bytes, filename: str = "spec.pdf",
                                                 user_hint: str = "") -> List[Dict]:
        client = self.openai_client
        if not client:
            return []

        base_prompt = (
            "Извлеки из PDF спецификацию (позиции/разделы).\n"
            "Верни только JSON-объект вида {\"items\": [...]}\n"
            "items: массив объектов {\"name\": string, \"quantity\": number, \"unit\": string, \"code\": string}.\n"
            "Правила:\n"
            "1) Разделы/заголовки тоже добавляй как строку: quantity=0, unit=\"\", code=\"\".\n"
            "2) Если значения нет — quantity=0, unit=\"\", code=\"\".\n"
            "3) Никаких лишних полей, только эти.\n"
        )
        if user_hint:
            base_prompt += f"\nПодсказка пользователя: {user_hint}\n"

        model_name_only = self.model.split('@')[0]
        is_o_series = bool(re.match(r'^o\d', model_name_only))

        def _build_kwargs(messages):
            kwargs = dict(model=model_name_only, messages=messages, max_completion_tokens=4000)
            if not is_o_series:
                kwargs["response_format"] = {"type": "json_object"}
                kwargs["temperature"] = 0.0
            return kwargs

        def _extract_content(content: str) -> str:
            if is_o_series and "```" in content:
                m = re.search(r"```(?:json)?\s*([\s\S]*?)```", content)
                if m:
                    return m.group(1).strip()
            return content

        # ── Метод 1: pdfplumber — структурированные таблицы ───────────────
        try:
            content_parts: List[str] = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    # Сначала пробуем extract_tables — сохраняет строки/колонки
                    tables = page.extract_tables()
                    for tbl in tables:
                        if not tbl:
                            continue
                        rows_str = []
                        for row in tbl:
                            rows_str.append(" | ".join(
                                (c.replace("\n", " ").strip() if c else "") for c in row
                            ))
                        table_md = "\n".join(rows_str)
                        if table_md.strip():
                            content_parts.append(f"[Таблица стр.{page_num}]\n{table_md}")

                    # Если таблиц нет — берём сырой текст страницы
                    if not tables:
                        raw = page.extract_text()
                        if raw and raw.strip():
                            content_parts.append(f"[Текст стр.{page_num}]\n{raw.strip()}")

            if content_parts:
                combined = "\n\n".join(content_parts)[:16000]
                prompt_text = (
                    f"{base_prompt}\n\n"
                    "Данные из PDF (таблицы отформатированы через |):\n\n"
                    f"{combined}"
                )
                messages = [{"role": "user", "content": prompt_text}]
                resp = await client.chat.completions.create(**_build_kwargs(messages))
                content = resp.choices[0].message.content or ""
                if content:
                    result = self._parse_items_from_json(_extract_content(content))
                    if result:
                        print(f"PDF parse: pdfplumber tables OK — {len(result)} items")
                        return result
                    print("PDF parse: pdfplumber вернул 0 позиций, переходим к Files API")
                else:
                    print(f"PDF parse pdfplumber: пустой ответ, finish_reason={resp.choices[0].finish_reason}")
        except Exception as e:
            print(f"PDF pdfplumber error: {e}")

        # ── Метод 2: OpenAI Files API (для моделей, поддерживающих файлы) ──
        try:
            f = await client.files.create(
                file=(filename, io.BytesIO(pdf_bytes), "application/pdf"),
                purpose="user_data"
            )
            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "file", "file": {"file_id": f.id}},
                        {"type": "text", "text": base_prompt},
                    ],
                }
            ]
            resp = await client.chat.completions.create(**_build_kwargs(messages))
            choice = resp.choices[0]
            content = choice.message.content or ""
            if not content:
                print(f"PDF Files API: empty response, finish_reason={choice.finish_reason}")
                return []
            result = self._parse_items_from_json(_extract_content(content))
            print(f"PDF parse: Files API OK, {len(result)} items")
            return result
        except Exception as e:
            print(f"PDF Files API error: {e}")
            return []


class PriceLogic:
    def __init__(self, ai_service: AIService):
        self.stopwords = set()
        self.section_titles = set()
        self.pricelist_cache = {}
        self.ai_service = ai_service

    async def load_stopwords(self, session: AsyncSession):
        try:
            result = await session.execute(select(StopWord.word))
            all_words = result.scalars().all()

            loaded_stopwords = set()
            for word_str in all_words:
                if word_str:
                    cleaned_word = word_str.lower().strip()
                    if len(cleaned_word) > 1:
                        loaded_stopwords.add(cleaned_word)
            self.stopwords = loaded_stopwords
        except Exception as e:
            print(f"!!! CRITICAL ERROR loading stopwords: {e}")
            self.stopwords = set()

    async def load_section_titles(self, session: AsyncSession):
        try:
            from db import SectionTitle
            result = await session.execute(select(SectionTitle.title))
            all_titles = result.scalars().all()

            loaded_titles = set()
            for title_str in all_titles:
                if title_str:
                    loaded_titles.add(title_str.lower().strip())
            self.section_titles = loaded_titles
        except Exception as e:
            print(f"!!! CRITICAL ERROR loading section titles: {e}")
            self.section_titles = set()

    async def load_pricelist_cache(self, session: AsyncSession):
        result = await session.execute(
            select(PriceListItem.name, PriceListItem.price, PriceListItem.price_material)
        )
        self.pricelist_cache = {
            name.lower(): {
                "material": float(price_material or 0.0),
                "work": float(price or 0.0)
            }
            for name, price, price_material in result.all()
        }

    def is_consumable(self, item_name: str) -> bool:
        name_lower = item_name.lower()
        for word in self.stopwords:
            if word in name_lower:
                return True
        return False

    def is_section_title(self, item_name: str) -> bool:
        if not item_name:
            return False

        name_lower = item_name.lower().strip()

        if name_lower in self.section_titles:
            return True

        best_match = process.extractOne(
            name_lower,
            self.section_titles,
            scorer=fuzz.token_set_ratio,
            score_cutoff=85
        )

        if best_match:
            return True

        return False

    async def find_internal_price(self, item_name: str) -> Tuple[float, float, str]:
        """Returns (material_price, work_price, source)."""
        if not self.pricelist_cache:
            return 0.0, 0.0, "not_found"

        name_lower = item_name.lower()
        if name_lower in self.pricelist_cache:
            prices = self.pricelist_cache[name_lower]
            return prices["material"], prices["work"], "internal"

        best_match = process.extractOne(
            name_lower,
            self.pricelist_cache.keys(),
            scorer=fuzz.token_set_ratio,
            score_cutoff=70
        )

        if best_match:
            matched_name = best_match[0]
            prices = self.pricelist_cache[matched_name]
            return prices["material"], prices["work"], "internal"

        return 0.0, 0.0, "not_found"

    async def process_specification(
            self,
            session: AsyncSession,
            user_id: int,
            spec_items: List[Dict],
            pdf_filename: Optional[str] = None,
            processing_msg: Optional[Message] = None,
            bot: Optional[Bot] = None,
            config: Optional[Config] = None
    ) -> Calculation:

        await self.load_stopwords(session)
        await self.load_section_titles(session)
        await self.load_pricelist_cache(session)

        new_calculation = Calculation(
            user_id=user_id,
            status="pending",
            pdf_filename=pdf_filename
        )
        try:
            session.add(new_calculation)
            await session.commit()
            await session.refresh(new_calculation)
        except Exception as e:
            await session.rollback()
            if bot and config:
                await _notify_owners_internal(bot, config, f"DB Error (Create Calculation):\n{e}")
            raise e

        calc_id = new_calculation.id

        cleaned_items = []
        garbage_triggers = ["наименовани", "позици", "единицаизм", "кодпродукци", "колво", "примечани", "опросног",
                            "лист №", "лист№"]

        for item in spec_items:
            name_raw = item.get("name", "").strip()
            qty = item.get("quantity", 0.0)
            unit = item.get("unit", "")

            if not name_raw or len(name_raw) < 2:
                continue

            name_clean = name_raw.lower().replace(" ", "")
            if any(trigger in name_clean for trigger in garbage_triggers):
                continue

            is_section = False
            if qty <= 0 and not unit:
                if self.is_section_title(name_raw):
                    is_section = True

            if qty <= 0 and not is_section and not unit:
                continue

            item['is_section_flag'] = is_section
            cleaned_items.append(item)

        total_calc_cost = 0.0
        real_items_count = sum(1 for x in cleaned_items if not x['is_section_flag'])
        if real_items_count == 0: real_items_count = len(cleaned_items)

        processed_count = 0
        items_to_add = []

        for i, item in enumerate(cleaned_items):
            is_section = item['is_section_flag']

            if not is_section:
                processed_count += 1
                if processing_msg and (processed_count % 3 == 0 or processed_count == real_items_count):
                    try:
                        await processing_msg.edit_text(f"Ищу цены... 💰 Обработано {processed_count}/{real_items_count}")
                    except TelegramBadRequest:
                        pass

            item_name = item.get("name", "Unknown Item").strip()
            code_val = item.get("code", "").strip()
            code = ""
            if code_val and code_val.lower() not in ['nan', 'none', '<na>']:
                code = code_val

            quantity = item.get("quantity", 0.0)
            unit = item.get("unit", "-")

            full_item_name = item_name
            if code and code.lower() not in item_name.lower() and "зип" not in item_name.lower():
                full_item_name = f"{item_name} {code}"

            source = "not_found"
            price_material = 0.0
            price_work = 0.0

            if is_section:
                source = "section"
                quantity = 0.0
            elif self.is_consumable(full_item_name):
                source = "consumable"
            else:
                price_material, price_work, source = await self.find_internal_price(full_item_name)

                if source == "not_found":
                    await asyncio.sleep(0.5)

                    try:
                        ai_response = await self.ai_service.get_price_with_analog_search(
                            item_name=item_name,
                            item_code=code,
                            pricelist_cache=self.pricelist_cache
                        )
                        price_work = ai_response.get("price", 0.0)
                        source = ai_response.get("source", "not_found")
                    except Exception as e:
                        err_str = str(e)
                        if "429" in err_str or "quota" in err_str.lower():
                            if bot and config:
                                await _notify_owners_internal(bot, config,
                                                              f"OpenAI API Error (Price Search):\n{err_str}")
                        print(f"Error AI processing item {item_name}: {e}")

            item_total = (price_material + price_work) * quantity
            total_calc_cost += item_total

            calc_item = CalculationItem(
                calculation_id=calc_id,
                name=item_name,
                code=code,
                mass=item.get("mass", 0.0),
                quantity=quantity,
                unit=unit,
                cost_per_unit=price_work,
                cost_material_per_unit=price_material,
                total_cost=item_total,
                source=source
            )
            items_to_add.append(calc_item)

        try:
            calc_to_update = await session.get(Calculation, calc_id)
            if calc_to_update:
                calc_to_update.total_cost = total_calc_cost

                if items_to_add:
                    session.add_all(items_to_add)

                await session.commit()
                await session.refresh(calc_to_update, ["items"])
                return calc_to_update
            else:
                raise ValueError("Calculation deleted during processing")

        except Exception as e:
            await session.rollback()
            if bot and config:
                await _notify_owners_internal(bot, config, f"DB Error (Save Items):\n{e}")
            raise e


app_config = load_config()
vector_db_instance = VectorDB(app_config)
ai_service_instance = AIService(vector_db_instance)
price_logic_instance = PriceLogic(ai_service_instance)