from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, BigInteger, Integer, Numeric, ForeignKey, DateTime, JSON, Text, select, Boolean, event, text
from sqlalchemy.sql import func
from sqlalchemy.engine import Engine
import datetime
from typing import List, Optional
from config import load_config

config = load_config()

engine = create_async_engine(config.db.url, connect_args={"timeout": 60})
async_session_factory = async_sessionmaker(engine, expire_on_commit=False)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    username: Mapped[Optional[str]] = mapped_column(String(100))
    first_name: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now())
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)

    calculations: Mapped[List["Calculation"]] = relationship(back_populates="user")


class PriceListItem(Base):
    __tablename__ = "price_list_item"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(500), index=True, unique=True)
    price: Mapped[float] = mapped_column(Numeric(10, 2))  # work/labor price (backward compat)
    price_material: Mapped[float] = mapped_column(Numeric(10, 2), default=0.0, server_default="0")


class StopWord(Base):
    __tablename__ = "stop_word"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    word: Mapped[str] = mapped_column(String(100), unique=True, index=True)


class Calculation(Base):
    __tablename__ = "calculation"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.user_id"))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, server_default=func.now())
    status: Mapped[str] = mapped_column(String(50), default="pending")
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), default=0.0)
    pdf_filename: Mapped[Optional[str]] = mapped_column(String(1000))

    user: Mapped["User"] = relationship(back_populates="calculations")
    items: Mapped[List["CalculationItem"]] = relationship(back_populates="calculation", cascade="all, delete-orphan")


class CalculationItem(Base):
    __tablename__ = "calculation_item"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    calculation_id: Mapped[int] = mapped_column(ForeignKey("calculation.id"))

    name: Mapped[str] = mapped_column(String(1000))
    code: Mapped[Optional[str]] = mapped_column(String(500))
    mass: Mapped[Optional[float]] = mapped_column(Numeric(10, 3), default=0.0)
    quantity: Mapped[float] = mapped_column(Numeric(10, 2))
    unit: Mapped[str] = mapped_column(String(50))
    cost_per_unit: Mapped[float] = mapped_column(Numeric(10, 2), default=0.0)  # work/labor price per unit
    cost_material_per_unit: Mapped[float] = mapped_column(Numeric(10, 2), default=0.0, server_default="0")
    total_cost: Mapped[float] = mapped_column(Numeric(12, 2), default=0.0)  # total = (material + work) * qty
    source: Mapped[str] = mapped_column(String(50), default="not_found")

    calculation: Mapped["Calculation"] = relationship(back_populates="items")


class BotSettings(Base):
    __tablename__ = "bot_settings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    openai_api_key: Mapped[Optional[str]] = mapped_column(Text)
    deepseek_api_key: Mapped[Optional[str]] = mapped_column(Text)
    gemini_api_key: Mapped[Optional[str]] = mapped_column(Text)
    ai_model: Mapped[str] = mapped_column(String(100), default="gpt-4o")
    system_prompt: Mapped[str] = mapped_column(Text, default="""Твоя задача — сопоставить позиции из спецификации оборудования с позициями из прайс-листа работ, чтобы определить стоимость монтажа каждой позиции.
Если точного совпадения нет, найди ближайшую аналогичную позицию по категории, бренду или серии.
Если ничего не найдено даже по аналогам — выполни поиск в интернете, чтобы определить среднерыночную стоимость монтажа данной позиции.

Входные данные

Спецификация оборудования — таблица с колонками, например:
Наименование / Модель / Количество / Бренд / Категория / Примечание

Прайс-лист работ — таблица с колонками:
Наименование работы / Категория / Единица / Стоимость / Примечание

Цель

Для каждой позиции из спецификации определить соответствующую работу из прайс-листа, чтобы использовать корректную цену монтажа.
Если точного совпадения нет — подобрать ближайший аналог по категории, бренду или типу устройства, а если и этого нет — найти стоимость монтажа в интернете.

Алгоритм действий

Нормализация текста

Приведи все наименования к нижнему регистру.

Удали спецсимволы (“-”, “_”, “/”, “,”), кроме смысловых (“+”, “pro”, “ptz”, “wifi”).

Замени синонимы: “видеокамера” = “камера”, “монтаж” = “установка”, “блок питания” = “БП”.

Определи ключевые поля: категория, бренд, модель, серия.

Точное совпадение

Ищи идентичные названия между спецификацией и прайс-листом.

Если найдено — пометь как точное совпадение (confidence = 1.0).

Совпадение по категории + бренду

Если точного совпадения нет, ищи позицию, где совпадают категория (например, “камера”, “регистр”, “сервер”) и бренд (например, Sony).

Пометь как совпадение по категории+бренду (confidence = 0.9).

Аналог по модели или серии

Если модели отличаются цифрой, индексом, версией (например, “2000” ↔️ “2001”, “pro” ↔️ “plus”), считай их аналогами.

Примеры:

“Камера Sony 2001” → “Монтаж камеры Sony 2000” — аналог по серии.

“NVR Pro” → “Монтаж NVR Plus” — аналог по функционалу.

Пометь как аналог по модели/серии (confidence = 0.8–0.85).

Совпадение только по категории

Если бренд и модель не найдены, но есть ставка “монтаж видеокамеры (любой бренд)” — используй её.

Пометь как категория без бренда (confidence = 0.75).

Если ничего не найдено в прайс-листе

Перейди к поиску в интернете.

Используй запрос вида:

«стоимость монтажа [наименование устройства/категория/бренд] в России, 2025 год»

Проанализируй 3–5 релевантных источников (цены сервисных компаний, маркетплейсов, форумов установщиков).

Определи среднерыночную стоимость монтажа с учётом единицы измерения (“за штуку”, “за точку”).

Укажи диапазон (минимум–максимум) и среднее значение.

Пометь как оценка по интернет-данным (confidence = 0.6–0.7).

Обязательно укажи, что цена получена из открытых источников, и приведи пример источников или ссылку.

Правила логического сопоставления

Различия в индексе, цифрах, приставках “pro”, “II”, “plus”, “v2” не считаются разными моделями.

Если устройство относится к тому же типу работ (например, “PTZ-камера” и “видеокамера”) — можно использовать ставку по категории.

Приоритет совпадений:

- точное совпадение,
- категория + бренд,
- аналог по модели/серии,
- категория,
- поиск в интернете.

Если найдено несколько кандидатов — выбери наиболее специфичный вариант (по категории или бренду).

Формат вывода

Для каждой позиции спецификации выведи:

Оборудование: Камера Sony 2001
Совпадение: Монтаж камеры Sony 2000
Тип совпадения: аналог по серии
Confidence: 0.85
Источник: Прайс-лист
Комментарий: модели отличаются индексом, но относятся к одной категории, трудоёмкость монтажа идентична.


Если совпадение найдено в интернете:

Оборудование: Камера Hikvision 500
Совпадение: не найдено в прайс-листе
Результат поиска: средняя стоимость монтажа камеры Hikvision — 2500–3200 ₽ (среднее 2850 ₽)
Источник: открытые источники (market.yandex.ru, profi.ru, forums.securitylab.ru)
Тип совпадения: оценка по интернет-данным
Confidence: 0.65
""")


class ConsumableWord(Base):
    __tablename__ = "consumable_word"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    word: Mapped[str] = mapped_column(String(100), unique=True, index=True)


class SectionTitle(Base):
    __tablename__ = "section_title"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(200), unique=True, index=True)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Migrations: add new columns if they don't exist yet
        migration_sqls = [
            "ALTER TABLE price_list_item ADD COLUMN price_material REAL DEFAULT 0.0",
            "ALTER TABLE calculation_item ADD COLUMN cost_material_per_unit REAL DEFAULT 0.0",
        ]
        for sql in migration_sqls:
            try:
                await conn.execute(text(sql))
            except Exception:
                pass  # Column already exists

    async with async_session_factory() as session:
        async with session.begin():
            exists_settings = await session.get(BotSettings, 1)
            if not exists_settings:
                cfg = load_config()
                session.add(BotSettings(
                    id=1,
                    openai_api_key=cfg.api.openai_api_key,
                    ai_model=cfg.api.ai_model
                ))

            await session.commit()