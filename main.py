import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.fsm.storage.memory import MemoryStorage
import uvicorn

from config import load_config
from db import init_db, async_session_factory, BotSettings
from services import ai_service_instance, price_logic_instance
from handlers import router as main_router
from aiogram.types import BotCommand
from web_app import app as web_app

async def on_startup(bot: Bot):
    await init_db()

    async with async_session_factory() as session:
        settings = await session.get(BotSettings, 1)
        if settings:
            await ai_service_instance.update_settings(settings)

        await price_logic_instance.load_stopwords(session)
        await price_logic_instance.load_section_titles(session)
        await price_logic_instance.load_pricelist_cache(session)

    commands = [
        BotCommand(command="start", description="Перезапустить бота / Главное меню"),
        BotCommand(command="help", description="Показать инструкцию"),
        BotCommand(command="admin", description="Админ-панель (для владельца)")
    ]
    await bot.set_my_commands(commands)

    await bot.delete_webhook(drop_pending_updates=True)


async def main():
    logging.basicConfig(level=logging.INFO)

    config = load_config()

    storage = MemoryStorage()
    session = AiohttpSession(timeout=120)
    bot = Bot(token=config.bot.token, session=session)
    dp = Dispatcher(storage=storage)

    dp.include_router(main_router)

    dp.startup.register(on_startup)

    web_port = getattr(config, 'web', None)
    web_port = int(web_port.port) if web_port and hasattr(web_port, 'port') else 8000

    web_config = uvicorn.Config(
        web_app,
        host="0.0.0.0",
        port=web_port,
        loop="none",
        log_level="info",
    )
    web_server = uvicorn.Server(web_config)

    try:
        await asyncio.gather(
            dp.start_polling(bot),
            web_server.serve(),
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())