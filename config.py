import configparser
from dataclasses import dataclass
from typing import List, Set


@dataclass
class BotConfig:
    token: str
    owner_ids: Set[int]


@dataclass
class DBConfig:
    url: str


@dataclass
class VectorDBConfig:
    path: str
    collection_name: str


@dataclass
class ApiConfig:
    openai_api_key: str
    ai_model: str


@dataclass
class Config:
    bot: BotConfig
    db: DBConfig
    vector_db: VectorDBConfig
    api: ApiConfig


def load_config(path: str = "config.ini") -> Config:
    config_parser = configparser.ConfigParser()
    config_parser.read(path)

    bot_config = config_parser["bot"]

    owner_ids_str = bot_config.get("owner_ids", "").split(',')
    owner_ids = {int(uid.strip()) for uid in owner_ids_str if uid.strip().isdigit()}

    return Config(
        bot=BotConfig(
            token=bot_config.get("token"),
            owner_ids=owner_ids,
        ),
        db=DBConfig(url=config_parser["database"].get("db_url")),
        vector_db=VectorDBConfig(
            path=config_parser["vector_db"].get("path"),
            collection_name=config_parser["vector_db"].get("collection_name"),
        ),
        api=ApiConfig(
            openai_api_key=config_parser["api"].get("openai_api_key"),
            ai_model=config_parser["api"].get("ai_model"),
        ),
    )