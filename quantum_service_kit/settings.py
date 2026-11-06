from typing import Annotated

from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def split_csv(value: object) -> object:
    """'1, 2,3' → ['1', '2', '3']; списки пропускаем как есть."""
    if isinstance(value, str):
        return [p.strip() for p in value.split(",") if p.strip()]
    return value


CsvInts = Annotated[list[int], NoDecode]
CsvStrs = Annotated[list[str], NoDecode]


class BaseServiceSettings(BaseSettings):
    """Общие настройки. Переменные окружения важнее .env."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"


class KafkaSettings(BaseServiceSettings):
    kafka_bootstrap: str = "localhost:9092"


class DbSettings(KafkaSettings):
    database_url: str = "sqlite+aiosqlite:///./local.db"
