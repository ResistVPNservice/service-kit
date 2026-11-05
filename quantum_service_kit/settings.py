from typing import Annotated

from pydantic import field_validator
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


class TelegramSettings(BaseServiceSettings):
    bot_token: str
    # Прокси до api.telegram.org, если Telegram на сервере работает плохо (например, в РФ):
    # socks5://tg-proxy:1080 — встроенный туннель через свой VLESS-сервер (см. README), или любой socks5/http.
    telegram_proxy: str = ""
    admin_ids: CsvInts = []
    public_base_url: str = ""
    support_url: str = "https://t.me/your_support"
    happ_ios_url: str = "https://apps.apple.com/app/happ-proxy-utility/id6504287215"
    happ_android_url: str = "https://play.google.com/store/apps/details?id=com.happproxy"
    happ_desktop_url: str = "https://www.happ.su/"

    @field_validator("admin_ids", mode="before")
    @classmethod
    def _split_admins(cls, v: object) -> object:
        return split_csv(v)

    @field_validator("public_base_url")
    @classmethod
    def _strip_slash(cls, v: str) -> str:
        return v.rstrip("/")
