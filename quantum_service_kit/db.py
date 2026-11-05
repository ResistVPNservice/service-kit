from datetime import UTC, datetime

from sqlalchemy import BigInteger, Integer, MetaData
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# BIGSERIAL в Postgres, а в SQLite (тесты) автоинкремент работает только у INTEGER PRIMARY KEY.
BigIntPK = BigInteger().with_variant(Integer(), "sqlite")


def as_utc(dt: datetime | None) -> datetime | None:
    """SQLite теряет часовой пояс — считаем «голое» время UTC."""
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


class Database:
    def __init__(self, url: str, metadata: MetaData) -> None:
        self.metadata = metadata
        self.engine = create_async_engine(url, pool_pre_ping=True)
        self.sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)

    def session(self) -> AsyncSession:
        return self.sessionmaker()

    async def create_all(self) -> None:
        # Для старта хватает create_all; когда схема начнёт меняться — Alembic на каждый сервис.
        async with self.engine.begin() as conn:
            await conn.run_sync(self.metadata.create_all)

    async def dispose(self) -> None:
        await self.engine.dispose()
