"""Надёжная доставка событий: transactional outbox и inbox.

Outbox: событие пишется в таблицу outbox в той же транзакции, что и изменение данных.
Отдельная фоновая задача публикует его в Kafka и удаляет строку. Если Kafka недоступна,
событие не теряется, а данные и события никогда не расходятся.

Inbox: Kafka гарантирует доставку «хотя бы один раз», поэтому получатель запоминает
event_id обработанных событий и молча пропускает повторы.
"""

import asyncio
import json
import logging
from uuid import UUID

from faststream.kafka import KafkaBroker
from prometheus_client import Counter, Gauge
from sqlalchemy import Column, DateTime, MetaData, String, Table, Text, delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from resist_contracts.events import Event, utcnow
from resist_service_kit.db import BigIntPK, Database

log = logging.getLogger(__name__)

OUTBOX_PUBLISHED = Counter("outbox_published_total", "Событий отправлено из outbox в Kafka", ["topic"])
OUTBOX_ERRORS = Counter("outbox_relay_errors_total", "Неудачных попыток отправить outbox в Kafka")
OUTBOX_LAST_BATCH = Gauge("outbox_last_batch_size", "Сколько событий было в последней отправленной пачке")
INBOX_DUPLICATES = Counter("inbox_duplicates_total", "Повторно доставленных и пропущенных событий")


class Outbox:
    def __init__(self, metadata: MetaData) -> None:
        self.table = Table(
            "outbox",
            metadata,
            Column("id", BigIntPK, primary_key=True, autoincrement=True),
            Column("topic", String(200), nullable=False),
            Column("key", String(200), nullable=False),
            Column("payload", Text, nullable=False),
            Column("created_at", DateTime(timezone=True), nullable=False),
            keep_existing=True,  # build() сервиса можно вызвать повторно (тесты)
        )

    async def add(self, session: AsyncSession, event: Event, key: int | str) -> None:
        """Положить событие в outbox. Вызывать внутри транзакции, меняющей данные."""
        await session.execute(
            insert(self.table).values(
                topic=event.TOPIC,
                key=str(key),
                payload=event.model_dump_json(),
                created_at=utcnow(),
            )
        )

    async def relay_once(self, db: Database, broker: KafkaBroker, batch: int = 100) -> int:
        """Опубликовать накопившиеся события. Возвращает, сколько отправлено."""
        t = self.table
        async with db.session() as session, session.begin():
            rows = (
                await session.execute(select(t).order_by(t.c.id).limit(batch).with_for_update(skip_locked=True))
            ).all()
            for row in rows:
                await broker.publish(json.loads(row.payload), topic=row.topic, key=row.key.encode())
                OUTBOX_PUBLISHED.labels(row.topic).inc()
            if rows:
                await session.execute(delete(t).where(t.c.id.in_([r.id for r in rows])))
        OUTBOX_LAST_BATCH.set(len(rows))
        return len(rows)

    async def run(self, db: Database, broker: KafkaBroker, interval: float = 0.3) -> None:
        while True:
            try:
                sent = await self.relay_once(db, broker)
            except asyncio.CancelledError:
                raise
            except Exception:
                OUTBOX_ERRORS.inc()
                log.exception("outbox relay failed, will retry")
                sent = 0
            if sent == 0:
                await asyncio.sleep(interval)


class Inbox:
    def __init__(self, metadata: MetaData) -> None:
        self.table = Table(
            "processed_events",
            metadata,
            Column("event_id", String(36), primary_key=True),
            Column("processed_at", DateTime(timezone=True), nullable=False),
            keep_existing=True,
        )

    async def seen(self, session: AsyncSession, event_id: UUID) -> bool:
        """True — событие уже обрабатывали (пропустить). Иначе отмечает его обработанным.

        Вызывать в той же транзакции, что и обработку: при откате отметка тоже откатится.
        """
        key = str(event_id)
        found = await session.scalar(select(self.table.c.event_id).where(self.table.c.event_id == key))
        if found is not None:
            INBOX_DUPLICATES.inc()
            return True
        await session.execute(insert(self.table).values(event_id=key, processed_at=utcnow()))
        return False
