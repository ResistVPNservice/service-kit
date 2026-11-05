"""Каркас сервиса: FastAPI (HTTP) + FastStream (Kafka) + Postgres + outbox в одном процессе."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from faststream import AckPolicy
from faststream.kafka import KafkaBroker

from quantum_common.db import Database
from quantum_common.messaging import Outbox

BackgroundTask = Callable[[], Awaitable[None]]


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def make_broker(bootstrap: str, client_id: str) -> KafkaBroker:
    # acks="all" + идемпотентный продюсер: подтверждённое событие не потеряется и не задвоится при ретрае.
    return KafkaBroker(bootstrap, client_id=client_id, acks="all", enable_idempotence=True)


def consumer(group_id: str) -> dict[str, Any]:
    """Общие параметры подписчика Kafka.

    * earliest — новая группа читает топик с начала: события, опубликованные до первого
      запуска сервиса, не пропадут (дальше Kafka помнит позицию группы сама);
    * NACK_ON_ERROR — если обработчик упал, сообщение не подтверждается и придёт снова.
    """
    return {"group_id": group_id, "ack_policy": AckPolicy.NACK_ON_ERROR, "auto_offset_reset": "earliest"}


def create_app(
    name: str,
    *,
    log_level: str = "INFO",
    db: Database | None = None,
    broker: KafkaBroker | None = None,
    outbox: Outbox | None = None,
    on_startup: list[Callable[[], Awaitable[None]]] | None = None,
    background: list[BackgroundTask] | None = None,
    on_shutdown: list[Callable[[], Awaitable[None]]] | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        setup_logging(log_level)
        log = logging.getLogger(name)
        if db is not None:
            await db.create_all()
        if broker is not None:
            await broker.start()
        for hook in on_startup or []:
            await hook()

        tasks: list[asyncio.Task[None]] = []
        if outbox is not None and db is not None and broker is not None:
            tasks.append(asyncio.create_task(outbox.run(db, broker), name=f"{name}-outbox"))
        tasks += [asyncio.create_task(fn(), name=f"{name}-bg") for fn in background or []]
        log.info("%s started", name)
        try:
            yield
        finally:
            for t in tasks:
                t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await t
            for hook in on_shutdown or []:
                await hook()
            if broker is not None:
                await broker.stop()
            if db is not None:
                await db.dispose()
            log.info("%s stopped", name)

    app = FastAPI(title=name, lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": name}

    return app
