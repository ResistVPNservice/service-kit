from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from sqlalchemy import MetaData

from quantum_contracts import TrialActivated
from quantum_service_kit.app import consumer, create_app
from quantum_service_kit.db import Database, as_utc
from quantum_service_kit.messaging import Inbox, Outbox
from quantum_service_kit.settings import split_csv


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.sent: list[tuple[str, Any, bytes]] = []
        self.fail = fail

    async def publish(self, message: Any, topic: str, key: bytes) -> None:
        if self.fail:
            raise ConnectionError("kafka is down")
        self.sent.append((topic, message, key))


async def make_db(tmp_path: Path) -> tuple[Database, Outbox, Inbox]:
    md = MetaData()
    outbox, inbox = Outbox(md), Inbox(md)
    db = Database(f"sqlite+aiosqlite:///{(tmp_path / 'kit.db').as_posix()}", md)
    await db.create_all()
    return db, outbox, inbox


async def test_outbox_publishes_in_order_and_cleans_up(tmp_path: Path) -> None:
    db, outbox, _ = await make_db(tmp_path)
    async with db.session() as s, s.begin():
        for tg in (1, 2, 3):
            await outbox.add(s, TrialActivated(tg_id=tg, days=3), key=tg)
    broker = FakeBroker()
    assert await outbox.relay_once(db, broker) == 3
    assert [m["tg_id"] for _, m, _ in broker.sent] == [1, 2, 3]
    assert {t for t, _, _ in broker.sent} == {"user.trial-activated"}
    assert broker.sent[0][2] == b"1"  # ключ = tg_id → одна партиция на пользователя
    assert await outbox.relay_once(db, broker) == 0
    await db.dispose()


async def test_outbox_keeps_events_when_kafka_is_down(tmp_path: Path) -> None:
    db, outbox, _ = await make_db(tmp_path)
    async with db.session() as s, s.begin():
        await outbox.add(s, TrialActivated(tg_id=7, days=3), key=7)
    try:
        await outbox.relay_once(db, FakeBroker(fail=True))
    except ConnectionError:
        pass
    ok = FakeBroker()
    assert await outbox.relay_once(db, ok) == 1  # событие не потерялось
    await db.dispose()


async def test_outbox_rolls_back_with_business_transaction(tmp_path: Path) -> None:
    db, outbox, _ = await make_db(tmp_path)
    try:
        async with db.session() as s, s.begin():
            await outbox.add(s, TrialActivated(tg_id=9, days=3), key=9)
            raise RuntimeError("business logic failed")
    except RuntimeError:
        pass
    assert await outbox.relay_once(db, FakeBroker()) == 0  # нет данных — нет и события
    await db.dispose()


async def test_inbox_deduplicates(tmp_path: Path) -> None:
    db, _, inbox = await make_db(tmp_path)
    eid = uuid4()
    async with db.session() as s, s.begin():
        assert await inbox.seen(s, eid) is False
    async with db.session() as s, s.begin():
        assert await inbox.seen(s, eid) is True
    await db.dispose()


async def test_health_and_metrics() -> None:
    app = create_app("test-service")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        assert (await c.get("/health")).json() == {"status": "ok", "service": "test-service"}
        metrics = await c.get("/metrics/")
        assert metrics.status_code == 200
        assert "outbox_published_total" in metrics.text


def test_helpers() -> None:
    assert split_csv("1, 2,,3") == ["1", "2", "3"]
    assert split_csv([1]) == [1]
    opts = consumer("g")
    assert opts["group_id"] == "g" and opts["auto_offset_reset"] == "earliest"
    assert as_utc(None) is None
