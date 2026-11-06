import json
import logging

import httpx
import pytest
from faststream.kafka import TestKafkaBroker

from resist_contracts import TrialActivated
from resist_service_kit.app import create_app, make_broker
from resist_service_kit.logs import JsonFormatter, TextFormatter, bind, current_context


def records(caplog: pytest.LogCaptureFixture, logger: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == logger]


def test_json_line_has_context_and_extra(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    bind(tg_id=42)
    logging.getLogger("t").info("trial granted", extra={"days": 3})
    data = json.loads(JsonFormatter().format(records(caplog, "t")[0]))
    assert data["message"] == "trial granted"
    assert data["level"] == "INFO" and data["time"].endswith("Z")
    assert data["tg_id"] == 42 and data["days"] == 3


def test_text_line_is_readable(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    logging.getLogger("t2").warning("panel unavailable", extra={"grant_id": "payment:1", "attempt": 2})
    line = TextFormatter().format(records(caplog, "t2")[0])
    assert "WARNING t2: panel unavailable" in line
    assert "grant_id=payment:1 attempt=2" in line


def test_exception_goes_into_json(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("t3").exception("failed")
    data = json.loads(JsonFormatter().format(records(caplog, "t3")[0]))
    assert "ValueError: boom" in data["exception"]


async def test_kafka_handler_gets_event_fields(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    broker = make_broker("localhost:9092", "test")
    seen: dict = {}

    @broker.subscriber(TrialActivated.TOPIC, group_id="g")
    async def handler(event: TrialActivated) -> None:
        seen.update(current_context())
        logging.getLogger("handler").info("handled")

    async with TestKafkaBroker(broker):
        event = TrialActivated(tg_id=7, days=3)
        await broker.publish(event, topic=TrialActivated.TOPIC)
    assert seen["tg_id"] == 7 and seen["event_id"] == str(event.event_id)
    rec = records(caplog, "handler")[0]
    assert json.loads(JsonFormatter().format(rec))["tg_id"] == 7
    assert current_context().get("event_id") != str(event.event_id)  # контекст события не протекает наружу


async def test_http_request_line_and_quiet_health(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)
    app = create_app("test-service")

    @app.get("/items/{item_id}")
    async def item(item_id: int) -> dict:
        bind(tg_id=item_id)
        logging.getLogger("svc").info("item read")
        return {"id": item_id}

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        await c.get("/items/5")
        await c.get("/health")
    http = [json.loads(JsonFormatter().format(r)) for r in records(caplog, "http")]
    assert [(h["method"], h["path"], h["status"]) for h in http] == [("GET", "/items/5", 200)]  # /health не пишем
    assert "duration_ms" in http[0]
    assert json.loads(JsonFormatter().format(records(caplog, "svc")[0]))["tg_id"] == 5
