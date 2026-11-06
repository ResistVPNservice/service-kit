"""Логи ResistVPN: одна строка — одно событие, с контекстом (tg_id, event_id, grant_id, ...).

LOG_FORMAT=json (по умолчанию, для Kubernetes/Loki):
    {"time": "2026-11-06T12:00:01.123Z", "level": "INFO", "service": "tg-user-service",
     "logger": "tg-user-service", "message": "trial granted", "tg_id": 42, "days": 3}

LOG_FORMAT=text (удобно читать глазами — локально, в Headlamp):
    2026-11-06 12:00:01 INFO tg-user-service: trial granted tg_id=42 days=3

Контекст подставляется сам:
  * Kafka: middleware достаёт tg_id / event_id / grant_id из события и топик;
  * HTTP: middleware пишет строку на запрос (метод, путь, статус, длительность);
  * в коде: bind(tg_id=...) — всё, что залогируется дальше в этом запросе/событии, получит поле.
Свои поля в конкретной строке: log.info("payment registered", extra={"payment_id": 5, "amount": 130}).
"""

import contextlib
import contextvars
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime
from typing import Any

from faststream import BaseMiddleware
from starlette.requests import Request
from starlette.responses import Response

_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar("log_context", default=None)
_service = "resistvpn"

# поля, которые есть у любой LogRecord — их не считаем «своими»
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime", "color_message"}
# из события Kafka в контекст логов
_EVENT_FIELDS = ("tg_id", "event_id", "grant_id", "payment_id")
# эти пути не логируем — их дёргают пробы Kubernetes и Prometheus каждые несколько секунд
_QUIET_PATHS = ("/health", "/metrics")


def bind(**fields: Any) -> None:
    """Добавить поля ко всем следующим строкам лога в текущем запросе / обработке события."""
    _context.set({**(_context.get() or {}), **{k: v for k, v in fields.items() if v is not None}})


@contextlib.contextmanager
def log_scope(**fields: Any) -> Iterator[None]:
    """Отдельный контекст логов на блок кода (обработка апдейта, задачи): по выходе — как было."""
    token = _context.set({k: v for k, v in fields.items() if v is not None})
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> dict[str, Any]:
    return dict(_context.get() or {})


# Контекст запоминается в момент вызова log.info(...), а не при форматировании —
# так он верен и для асинхронных/буферизующих обработчиков.
_base_factory = logging.getLogRecordFactory()


def _record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
    record = _base_factory(*args, **kwargs)
    record._ctx = _context.get() or {}  # type: ignore[attr-defined]
    return record


if getattr(logging.getLogRecordFactory(), "__name__", "") != "_record_factory":
    logging.setLogRecordFactory(_record_factory)


def _fields(record: logging.LogRecord) -> dict[str, Any]:
    extra = {k: v for k, v in vars(record).items() if k not in _STANDARD and not k.startswith("_")}
    return {**getattr(record, "_ctx", {}), **extra}


def _plain(value: Any) -> Any:
    return value if isinstance(value, str | int | float | bool) or value is None else str(value)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": _service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        data.update({k: _plain(v) for k, v in _fields(record).items()})
        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        fields = " ".join(f"{k}={_plain(v)}" for k, v in _fields(record).items())
        if fields:
            head, sep, tail = line.partition("\n")  # трейсбек — после полей
            line = f"{head} {fields}{sep}{tail}"
        return line


def setup_logging(level: str = "INFO", service: str | None = None, fmt: str | None = None) -> None:
    """Один обработчик на весь процесс; uvicorn, FastStream, aiogram, httpx пишут через него же."""
    global _service
    if service:
        _service = service
    fmt = (fmt or os.environ.get("LOG_FORMAT", "json")).lower()
    handler = logging.StreamHandler()
    handler.setFormatter(TextFormatter() if fmt == "text" else JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "faststream", "aiogram"):
        lg = logging.getLogger(name)
        lg.handlers[:] = []
        lg.propagate = True
    # свою строку на HTTP-запрос пишет http_log_middleware — штатный access-лог uvicorn выключаем
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("httpx").setLevel(max(root.level, logging.WARNING))


async def http_log_middleware(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
    """FastAPI-middleware: свой контекст на каждый запрос и одна строка лога по его завершении."""
    token = _context.set({})
    started = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        path = request.url.path
        if not path.startswith(_QUIET_PATHS):
            level = logging.WARNING if status >= 500 else logging.INFO
            logging.getLogger("http").log(
                level,
                "%s %s %s",
                request.method,
                path,
                status,
                extra={
                    "method": request.method,
                    "path": path,
                    "status": status,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 1),
                },
            )
        _context.reset(token)


class KafkaLogContextMiddleware(BaseMiddleware):
    """FastStream-middleware: на время обработки события в логах есть топик и tg_id/event_id/grant_id."""

    async def consume_scope(self, call_next: Callable[[Any], Awaitable[Any]], msg: Any) -> Any:
        fields: dict[str, Any] = {}
        topic = getattr(getattr(msg, "raw_message", None), "topic", None)
        if topic:
            fields["topic"] = topic
        try:
            body = json.loads(msg.body)
            if isinstance(body, dict):
                fields.update({k: body[k] for k in _EVENT_FIELDS if k in body})
        except (TypeError, ValueError):
            pass
        token = _context.set(fields)
        try:
            return await call_next(msg)
        finally:
            _context.reset(token)
