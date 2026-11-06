# service-kit

Каркас микросервиса QuantumVPN — чтобы каждый сервис не писал одно и то же.

| Модуль | Что даёт |
|---|---|
| `app` | `create_app()`: FastAPI + запуск брокера Kafka, БД, outbox-relay и фоновых задач; `/health` и `/metrics` из коробки; `make_broker()` (идемпотентный продюсер, метрики FastStream), `consumer()` — общие параметры подписчиков |
| `messaging` | **Transactional outbox** (событие пишется в той же транзакции, что и данные, отправляется фоном) и **inbox** (повторная доставка отсеивается по `event_id`) |
| `db` | async SQLAlchemy, `Database`, `as_utc()` |
| `settings` | pydantic-settings: `KafkaSettings`, `DbSettings`, списки через запятую |

Метрики: `outbox_published_total{topic}`, `outbox_relay_errors_total`, `outbox_last_batch_size`, `inbox_duplicates_total`, плюс `faststream_*` по каждому топику.

```toml
dependencies = ["quantum-service-kit @ git+https://github.com/ResistVPNservice/service-kit.git@v1.0.0"]
```
