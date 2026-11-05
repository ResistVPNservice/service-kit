"""Контракты событий в Kafka.

Один тип события — один топик (имя в TOPIC). Ключ сообщения — tg_id: события одного
пользователя лежат в одной партиции и обрабатываются строго по порядку, поэтому
двойное нажатие или дубль платежа не обгонят друг друга.

Цепочка выдачи доступа:

    user.trial-activated ─┐
    payment.succeeded ────┼─► subscription-service ─► provisioning.extend-requested
    (админ: POST /grants) ┘                                   │
                                                   provisioning-service ─► Remnawave
                                                              │
              provisioning.access-extended / provisioning.failed
                                                              │
                         subscription-service ─► subscription.extended ─► notification-service
"""

from datetime import UTC, datetime
from typing import ClassVar, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(UTC)


class Event(BaseModel):
    TOPIC: ClassVar[str]

    event_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime = Field(default_factory=utcnow)


GrantSource = Literal["trial", "payment", "admin"]


class TrialActivated(Event):
    """user-service: пользователь нажал «Попробовать бесплатно» (ровно один раз)."""

    TOPIC: ClassVar[str] = "user.trial-activated"

    tg_id: int
    tg_username: str | None = None
    days: int


class PaymentSucceeded(Event):
    """payment-service: Telegram подтвердил оплату, платёж сохранён (дубли отсеяны)."""

    TOPIC: ClassVar[str] = "payment.succeeded"

    payment_id: int
    tg_id: int
    tg_username: str | None = None
    months: int
    days: int
    amount: int
    currency: str
    charge_id: str


class ExtendAccessRequested(Event):
    """subscription-service → provisioning-service: продлить доступ в панели на days дней.

    grant_id уникален для каждого начисления: provisioning-service по нему отсеивает повторы.
    """

    TOPIC: ClassVar[str] = "provisioning.extend-requested"

    grant_id: str
    tg_id: int
    tg_username: str | None = None
    days: int


class AccessExtended(Event):
    """provisioning-service: панель продлила доступ, вот актуальные данные подписки."""

    TOPIC: ClassVar[str] = "provisioning.access-extended"

    grant_id: str
    tg_id: int
    rw_username: str
    short_uuid: str
    subscription_url: str
    expire_at: datetime


class ProvisioningFailed(Event):
    """provisioning-service: начисление не удалось.

    retrying=True — панель недоступна, сервис продолжает попытки (сигнал админу).
    retrying=False — ошибка постоянная (панель отклонила запрос), начисление отменено.
    """

    TOPIC: ClassVar[str] = "provisioning.failed"

    grant_id: str
    tg_id: int
    error: str
    retrying: bool


class SubscriptionExtended(Event):
    """subscription-service: подписка продлена — уведомить пользователя, обновить напоминания."""

    TOPIC: ClassVar[str] = "subscription.extended"

    grant_id: str
    tg_id: int
    source: GrantSource
    days: int
    expire_at: datetime
    subscription_url: str
    short_uuid: str


ALL_EVENTS: tuple[type[Event], ...] = (
    TrialActivated,
    PaymentSucceeded,
    ExtendAccessRequested,
    AccessExtended,
    ProvisioningFailed,
    SubscriptionExtended,
)
