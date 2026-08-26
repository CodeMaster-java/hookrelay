from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class EventStatus(str, Enum):
    """Lifecycle of a webhook event inside a hookrelay backend."""

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    DEAD_LETTER = "dead_letter"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WebhookEvent(BaseModel):
    """A single webhook delivery tracked through its retry lifecycle.

    `source` identifies which integration produced the event (e.g. "evolution-api",
    "stripe"), so a single backend can safely hold events from multiple integrations.
    `idempotency_key`, when set, is enforced as unique by the backend: a second
    `enqueue()` call with the same key is a no-op.
    """

    id: str = Field(default_factory=lambda: str(uuid4()))
    source: str
    payload: dict[str, Any]
    headers: dict[str, str] = Field(default_factory=dict)
    status: EventStatus = EventStatus.PENDING
    attempts: int = 0
    max_attempts: int = 5
    idempotency_key: str | None = None
    last_error: str | None = None
    next_retry_at: datetime = Field(default_factory=_utcnow)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    @property
    def exhausted(self) -> bool:
        return self.attempts >= self.max_attempts
