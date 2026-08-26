from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone

from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy


class Backend(ABC):
    """Storage and scheduling contract shared by every hookrelay backend.

    A backend only owns persistence and claiming semantics. The retry-vs-dead-letter
    decision itself lives in `apply_failure()` below so every implementation applies
    the exact same policy.
    """

    def __init__(self, retry_policy: RetryPolicy | None = None) -> None:
        self.retry_policy = retry_policy or RetryPolicy()

    @abstractmethod
    async def enqueue(self, event: WebhookEvent) -> bool:
        """Persist a new event. Returns False without error if its idempotency_key
        already exists (the event is treated as already accepted)."""

    @abstractmethod
    async def claim_due(self, limit: int) -> list[WebhookEvent]:
        """Atomically mark up to `limit` due pending events as processing and return them.
        Safe to call concurrently from multiple workers: an event is claimed by at most one
        caller."""

    @abstractmethod
    async def ack(self, event_id: str) -> None:
        """Mark an event as successfully processed."""

    @abstractmethod
    async def fail(self, event_id: str, error: str) -> None:
        """Record a failed processing attempt, scheduling a retry or moving the event
        to the dead-letter queue once `max_attempts` is exhausted."""

    @abstractmethod
    async def list_dead_letters(self, limit: int = 100, offset: int = 0) -> list[WebhookEvent]:
        """List events that exhausted their retries, most recently failed first."""

    @abstractmethod
    async def requeue_dead_letter(self, event_id: str) -> bool:
        """Reset a dead-lettered event back to pending for another full retry cycle.
        Returns False if no dead-lettered event with that id exists."""


def apply_failure(event: WebhookEvent, error: str, policy: RetryPolicy) -> WebhookEvent:
    """Returns a copy of `event` updated after a failed processing attempt.

    Centralizes the retry-count/backoff/dead-letter decision so every backend
    (Postgres, Redis, memory, ...) applies identical semantics.
    """
    now = datetime.now(timezone.utc)
    updated = event.model_copy(
        update={
            "attempts": event.attempts + 1,
            "last_error": error,
            "updated_at": now,
        }
    )
    if updated.exhausted:
        return updated.model_copy(update={"status": EventStatus.DEAD_LETTER})
    delay = policy.delay_for_attempt(updated.attempts)
    return updated.model_copy(
        update={
            "status": EventStatus.PENDING,
            "next_retry_at": now + timedelta(seconds=delay),
        }
    )
