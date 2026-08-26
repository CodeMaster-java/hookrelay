from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from redis.asyncio import Redis

from hookrelay.backends.base import Backend, apply_failure
from hookrelay.exceptions import EventNotFoundError
from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy

_IDEMPOTENCY_TTL_SECONDS = 7 * 24 * 60 * 60


class RedisBackend(Backend):
    """Redis-backed implementation.

    Suitable for a small number of workers: claiming relies on `ZREM` being atomic
    per member, which guarantees an event is only ever claimed once. A claimed event
    is given a lease of `claim_lease_seconds`; if the worker that claimed it crashes
    before acking or failing it, the event stays claimable again only after you call
    `reap_stale_claims()`, which hookrelay does not do on its own. For very high
    worker concurrency, prefer PostgresBackend.
    """

    def __init__(
        self,
        redis: Redis,
        retry_policy: RetryPolicy | None = None,
        namespace: str = "hookrelay",
        claim_lease_seconds: float = 300.0,
    ) -> None:
        super().__init__(retry_policy)
        self._redis = redis
        self._namespace = namespace
        self._claim_lease_seconds = claim_lease_seconds

    def _event_key(self, event_id: str) -> str:
        return f"{self._namespace}:event:{event_id}"

    def _idempotency_key(self, key: str) -> str:
        return f"{self._namespace}:idempotency:{key}"

    @property
    def _schedule_key(self) -> str:
        return f"{self._namespace}:schedule"

    @property
    def _processing_key(self) -> str:
        return f"{self._namespace}:processing"

    @property
    def _dead_letter_key(self) -> str:
        return f"{self._namespace}:dead_letter"

    @staticmethod
    def _decode(raw: bytes | str) -> str:
        return raw.decode() if isinstance(raw, bytes) else raw

    async def _get_event(self, event_id: str) -> WebhookEvent | None:
        data = await self._redis.get(self._event_key(event_id))
        return WebhookEvent.model_validate_json(data) if data is not None else None

    async def _save_event(self, event: WebhookEvent) -> None:
        await self._redis.set(self._event_key(event.id), event.model_dump_json())

    async def enqueue(self, event: WebhookEvent) -> bool:
        if event.idempotency_key is not None:
            is_new = await self._redis.set(
                self._idempotency_key(event.idempotency_key),
                event.id,
                nx=True,
                ex=_IDEMPOTENCY_TTL_SECONDS,
            )
            if not is_new:
                return False
        await self._save_event(event)
        await self._redis.zadd(self._schedule_key, {event.id: event.next_retry_at.timestamp()})
        return True

    async def claim_due(self, limit: int) -> list[WebhookEvent]:
        now = datetime.now(timezone.utc)
        candidate_ids = cast(
            list[bytes | str],
            await self._redis.zrangebyscore(
                self._schedule_key, min=0, max=now.timestamp(), start=0, num=limit
            ),
        )
        claimed: list[WebhookEvent] = []
        lease_expires_at = now.timestamp() + self._claim_lease_seconds
        for raw_id in candidate_ids:
            event_id = self._decode(raw_id)
            removed = await self._redis.zrem(self._schedule_key, event_id)
            if not removed:
                continue  # another worker claimed it between the read and this ZREM
            event = await self._get_event(event_id)
            if event is None:
                continue
            processing_event = event.model_copy(update={"status": EventStatus.PROCESSING})
            await self._save_event(processing_event)
            await self._redis.zadd(self._processing_key, {event_id: lease_expires_at})
            claimed.append(processing_event)
        return claimed

    async def ack(self, event_id: str) -> None:
        await self._redis.zrem(self._processing_key, event_id)
        await self._redis.delete(self._event_key(event_id))

    async def fail(self, event_id: str, error: str) -> WebhookEvent:
        await self._redis.zrem(self._processing_key, event_id)
        event = await self._get_event(event_id)
        if event is None:
            raise EventNotFoundError(event_id)
        updated_event = apply_failure(event, error, self.retry_policy)
        await self._save_event(updated_event)
        if updated_event.status is EventStatus.DEAD_LETTER:
            await self._redis.zadd(
                self._dead_letter_key, {event_id: updated_event.updated_at.timestamp()}
            )
        else:
            await self._redis.zadd(
                self._schedule_key, {event_id: updated_event.next_retry_at.timestamp()}
            )
        return updated_event

    async def reap_stale_claims(self, limit: int = 100) -> int:
        """Requeues or dead-letters events whose processing lease expired without the
        worker that claimed them acking or failing them, most commonly because that
        worker crashed mid-handler. This reuses `fail()`, so a reaped event counts as
        a failed attempt toward `max_attempts` like any other failure, instead of
        being retried forever by a handler that keeps crashing the same way.

        hookrelay does not call this on its own: schedule it yourself, for example
        every `claim_lease_seconds / 2`, from whatever periodic task runner you
        already use. Returns how many stale claims were reaped.
        """
        now = datetime.now(timezone.utc).timestamp()
        stale_ids = cast(
            list[bytes | str],
            await self._redis.zrangebyscore(
                self._processing_key, min=0, max=now, start=0, num=limit
            ),
        )
        reaped = 0
        error = "stale claim: lease expired before the worker acked or failed it"
        for raw_id in stale_ids:
            event_id = self._decode(raw_id)
            removed = await self._redis.zrem(self._processing_key, event_id)
            if not removed:
                continue  # acked, failed, or already reaped concurrently
            try:
                await self.fail(event_id, error)
            except EventNotFoundError:
                continue
            reaped += 1
        return reaped

    async def list_dead_letters(self, limit: int = 100, offset: int = 0) -> list[WebhookEvent]:
        end = offset + limit - 1
        ids = cast(
            list[bytes | str],
            await self._redis.zrevrange(self._dead_letter_key, start=offset, end=end),
        )
        events = []
        for raw_id in ids:
            event_id = self._decode(raw_id)
            event = await self._get_event(event_id)
            if event is not None:
                events.append(event)
        return events

    async def requeue_dead_letter(self, event_id: str) -> bool:
        removed = await self._redis.zrem(self._dead_letter_key, event_id)
        if not removed:
            return False
        event = await self._get_event(event_id)
        if event is None:
            return False
        now = datetime.now(timezone.utc)
        requeued_event = event.model_copy(
            update={
                "status": EventStatus.PENDING,
                "attempts": 0,
                "last_error": None,
                "next_retry_at": now,
                "updated_at": now,
            }
        )
        await self._save_event(requeued_event)
        await self._redis.zadd(self._schedule_key, {event_id: now.timestamp()})
        return True
