from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Row,
    String,
    Table,
    Text,
    delete,
    select,
    update,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from hookrelay.backends.base import Backend, apply_failure
from hookrelay.exceptions import EventNotFoundError
from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy

metadata = MetaData()

events_table = Table(
    "hookrelay_events",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("source", String(255), nullable=False),
    Column("idempotency_key", String(255), nullable=True, unique=True),
    Column("payload", JSON, nullable=False),
    Column("headers", JSON, nullable=False, server_default="{}"),
    Column("status", String(20), nullable=False),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("max_attempts", Integer, nullable=False),
    Column("last_error", Text, nullable=True),
    Column("next_retry_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Index("ix_hookrelay_events_status_next_retry", "status", "next_retry_at"),
)


def _as_utc(value: datetime) -> datetime:
    """SQLite has no native timezone-aware datetime storage, so a value written as
    UTC can come back naive on read. Restoring the tzinfo here keeps every event
    field comparable with the tz-aware `datetime.now(timezone.utc)` used elsewhere
    (in particular, by `apply_failure()`)."""
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _row_to_event(row: Row[Any]) -> WebhookEvent:
    return WebhookEvent(
        id=row.id,
        source=row.source,
        idempotency_key=row.idempotency_key,
        payload=row.payload,
        headers=row.headers,
        status=EventStatus(row.status),
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        last_error=row.last_error,
        next_retry_at=_as_utc(row.next_retry_at),
        created_at=_as_utc(row.created_at),
        updated_at=_as_utc(row.updated_at),
    )


class SQLiteBackend(Backend):
    """SQLite-backed implementation for small single-process deployments and
    scripts, where running Postgres or Redis just for retry bookkeeping is
    overkill but MemoryBackend's lack of persistence is a dealbreaker.

    SQLite serializes writes at the database level, so every write method here is
    additionally guarded by an in-process `asyncio.Lock`, the same technique
    MemoryBackend uses: it keeps concurrent calls from the same process safe and
    avoids `database is locked` errors, but it does not give the cross-process,
    multiple-worker safety that PostgresBackend's `SELECT ... FOR UPDATE SKIP
    LOCKED` provides. Prefer PostgresBackend if you plan to run more than one
    worker process against the same database.
    """

    def __init__(self, engine: AsyncEngine, retry_policy: RetryPolicy | None = None) -> None:
        super().__init__(retry_policy)
        self._engine = engine
        self._lock = asyncio.Lock()

    async def init_schema(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def enqueue(self, event: WebhookEvent) -> bool:
        stmt = sqlite_insert(events_table).values(
            id=event.id,
            source=event.source,
            idempotency_key=event.idempotency_key,
            payload=event.payload,
            headers=event.headers,
            status=event.status.value,
            attempts=event.attempts,
            max_attempts=event.max_attempts,
            last_error=event.last_error,
            next_retry_at=event.next_retry_at,
            created_at=event.created_at,
            updated_at=event.updated_at,
        )
        if event.idempotency_key is not None:
            stmt = stmt.on_conflict_do_nothing(index_elements=["idempotency_key"])
        async with self._lock, self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return result.rowcount > 0

    async def claim_due(self, limit: int) -> list[WebhookEvent]:
        now = datetime.now(timezone.utc)
        async with self._lock, self._engine.begin() as conn:
            rows = (
                await conn.execute(
                    select(events_table)
                    .where(events_table.c.status == EventStatus.PENDING.value)
                    .where(events_table.c.next_retry_at <= now)
                    .order_by(events_table.c.next_retry_at)
                    .limit(limit)
                )
            ).all()
            if not rows:
                return []
            claimed = [
                _row_to_event(row).model_copy(
                    update={"status": EventStatus.PROCESSING, "updated_at": now}
                )
                for row in rows
            ]
            await conn.execute(
                update(events_table)
                .where(events_table.c.id.in_([event.id for event in claimed]))
                .values(status=EventStatus.PROCESSING.value, updated_at=now)
            )
        return claimed

    async def ack(self, event_id: str) -> None:
        stmt = (
            update(events_table)
            .where(events_table.c.id == event_id)
            .values(status=EventStatus.SUCCESS.value, updated_at=datetime.now(timezone.utc))
        )
        async with self._lock, self._engine.begin() as conn:
            await conn.execute(stmt)

    async def fail(self, event_id: str, error: str) -> WebhookEvent:
        async with self._lock, self._engine.begin() as conn:
            row = (
                await conn.execute(select(events_table).where(events_table.c.id == event_id))
            ).first()
            if row is None:
                raise EventNotFoundError(event_id)
            updated_event = apply_failure(_row_to_event(row), error, self.retry_policy)
            await conn.execute(
                update(events_table)
                .where(events_table.c.id == event_id)
                .values(
                    status=updated_event.status.value,
                    attempts=updated_event.attempts,
                    last_error=updated_event.last_error,
                    next_retry_at=updated_event.next_retry_at,
                    updated_at=updated_event.updated_at,
                )
            )
            return updated_event

    async def list_dead_letters(self, limit: int = 100, offset: int = 0) -> list[WebhookEvent]:
        stmt = (
            select(events_table)
            .where(events_table.c.status == EventStatus.DEAD_LETTER.value)
            .order_by(events_table.c.updated_at.desc())
            .limit(limit)
            .offset(offset)
        )
        async with self._engine.connect() as conn:
            rows = (await conn.execute(stmt)).all()
        return [_row_to_event(row) for row in rows]

    async def requeue_dead_letter(self, event_id: str) -> bool:
        now = datetime.now(timezone.utc)
        stmt = (
            update(events_table)
            .where(events_table.c.id == event_id)
            .where(events_table.c.status == EventStatus.DEAD_LETTER.value)
            .values(
                status=EventStatus.PENDING.value,
                attempts=0,
                last_error=None,
                next_retry_at=now,
                updated_at=now,
            )
        )
        async with self._lock, self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return result.rowcount > 0

    async def purge(
        self,
        older_than: datetime,
        statuses: Sequence[EventStatus] = (EventStatus.SUCCESS, EventStatus.DEAD_LETTER),
    ) -> int:
        """Deletes events in one of `statuses` last updated before `older_than`.
        Returns how many rows were removed. Call this periodically yourself
        (hookrelay does not run it automatically) to keep a long-running
        deployment's table from growing without bound."""
        stmt = delete(events_table).where(
            events_table.c.status.in_([s.value for s in statuses]),
            events_table.c.updated_at < older_than,
        )
        async with self._lock, self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return result.rowcount
