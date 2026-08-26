from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Row,
    String,
    Table,
    Text,
    func,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from hookrelay.backends.base import Backend, apply_failure
from hookrelay.models import EventStatus, WebhookEvent
from hookrelay.retry import RetryPolicy

metadata = MetaData()

events_table = Table(
    "hookrelay_events",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("source", String(255), nullable=False),
    Column("idempotency_key", String(255), nullable=True, unique=True),
    Column("payload", JSONB, nullable=False),
    Column("headers", JSONB, nullable=False, server_default="{}"),
    Column("status", String(20), nullable=False),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("max_attempts", Integer, nullable=False),
    Column("last_error", Text, nullable=True),
    Column("next_retry_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Index("ix_hookrelay_events_status_next_retry", "status", "next_retry_at"),
)


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
        next_retry_at=row.next_retry_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class PostgresBackend(Backend):
    """Postgres-backed implementation, safe for multiple concurrent workers.

    Requires the `hookrelay_events` table to exist; call `init_schema()` once
    (e.g. at application startup) or manage it through your own Alembic migrations
    using the `hookrelay.backends.postgres.metadata` object.
    """

    def __init__(self, engine: AsyncEngine, retry_policy: RetryPolicy | None = None) -> None:
        super().__init__(retry_policy)
        self._engine = engine

    async def init_schema(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def enqueue(self, event: WebhookEvent) -> bool:
        stmt = pg_insert(events_table).values(
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
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return result.rowcount > 0

    async def claim_due(self, limit: int) -> list[WebhookEvent]:
        now = datetime.now(timezone.utc)
        claim_ids = (
            select(events_table.c.id)
            .where(events_table.c.status == EventStatus.PENDING.value)
            .where(events_table.c.next_retry_at <= now)
            .order_by(events_table.c.next_retry_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        stmt = (
            update(events_table)
            .where(events_table.c.id.in_(claim_ids))
            .values(status=EventStatus.PROCESSING.value, updated_at=now)
            .returning(events_table)
        )
        async with self._engine.begin() as conn:
            rows = (await conn.execute(stmt)).all()
        return [_row_to_event(row) for row in rows]

    async def ack(self, event_id: str) -> None:
        stmt = (
            update(events_table)
            .where(events_table.c.id == event_id)
            .values(status=EventStatus.SUCCESS.value, updated_at=func.now())
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    async def fail(self, event_id: str, error: str) -> None:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(select(events_table).where(events_table.c.id == event_id))
            ).first()
            if row is None:
                return
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
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
            return result.rowcount > 0
