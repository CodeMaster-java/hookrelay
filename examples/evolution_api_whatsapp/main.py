"""FastAPI app that receives WhatsApp webhooks from a self-hosted Evolution API
instance reliably: the endpoint only enqueues, a worker processes with retries,
and messages that never process successfully land in the dead-letter queue
instead of vanishing.

Run:
    export WEBHOOK_SHARED_SECRET=change-me
    export DATABASE_URL=postgresql+asyncpg://user:pass@localhost/hookrelay
    uvicorn main:app --reload

Then, in your Evolution API instance settings, point the instance's webhook at
this app's /webhooks/whatsapp URL and add a custom header
`Authorization: Bearer change-me` to the webhook configuration.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from hookrelay import RetryPolicy, Worker
from hookrelay.backends.postgres import PostgresBackend
from hookrelay.fastapi import create_webhook_router

logger = logging.getLogger("evolution_webhook_example")

WEBHOOK_SHARED_SECRET = os.environ["WEBHOOK_SHARED_SECRET"]
DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_async_engine(DATABASE_URL)
backend = PostgresBackend(engine, retry_policy=RetryPolicy(max_attempts=5, base_delay=2.0))


def verify_signature(_body: bytes, headers: Any) -> bool:
    expected = f"Bearer {WEBHOOK_SHARED_SECRET}"
    return hmac.compare_digest(headers.get("authorization", ""), expected)


def extract_idempotency_key(payload: dict[str, Any]) -> str | None:
    # WhatsApp message ids are unique per message; Evolution API resends the same
    # message.upsert event on transient delivery failures, so this dedupes retries
    # coming from Evolution API itself, on top of hookrelay's own retry/DLQ.
    return payload.get("data", {}).get("key", {}).get("id")


async def handle_whatsapp_event(event) -> None:
    payload = event.payload
    event_type = payload.get("event")

    if event_type == "messages.upsert":
        message = payload["data"]
        text = message.get("message", {}).get("conversation", "<non-text message>")
        logger.info("WhatsApp message from %s: %s", message.get("key", {}).get("remoteJid"), text)
        # your business logic here: persist the message, trigger a bot reply, etc.
        # raising an exception causes hookrelay to retry this event with backoff.
    else:
        logger.debug("ignoring event type %s", event_type)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await backend.init_schema()
    worker = Worker(backend, handle_whatsapp_event)
    worker_task = asyncio.create_task(worker.run_forever())
    yield
    worker.stop()
    await worker_task
    await engine.dispose()


app = FastAPI(lifespan=lifespan)
app.include_router(
    create_webhook_router(
        backend=backend,
        source="evolution-api",
        verify_signature=verify_signature,
        idempotency_key=extract_idempotency_key,
    ),
    prefix="/webhooks/whatsapp",
)
