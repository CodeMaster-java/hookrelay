from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from hookrelay.backends.base import Backend
from hookrelay.models import WebhookEvent

SignatureVerifier = Callable[[bytes, Mapping[str, str]], bool]
IdempotencyKeyExtractor = Callable[[dict[str, Any]], str | None]


def create_webhook_router(
    *,
    backend: Backend,
    source: str,
    path: str = "/",
    max_attempts: int = 5,
    verify_signature: SignatureVerifier | None = None,
    idempotency_key: IdempotencyKeyExtractor | None = None,
) -> APIRouter:
    """Builds a FastAPI router with a single POST endpoint that enqueues incoming
    webhooks into `backend` instead of processing them inline.

    `verify_signature` receives the raw request body and headers and must return
    True to accept the request; a webhook provider's HMAC check is a typical use.
    `idempotency_key` derives a dedupe key from the parsed payload (e.g. the
    provider's own event id) so retried deliveries from the provider itself are
    not double-processed.
    """
    router = APIRouter()

    @router.post(path)
    async def receive_webhook(request: Request) -> dict[str, Any]:
        body = await request.body()
        if verify_signature is not None and not verify_signature(body, request.headers):
            raise HTTPException(status_code=401, detail="invalid webhook signature")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="invalid JSON body") from exc

        event = WebhookEvent(
            source=source,
            payload=payload,
            headers=dict(request.headers),
            max_attempts=max_attempts,
            idempotency_key=idempotency_key(payload) if idempotency_key else None,
        )
        accepted = await backend.enqueue(event)
        return {"accepted": accepted, "id": event.id}

    return router
