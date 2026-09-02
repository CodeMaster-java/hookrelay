from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, cast

from flask import Blueprint, request

from hookrelay.backends.base import Backend
from hookrelay.models import WebhookEvent

SignatureVerifier = Callable[[bytes, Mapping[str, str]], bool]
IdempotencyKeyExtractor = Callable[[dict[str, Any]], str | None]


def create_webhook_blueprint(
    *,
    backend: Backend,
    source: str,
    name: str | None = None,
    path: str = "/",
    max_attempts: int = 5,
    verify_signature: SignatureVerifier | None = None,
    idempotency_key: IdempotencyKeyExtractor | None = None,
) -> Blueprint:
    """Builds a Flask blueprint with a single POST route that enqueues incoming
    webhooks into `backend` instead of processing them inline.

    `verify_signature` receives the raw request body and headers and must return
    True to accept the request; a webhook provider's HMAC check is a typical use.
    `idempotency_key` derives a dedupe key from the parsed payload (e.g. the
    provider's own event id) so retried deliveries from the provider itself are
    not double-processed.

    Register it with a URL prefix, e.g.
    `app.register_blueprint(blueprint, url_prefix="/webhooks/whatsapp")`.
    Requires the `flask[async]` extra so Flask can run this as an async view.
    """
    blueprint = Blueprint(name or f"hookrelay_{source}", __name__)

    @blueprint.route(path, methods=["POST"])
    async def receive_webhook() -> tuple[dict[str, Any], int] | dict[str, Any]:
        body = request.get_data()
        # Passed through as-is (cast only for mypy) rather than converted to a plain
        # dict, since Werkzeug's Headers does case-insensitive lookups by header name
        # and a plain dict built from it would not.
        if verify_signature is not None and not verify_signature(
            body, cast(Mapping[str, str], request.headers)
        ):
            return {"detail": "invalid webhook signature"}, 401
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return {"detail": "invalid JSON body"}, 400

        event = WebhookEvent(
            source=source,
            payload=payload,
            headers=dict(request.headers),
            max_attempts=max_attempts,
            idempotency_key=idempotency_key(payload) if idempotency_key else None,
        )
        accepted = await backend.enqueue(event)
        return {"accepted": accepted, "id": event.id}

    return blueprint
