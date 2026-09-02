from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from django.http import HttpRequest, HttpResponse, HttpResponseNotAllowed, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from hookrelay.backends.base import Backend
from hookrelay.models import WebhookEvent

SignatureVerifier = Callable[[bytes, Mapping[str, str]], bool]
IdempotencyKeyExtractor = Callable[[dict[str, Any]], str | None]


def create_webhook_view(
    *,
    backend: Backend,
    source: str,
    max_attempts: int = 5,
    verify_signature: SignatureVerifier | None = None,
    idempotency_key: IdempotencyKeyExtractor | None = None,
) -> Callable[[HttpRequest], Awaitable[HttpResponse]]:
    """Builds an async Django view that enqueues incoming webhooks into `backend`
    instead of processing them inline.

    `verify_signature` receives the raw request body and headers and must return
    True to accept the request; a webhook provider's HMAC check is a typical use.
    `idempotency_key` derives a dedupe key from the parsed payload (e.g. the
    provider's own event id) so retried deliveries from the provider itself are
    not double-processed.

    Wire it directly into urls.py, e.g.
    `path("webhooks/whatsapp/", create_webhook_view(...))`. The view is exempt
    from CSRF protection since a webhook carries no session cookie to protect.
    """

    async def webhook_view(request: HttpRequest) -> HttpResponse:
        if request.method != "POST":
            return HttpResponseNotAllowed(["POST"])

        body = request.body
        # Passed through as-is (cast only for mypy) rather than converted to a plain
        # dict, since Django's HttpHeaders does case-insensitive lookups by header
        # name and a plain dict built from it would not.
        if verify_signature is not None and not verify_signature(
            body, cast(Mapping[str, str], request.headers)
        ):
            return JsonResponse({"detail": "invalid webhook signature"}, status=401)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return JsonResponse({"detail": "invalid JSON body"}, status=400)

        event = WebhookEvent(
            source=source,
            payload=payload,
            headers=dict(request.headers),
            max_attempts=max_attempts,
            idempotency_key=idempotency_key(payload) if idempotency_key else None,
        )
        accepted = await backend.enqueue(event)
        return JsonResponse({"accepted": accepted, "id": event.id})

    # csrf_exempt lacks type stubs, so it is applied via a plain call (instead of
    # `@csrf_exempt`) and the result cast back to the view's real signature, keeping
    # webhook_view itself fully typed for mypy strict.
    return cast(
        Callable[[HttpRequest], Awaitable[HttpResponse]], csrf_exempt(webhook_view)
    )
