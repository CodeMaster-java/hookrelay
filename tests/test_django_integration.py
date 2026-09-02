from __future__ import annotations

import hashlib
import hmac
import json

import django
import pytest
from django.conf import settings

if not settings.configured:
    settings.configure(DEBUG=True, SECRET_KEY="test", USE_TZ=True, DATABASES={})
    django.setup()

from django.test import RequestFactory  # noqa: E402

from hookrelay.backends.memory import MemoryBackend  # noqa: E402
from hookrelay.django import create_webhook_view  # noqa: E402

SECRET = b"test-secret"


def sign(body: bytes) -> str:
    return hmac.new(SECRET, body, hashlib.sha256).hexdigest()


def verify_signature(body: bytes, headers) -> bool:
    expected = sign(body)
    return hmac.compare_digest(headers.get("x-signature", ""), expected)


@pytest.fixture
def backend():
    return MemoryBackend()


@pytest.fixture
def view(backend):
    return create_webhook_view(
        backend=backend,
        source="evolution-api",
        verify_signature=verify_signature,
        idempotency_key=lambda payload: payload.get("event_id"),
    )


@pytest.fixture
def rf():
    return RequestFactory()


async def test_valid_signature_is_accepted_and_enqueued(view, rf, backend):
    body = json.dumps({"event_id": "evt-1", "message": "hi"}).encode()
    request = rf.post(
        "/webhooks/", data=body, content_type="application/json",
        HTTP_X_SIGNATURE=sign(body),
    )
    response = await view(request)

    assert response.status_code == 200
    assert json.loads(response.content)["accepted"] is True
    assert len(await backend.claim_due(10)) == 1


async def test_invalid_signature_is_rejected(view, rf, backend):
    body = json.dumps({"event_id": "evt-1"}).encode()
    request = rf.post(
        "/webhooks/", data=body, content_type="application/json",
        HTTP_X_SIGNATURE="wrong",
    )
    response = await view(request)

    assert response.status_code == 401
    assert await backend.claim_due(10) == []


async def test_duplicate_event_id_is_not_enqueued_twice(view, rf, backend):
    body = json.dumps({"event_id": "evt-1"}).encode()

    def make_request():
        return rf.post(
            "/webhooks/", data=body, content_type="application/json",
            HTTP_X_SIGNATURE=sign(body),
        )

    first = await view(make_request())
    second = await view(make_request())

    assert json.loads(first.content)["accepted"] is True
    assert json.loads(second.content)["accepted"] is False
    assert len(await backend.claim_due(10)) == 1


async def test_invalid_json_body_returns_400(view, rf):
    body = b"not json"
    request = rf.post(
        "/webhooks/", data=body, content_type="application/json",
        HTTP_X_SIGNATURE=sign(body),
    )
    response = await view(request)
    assert response.status_code == 400


async def test_non_post_method_is_rejected(view, rf):
    request = rf.get("/webhooks/")
    response = await view(request)
    assert response.status_code == 405
