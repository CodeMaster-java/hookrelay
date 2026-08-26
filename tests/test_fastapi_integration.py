from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from hookrelay.backends.memory import MemoryBackend
from hookrelay.fastapi import create_webhook_router

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
def client(backend):
    app = FastAPI()
    router = create_webhook_router(
        backend=backend,
        source="evolution-api",
        verify_signature=verify_signature,
        idempotency_key=lambda payload: payload.get("event_id"),
    )
    app.include_router(router, prefix="/webhooks")
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


async def test_valid_signature_is_accepted_and_enqueued(client, backend):
    body = json.dumps({"event_id": "evt-1", "message": "hi"}).encode()
    response = await client.post(
        "/webhooks/", content=body, headers={"x-signature": sign(body)}
    )

    assert response.status_code == 200
    assert response.json()["accepted"] is True
    assert len(await backend.claim_due(10)) == 1


async def test_invalid_signature_is_rejected(client, backend):
    body = json.dumps({"event_id": "evt-1"}).encode()
    response = await client.post(
        "/webhooks/", content=body, headers={"x-signature": "wrong"}
    )

    assert response.status_code == 401
    assert await backend.claim_due(10) == []


async def test_duplicate_event_id_is_not_enqueued_twice(client, backend):
    body = json.dumps({"event_id": "evt-1"}).encode()
    headers = {"x-signature": sign(body)}

    first = await client.post("/webhooks/", content=body, headers=headers)
    second = await client.post("/webhooks/", content=body, headers=headers)

    assert first.json()["accepted"] is True
    assert second.json()["accepted"] is False
    assert len(await backend.claim_due(10)) == 1


async def test_invalid_json_body_returns_400(client):
    response = await client.post(
        "/webhooks/", content=b"not json", headers={"x-signature": sign(b"not json")}
    )
    assert response.status_code == 400
