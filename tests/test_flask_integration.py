from __future__ import annotations

import asyncio
import hashlib
import hmac
import json

import pytest
from flask import Flask

from hookrelay.backends.memory import MemoryBackend
from hookrelay.flask import create_webhook_blueprint

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
    app = Flask(__name__)
    blueprint = create_webhook_blueprint(
        backend=backend,
        source="evolution-api",
        verify_signature=verify_signature,
        idempotency_key=lambda payload: payload.get("event_id"),
    )
    app.register_blueprint(blueprint, url_prefix="/webhooks")
    return app.test_client()


# Flask's test client is a synchronous WSGI client that bridges to the async
# view via asgiref's AsyncToSync, which refuses to run inside a thread that
# already has an event loop active. So these tests stay plain `def` (not
# `async def`, unlike the FastAPI/httpx tests) and reach into the async
# backend through asyncio.run() only where needed, same as real Flask WSGI
# request handling does under the hood.


def test_valid_signature_is_accepted_and_enqueued(client, backend):
    body = json.dumps({"event_id": "evt-1", "message": "hi"}).encode()
    response = client.post(
        "/webhooks/", data=body, headers={"x-signature": sign(body)}
    )

    assert response.status_code == 200
    assert response.get_json()["accepted"] is True
    assert len(asyncio.run(backend.claim_due(10))) == 1


def test_invalid_signature_is_rejected(client, backend):
    body = json.dumps({"event_id": "evt-1"}).encode()
    response = client.post(
        "/webhooks/", data=body, headers={"x-signature": "wrong"}
    )

    assert response.status_code == 401
    assert asyncio.run(backend.claim_due(10)) == []


def test_duplicate_event_id_is_not_enqueued_twice(client, backend):
    body = json.dumps({"event_id": "evt-1"}).encode()
    headers = {"x-signature": sign(body)}

    first = client.post("/webhooks/", data=body, headers=headers)
    second = client.post("/webhooks/", data=body, headers=headers)

    assert first.get_json()["accepted"] is True
    assert second.get_json()["accepted"] is False
    assert len(asyncio.run(backend.claim_due(10))) == 1


def test_invalid_json_body_returns_400(client):
    response = client.post(
        "/webhooks/", data=b"not json", headers={"x-signature": sign(b"not json")}
    )
    assert response.status_code == 400
