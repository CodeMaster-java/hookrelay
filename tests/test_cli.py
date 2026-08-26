"""CLI tests, run against a temporary SQLite file so they need no external
service, exercising the same _connect() URL dispatch used for every backend."""

from __future__ import annotations

import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from hookrelay import EventStatus, WebhookEvent
from hookrelay.backends.sqlite import SQLiteBackend
from hookrelay.cli import main


def _db_url(tmp_path, name: str) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / name}"


def _init_schema(db_url: str) -> None:
    async def _init() -> None:
        engine = create_async_engine(db_url)
        await SQLiteBackend(engine).init_schema()
        await engine.dispose()

    asyncio.run(_init())


def _seed_dead_letter(db_url: str) -> str:
    async def _seed() -> str:
        engine = create_async_engine(db_url)
        backend = SQLiteBackend(engine)
        await backend.init_schema()
        event = WebhookEvent(source="test", payload={"hello": "world"}, max_attempts=1)
        await backend.enqueue(event)
        await backend.claim_due(limit=10)
        await backend.fail(event.id, "downstream is down")
        await engine.dispose()
        return event.id

    return asyncio.run(_seed())


def test_list_prints_no_dead_lettered_events_message(tmp_path, capsys):
    db_url = _db_url(tmp_path, "empty.db")
    _init_schema(db_url)

    exit_code = main(["dead-letters", "list", "--backend", db_url])

    assert exit_code == 0
    assert "no dead-lettered events" in capsys.readouterr().out


def test_list_prints_dead_lettered_events(tmp_path, capsys):
    db_url = _db_url(tmp_path, "events.db")
    event_id = _seed_dead_letter(db_url)

    exit_code = main(["dead-letters", "list", "--backend", db_url])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert event_id in output
    assert "downstream is down" in output


def test_requeue_moves_event_back_to_pending(tmp_path, capsys):
    db_url = _db_url(tmp_path, "events.db")
    event_id = _seed_dead_letter(db_url)

    exit_code = main(["dead-letters", "requeue", event_id, "--backend", db_url])
    assert exit_code == 0
    assert "requeued" in capsys.readouterr().out

    exit_code = main(["dead-letters", "list", "--backend", db_url])
    assert exit_code == 0
    assert "no dead-lettered events" in capsys.readouterr().out

    async def _claim() -> list[WebhookEvent]:
        engine = create_async_engine(db_url)
        backend = SQLiteBackend(engine)
        claimed = await backend.claim_due(limit=10)
        await engine.dispose()
        return claimed

    claimed = asyncio.run(_claim())
    assert [event.id for event in claimed] == [event_id]
    assert claimed[0].status == EventStatus.PROCESSING


def test_requeue_unknown_event_id_exits_with_error(tmp_path, capsys):
    db_url = _db_url(tmp_path, "empty.db")
    _init_schema(db_url)

    exit_code = main(["dead-letters", "requeue", "does-not-exist", "--backend", db_url])

    assert exit_code == 1
    assert "webhook event not found: does-not-exist" in capsys.readouterr().err


def test_unrecognized_backend_scheme_exits_with_error(capsys):
    exit_code = main(["dead-letters", "list", "--backend", "mongodb://localhost/db"])

    assert exit_code == 2
    assert "unrecognized backend URL scheme" in capsys.readouterr().err
