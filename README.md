# hookrelay

[![CI](https://github.com/CodeMaster-java/hookrelay/actions/workflows/ci.yml/badge.svg)](https://github.com/CodeMaster-java/hookrelay/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/hookrelay.svg)](https://pypi.org/project/hookrelay/)
[![Python versions](https://img.shields.io/pypi/pyversions/hookrelay.svg)](https://pypi.org/project/hookrelay/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Reliable webhook processing for Python: automatic retries with exponential
backoff, a dead-letter queue for events that never succeed, and idempotency so
duplicate deliveries are not processed twice.

Webhook providers (WhatsApp/Evolution API, Stripe, GitHub, ...) fire an HTTP
request and expect a fast response. If your handler is slow, flaky, or
depends on another service that's temporarily down, you either block the
request or silently drop the event. hookrelay separates *receiving* a webhook
from *processing* it: an endpoint enqueues the event and returns immediately,
a background worker processes it with retries, and anything that keeps
failing lands in a dead-letter queue instead of disappearing.

## Install

```bash
pip install hookrelay[fastapi,postgres]   # or [redis] / [sqlite] instead of [postgres]
pip install hookrelay[flask,sqlite]       # or [django] instead of [flask]
pip install hookrelay[metrics]            # optional: Prometheus metrics for Worker
```

## Quickstart (FastAPI + Postgres)

```python
from sqlalchemy.ext.asyncio import create_async_engine

from hookrelay import RetryPolicy, Worker
from hookrelay.backends.postgres import PostgresBackend
from hookrelay.fastapi import create_webhook_router

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
backend = PostgresBackend(engine, retry_policy=RetryPolicy(max_attempts=5))

# 1. Receive: mount a router that enqueues instead of processing inline.
router = create_webhook_router(backend=backend, source="evolution-api")
app.include_router(router, prefix="/webhooks/whatsapp")

# 2. Process: run this as a background task or a separate process.
async def handle(event):
    ...  # your business logic; raise to trigger a retry

worker = Worker(backend, handle)
await worker.run_forever()
```

On startup, create the table once with `await backend.init_schema()`, or
generate a migration from `hookrelay.backends.postgres.metadata` if your
project manages schema through Alembic.

## Quickstart (Flask)

```python
from sqlalchemy.ext.asyncio import create_async_engine

from hookrelay.backends.sqlite import SQLiteBackend
from hookrelay.flask import create_webhook_blueprint

engine = create_async_engine("sqlite+aiosqlite:///webhooks.db")
backend = SQLiteBackend(engine)

blueprint = create_webhook_blueprint(backend=backend, source="evolution-api")
app.register_blueprint(blueprint, url_prefix="/webhooks/whatsapp")
```

Requires the `flask[async]` extra (Flask's `async def` view support) in
addition to whichever backend extra you use.

## Quickstart (Django)

```python
# urls.py
from sqlalchemy.ext.asyncio import create_async_engine
from django.urls import path

from hookrelay.backends.sqlite import SQLiteBackend
from hookrelay.django import create_webhook_view

engine = create_async_engine("sqlite+aiosqlite:///webhooks.db")
backend = SQLiteBackend(engine)

urlpatterns = [
    path("webhooks/whatsapp/", create_webhook_view(backend=backend, source="evolution-api")),
]
```

The view is exempt from Django's CSRF protection since a webhook carries no
session cookie to protect.

## How it works

- **Backend**: stores events and decides what's due for (re)processing.
  `PostgresBackend` and `RedisBackend` are safe for concurrent workers;
  `SQLiteBackend` is safe within a single process; `MemoryBackend` is for
  local development and tests.
- **Worker**: polls the backend, calls your handler, and acks or fails the
  event based on whether the handler raised. `Worker(..., concurrency=N)`
  runs up to `N` handlers from the same claimed batch at once, bounded by an
  `asyncio.Semaphore`; `Worker(..., max_calls_per_second=N)` spaces out when
  handlers start so your handler never calls a rate-limited downstream
  provider faster than that, regardless of `concurrency`. Either way, each
  event is still individually acked or failed, so the retry and dead-letter
  contract doesn't change.
- **RetryPolicy**: exponential backoff with jitter (`base_delay * multiplier
  ** attempt`, capped at `max_delay`), shared by every backend so retry
  behavior doesn't depend on which one you picked.
- **Idempotency**: pass `idempotency_key` when building a `WebhookEvent` (or
  an `idempotency_key` extractor to any of the framework adapters:
  `create_webhook_router`, `create_webhook_blueprint`, `create_webhook_view`)
  and a second delivery with the same key is a no-op.
- **Dead-letter queue**: once `max_attempts` is exhausted, an event moves to
  `EventStatus.DEAD_LETTER`. Inspect it with `backend.list_dead_letters()`
  and retry it manually with `backend.requeue_dead_letter(event_id)`, or
  from the command line with `hookrelay dead-letters list|requeue` (see
  below).
- **Metrics**: pass a `WorkerMetrics` implementation to `Worker(...,
  metrics=...)` for counters of processed, retried, and dead-lettered events
  plus a handler-duration histogram. `hookrelay.metrics.PrometheusMetrics`
  is a ready-made one (requires the `metrics` extra); implement the
  `WorkerMetrics` protocol yourself to plug into anything else.

## Choosing a backend

| | Postgres | Redis | SQLite | Memory |
|---|---|---|---|---|
| Multiple workers | Yes (`SELECT ... FOR UPDATE SKIP LOCKED`) | Yes, for modest concurrency | No (single process only) | No |
| Recovers a worker that crashes mid-processing | Yes | Yes, call `reap_stale_claims()` periodically | No | No |
| Extra infra required | Postgres (you probably already have it) | Redis | None (a local file) | None |

If you're unsure, start with Postgres: you likely already run one, and it
gives you the strongest guarantees. `SQLiteBackend` is a good fit for a
single-process deployment or script where standing up Postgres or Redis just
for retry bookkeeping isn't worth it.

## Maintenance

Two housekeeping operations are opt-in: hookrelay never runs them on its own,
so wire them into whatever periodic task runner you already use (a cron job,
an `asyncio` task, ...).

- **`RedisBackend.reap_stale_claims()`**: a claimed event carries a lease
  (`claim_lease_seconds`, default 300). If the worker that claimed it
  crashes before acking or failing it, the event is stuck until you call
  `reap_stale_claims()`, which requeues it (or dead-letters it, if that was
  its last attempt) exactly like any other failure. Call it every
  `claim_lease_seconds / 2` or so.
- **`PostgresBackend.purge()` / `SQLiteBackend.purge()`**: successful and
  dead-lettered events stay in the table indefinitely otherwise. Call
  `backend.purge(older_than=some_datetime)` periodically to delete them past
  a retention window you choose.

## CLI

Installing hookrelay also installs a `hookrelay` command for inspecting and
recovering dead-lettered events against a running deployment:

```bash
hookrelay dead-letters list --backend postgresql+asyncpg://user:pass@localhost/db
hookrelay dead-letters requeue <event-id> --backend redis://localhost:6379/0
```

`--backend` accepts a Postgres, Redis, or SQLite URL; the CLI picks the
matching backend implementation from its scheme and only needs that
backend's extra installed.

## Known limitations

- There's no built-in dashboard for dead-letter events; `list_dead_letters()`,
  `requeue_dead_letter()`, and the `hookrelay dead-letters` CLI are meant to
  be wired into your own admin tooling, not to replace it.
- `SQLiteBackend` is safe for concurrent calls within one process (guarded
  by an internal lock) but not for multiple worker processes writing to the
  same database file; use `PostgresBackend` if you need that.

## Roadmap

Planned improvements and deliberate non-goals live in [ROADMAP.md](ROADMAP.md).

## Example

See [`examples/evolution_api_whatsapp`](examples/evolution_api_whatsapp) for a
complete FastAPI app that receives Evolution API (self-hosted WhatsApp)
webhooks with signature verification and idempotency.

## Contributing

Issues and PRs are welcome. Run the test suite with:

```bash
pip install -e ".[dev]"
pytest
```

Postgres- and Redis-backed tests are skipped automatically unless
`HOOKRELAY_TEST_DATABASE_URL` and `HOOKRELAY_TEST_REDIS_URL` are set; see
`.github/workflows/ci.yml` for how CI provides both.

## License

MIT
