# hookrelay

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
pip install hookrelay[fastapi,postgres]   # or [redis] instead of [postgres]
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

## How it works

- **Backend**: stores events and decides what's due for (re)processing.
  `PostgresBackend` and `RedisBackend` are safe for concurrent workers;
  `MemoryBackend` is for local development and tests.
- **Worker**: polls the backend, calls your handler, and acks or fails the
  event based on whether the handler raised.
- **RetryPolicy**: exponential backoff with jitter (`base_delay * multiplier
  ** attempt`, capped at `max_delay`), shared by every backend so retry
  behavior doesn't depend on which one you picked.
- **Idempotency**: pass `idempotency_key` when building a `WebhookEvent` (or
  an `idempotency_key` extractor to `create_webhook_router`) and a second
  delivery with the same key is a no-op.
- **Dead-letter queue**: once `max_attempts` is exhausted, an event moves to
  `EventStatus.DEAD_LETTER`. Inspect it with `backend.list_dead_letters()`
  and retry it manually with `backend.requeue_dead_letter(event_id)`.

## Choosing a backend

| | Postgres | Redis | Memory |
|---|---|---|---|
| Multiple workers | Yes (`SELECT ... FOR UPDATE SKIP LOCKED`) | Yes, for modest concurrency | No |
| Recovers a worker that crashes mid-processing | Yes | No (event stays `processing`) | No |
| Extra infra required | Postgres (you probably already have it) | Redis | None |

If you're unsure, start with Postgres: you likely already run one, and it
gives you the strongest guarantees.

## Known limitations

- `RedisBackend` has no stale-claim recovery: if a worker dies after claiming
  an event but before acking or failing it, that event stays `processing`
  forever. `PostgresBackend` doesn't have this problem because claims aren't
  tied to a specific worker's lifetime beyond the transaction.
- There's no built-in dashboard for dead-letter events; `list_dead_letters()`
  and `requeue_dead_letter()` are meant to be wired into your own admin
  tooling.

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
