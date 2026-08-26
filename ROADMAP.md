# Roadmap

Everything that was tracked here for v0.1 (stale-claim recovery for
`RedisBackend`, retention/cleanup for `PostgresBackend` and `SQLiteBackend`,
Prometheus metrics, a dead-letters CLI, concurrent handler execution, and a
`SQLiteBackend`) shipped in v0.2. Rate limiting (`Worker(...,
max_calls_per_second=N)`) shipped in v0.3. See the README's "How it works",
"Maintenance", and "CLI" sections for how to use all of it.

## Ideas for what's next

- **Framework adapters beyond FastAPI.**
  `hookrelay.fastapi.create_webhook_router()` is a thin adapter over
  `Backend.enqueue()` (verify signature, parse body, enqueue, return); the
  same shape applies just as well to Flask and Django. Adapters for those
  two would widen who can adopt hookrelay without touching the
  Backend/Worker contract at all.

Nothing here is committed to a timeline; this is a place to track what's
deliberately left out of the current release and why, so scope stays
honest instead of growing by accident.

## Non-goals (for now)

- A hosted dashboard/UI. `list_dead_letters()` and `requeue_dead_letter()`
  (and the `hookrelay dead-letters` CLI) are meant to be wired into whatever
  admin tooling a project already has; building a UI is a much bigger
  surface area than this library's scope.
- Distributed leader election or exactly-once delivery guarantees beyond
  what each backend's native atomicity already provides. hookrelay aims for
  at-least-once delivery with idempotency as the tool for deduplication,
  not a distributed consensus system.
- Anything about the HTTP requests a handler makes, like SSRF protection or
  HMAC-signing outgoing payloads. hookrelay only receives webhooks; `Worker`
  calls the Python handler function you write, it never makes an HTTP
  request on your behalf. Those concerns belong to your handler's own HTTP
  client, not to hookrelay.
