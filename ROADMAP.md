# Roadmap

Everything that was tracked here for v0.1 (stale-claim recovery for
`RedisBackend`, retention/cleanup for `PostgresBackend` and `SQLiteBackend`,
Prometheus metrics, a dead-letters CLI, concurrent handler execution, and a
`SQLiteBackend`) shipped in v0.2. Rate limiting (`Worker(...,
max_calls_per_second=N)`) shipped in v0.3. Framework adapters for Flask
(`hookrelay.flask.create_webhook_blueprint()`) and Django
(`hookrelay.django.create_webhook_view()`), alongside the existing FastAPI
one, shipped in v0.4. See the README's "How it works", "Maintenance", "CLI",
and "Quickstart" sections for how to use all of it.

## Ideas for what's next

Nothing is currently tracked here. This section stays as a place to record
what's deliberately left out of a release and why, so scope stays honest
instead of growing by accident.

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
