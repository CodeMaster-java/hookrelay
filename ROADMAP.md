# Roadmap

Everything that was tracked here for v0.1 (stale-claim recovery for
`RedisBackend`, retention/cleanup for `PostgresBackend` and `SQLiteBackend`,
Prometheus metrics, a dead-letters CLI, concurrent handler execution, and a
`SQLiteBackend`) shipped in v0.2. See the README's "How it works",
"Maintenance", and "CLI" sections for how to use them.

This file now only tracks what's deliberately kept out of scope, so that
stays honest instead of growing by accident.

## Non-goals (for now)

- A hosted dashboard/UI. `list_dead_letters()` and `requeue_dead_letter()`
  (and the `hookrelay dead-letters` CLI) are meant to be wired into whatever
  admin tooling a project already has; building a UI is a much bigger
  surface area than this library's scope.
- Distributed leader election or exactly-once delivery guarantees beyond
  what each backend's native atomicity already provides. hookrelay aims for
  at-least-once delivery with idempotency as the tool for deduplication,
  not a distributed consensus system.
