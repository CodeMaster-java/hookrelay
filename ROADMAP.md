# Roadmap

Ideas for hookrelay beyond the current MVP. Nothing here is committed to a
timeline; this is a place to track what's deliberately left out of v0.1 and
why, so scope stays honest instead of growing by accident.

## Reliability

- **Stale-claim recovery for `RedisBackend`.** If a worker crashes after
  claiming an event but before acking or failing it, that event is stuck in
  `processing` forever today (documented in the README as a known
  limitation). A lease-with-TTL on claimed events, reaped by a periodic
  sweep that puts timed-out claims back on the schedule, would close this
  gap without adding a new required dependency.
- **Retention/cleanup for `PostgresBackend`.** Successful and dead-lettered
  events currently stay in the table indefinitely. A `purge(older_than)`
  helper (or a documented cron/Alembic pattern) would let long-running
  deployments keep the table small on purpose instead of by surprise.

## Observability

- **Prometheus metrics in `Worker`.** Counters for processed, retried, and
  dead-lettered events, plus a histogram of handler duration, so hookrelay's
  behavior shows up in existing dashboards instead of only in logs.
- **A read-only CLI for dead letters.** `hookrelay dead-letters list` /
  `requeue <id>` over a configured backend, so inspecting and recovering
  failed events doesn't require writing a throwaway script every time.

## Throughput

- **Concurrent handler execution within a batch.** `Worker.run_once()`
  currently processes its claimed batch sequentially; running handlers
  concurrently (bounded by a configurable limit) would raise throughput for
  I/O-bound handlers without changing the retry/DLQ contract.

## Backends

- **A `SQLiteBackend`.** For small single-process deployments and scripts
  where running Postgres or Redis just for retry bookkeeping is overkill,
  but `MemoryBackend`'s lack of persistence is a dealbreaker.

## Non-goals (for now)

- A hosted dashboard/UI. `list_dead_letters()` and `requeue_dead_letter()`
  are meant to be wired into whatever admin tooling a project already has;
  building a UI is a much bigger surface area than this library's scope.
- Distributed leader election or exactly-once delivery guarantees beyond
  what each backend's native atomicity already provides. hookrelay aims for
  at-least-once delivery with idempotency as the tool for deduplication,
  not a distributed consensus system.
