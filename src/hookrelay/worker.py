from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from hookrelay.backends.base import Backend
from hookrelay.metrics import WorkerMetrics
from hookrelay.models import EventStatus, WebhookEvent

logger = logging.getLogger("hookrelay.worker")

Handler = Callable[[WebhookEvent], Awaitable[None]]


class _RateLimiter:
    """Spaces out `acquire()` calls so no two start less than `1 / calls_per_second`
    apart, on average. This is deliberately simpler than a token bucket: it never
    lets a burst of queued work start faster than the configured rate, which is
    the safer default when the goal is staying under a downstream provider's own
    rate limit rather than maximizing throughput."""

    def __init__(self, calls_per_second: float) -> None:
        if calls_per_second <= 0:
            raise ValueError("calls_per_second must be positive")
        self._interval = 1.0 / calls_per_second
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._next_allowed_at - now
            self._next_allowed_at = max(now, self._next_allowed_at) + self._interval
        if wait > 0:
            await asyncio.sleep(wait)


class Worker:
    """Polls a backend for due events and runs `handler` for each one.

    A handler that raises is treated as a failed delivery attempt: the event is
    rescheduled with backoff, or moved to the dead-letter queue once its retries
    are exhausted. A handler that returns normally acknowledges the event.
    """

    def __init__(
        self,
        backend: Backend,
        handler: Handler,
        *,
        batch_size: int = 10,
        poll_interval: float = 1.0,
        concurrency: int = 1,
        max_calls_per_second: float | None = None,
        metrics: WorkerMetrics | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self._backend = backend
        self._handler = handler
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._concurrency = concurrency
        self._rate_limiter = (
            _RateLimiter(max_calls_per_second) if max_calls_per_second is not None else None
        )
        self._metrics = metrics
        self._running = False

    async def run_once(self) -> int:
        """Claims and processes a single batch. Returns how many events were claimed.

        Events within the batch run with at most `concurrency` handlers in flight
        at once, and if `max_calls_per_second` is set, no two handlers start less
        than `1 / max_calls_per_second` apart; each event is still individually
        acked or failed, so the retry and dead-letter contract is unchanged by
        either of these."""
        events = await self._backend.claim_due(self._batch_size)
        if events:
            semaphore = asyncio.Semaphore(self._concurrency)
            await asyncio.gather(*(self._process(event, semaphore) for event in events))
        return len(events)

    async def _process(self, event: WebhookEvent, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            if self._rate_limiter is not None:
                await self._rate_limiter.acquire()
            start = time.monotonic()
            try:
                await self._handler(event)
            except Exception as exc:  # noqa: BLE001 - any handler failure triggers a retry
                logger.warning("hookrelay: event %s failed: %s", event.id, exc)
                updated = await self._backend.fail(event.id, str(exc))
                self._record_outcome(event, start, updated.status)
            else:
                await self._backend.ack(event.id)
                self._record_outcome(event, start, EventStatus.SUCCESS)

    def _record_outcome(self, event: WebhookEvent, start: float, status: EventStatus) -> None:
        if self._metrics is None:
            return
        self._metrics.observe_handler_duration(event.source, time.monotonic() - start)
        if status is EventStatus.SUCCESS:
            self._metrics.record_processed(event.source)
        elif status is EventStatus.DEAD_LETTER:
            self._metrics.record_dead_lettered(event.source)
        else:
            self._metrics.record_retried(event.source)

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            processed = await self.run_once()
            if processed == 0:
                await asyncio.sleep(self._poll_interval)

    def stop(self) -> None:
        self._running = False
