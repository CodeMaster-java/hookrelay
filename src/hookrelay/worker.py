from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from hookrelay.backends.base import Backend
from hookrelay.models import WebhookEvent

logger = logging.getLogger("hookrelay.worker")

Handler = Callable[[WebhookEvent], Awaitable[None]]


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
    ) -> None:
        self._backend = backend
        self._handler = handler
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._running = False

    async def run_once(self) -> int:
        """Claims and processes a single batch. Returns how many events were claimed."""
        events = await self._backend.claim_due(self._batch_size)
        for event in events:
            try:
                await self._handler(event)
            except Exception as exc:  # noqa: BLE001 - any handler failure triggers a retry
                logger.warning("hookrelay: event %s failed: %s", event.id, exc)
                await self._backend.fail(event.id, str(exc))
            else:
                await self._backend.ack(event.id)
        return len(events)

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            processed = await self.run_once()
            if processed == 0:
                await asyncio.sleep(self._poll_interval)

    def stop(self) -> None:
        self._running = False
