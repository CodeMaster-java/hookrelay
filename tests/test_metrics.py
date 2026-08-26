from __future__ import annotations

from hookrelay.backends.memory import MemoryBackend
from hookrelay.metrics import PrometheusMetrics
from hookrelay.models import WebhookEvent
from hookrelay.retry import RetryPolicy
from hookrelay.worker import Worker

FAST_POLICY = RetryPolicy(max_attempts=2, base_delay=0.01, max_delay=0.02, jitter=0.0)


class FakeMetrics:
    def __init__(self) -> None:
        self.processed: list[str] = []
        self.retried: list[str] = []
        self.dead_lettered: list[str] = []
        self.durations: list[tuple[str, float]] = []

    def record_processed(self, source: str) -> None:
        self.processed.append(source)

    def record_retried(self, source: str) -> None:
        self.retried.append(source)

    def record_dead_lettered(self, source: str) -> None:
        self.dead_lettered.append(source)

    def observe_handler_duration(self, source: str, seconds: float) -> None:
        self.durations.append((source, seconds))


async def test_worker_records_processed_on_success():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    await backend.enqueue(WebhookEvent(source="orders", payload={}, max_attempts=2))
    metrics = FakeMetrics()

    async def handler(_event):
        pass

    worker = Worker(backend, handler, metrics=metrics)
    await worker.run_once()

    assert metrics.processed == ["orders"]
    assert metrics.retried == []
    assert metrics.dead_lettered == []
    assert len(metrics.durations) == 1
    assert metrics.durations[0][0] == "orders"


async def test_worker_records_retried_when_attempts_remain():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    await backend.enqueue(WebhookEvent(source="orders", payload={}, max_attempts=2))
    metrics = FakeMetrics()

    async def always_fails(_event):
        raise RuntimeError("boom")

    worker = Worker(backend, always_fails, metrics=metrics)
    await worker.run_once()

    assert metrics.retried == ["orders"]
    assert metrics.dead_lettered == []
    assert metrics.processed == []


async def test_worker_records_dead_lettered_once_attempts_are_exhausted():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    await backend.enqueue(WebhookEvent(source="orders", payload={}, max_attempts=1))
    metrics = FakeMetrics()

    async def always_fails(_event):
        raise RuntimeError("boom")

    worker = Worker(backend, always_fails, metrics=metrics)
    await worker.run_once()

    assert metrics.dead_lettered == ["orders"]
    assert metrics.retried == []


async def test_worker_without_metrics_does_not_raise():
    backend = MemoryBackend(retry_policy=FAST_POLICY)
    await backend.enqueue(WebhookEvent(source="orders", payload={}, max_attempts=1))

    async def handler(_event):
        pass

    worker = Worker(backend, handler)
    processed = await worker.run_once()

    assert processed == 1


async def test_prometheus_metrics_increments_counters_with_an_isolated_registry():
    from prometheus_client import CollectorRegistry

    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry=registry)

    metrics.record_processed("orders")
    metrics.record_retried("orders")
    metrics.record_dead_lettered("orders")
    metrics.observe_handler_duration("orders", 0.5)

    assert registry.get_sample_value(
        "hookrelay_events_processed_total", {"source": "orders"}
    ) == 1.0
    assert registry.get_sample_value(
        "hookrelay_events_retried_total", {"source": "orders"}
    ) == 1.0
    assert registry.get_sample_value(
        "hookrelay_events_dead_lettered_total", {"source": "orders"}
    ) == 1.0
    assert registry.get_sample_value(
        "hookrelay_handler_duration_seconds_count", {"source": "orders"}
    ) == 1.0
