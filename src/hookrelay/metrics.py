from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class WorkerMetrics(Protocol):
    """Contract a Worker reports its activity to.

    Implement this to plug hookrelay into whatever observability stack you
    already use; PrometheusMetrics below is one such implementation. Keeping
    this as a plain Protocol lets Worker be tested without prometheus_client
    (or any other metrics library) installed.
    """

    def record_processed(self, source: str) -> None:
        """Called once per event whose handler completed without raising."""

    def record_retried(self, source: str) -> None:
        """Called once per event that failed but has attempts left."""

    def record_dead_lettered(self, source: str) -> None:
        """Called once per event that failed and exhausted its attempts."""

    def observe_handler_duration(self, source: str, seconds: float) -> None:
        """Called once per event with how long the handler call took, regardless
        of whether it succeeded or failed."""


class PrometheusMetrics:
    """WorkerMetrics implementation backed by prometheus_client.

    Requires the `metrics` extra: `pip install hookrelay[metrics]`.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        from prometheus_client import CollectorRegistry, Counter, Histogram

        if registry is None:
            registry = CollectorRegistry()

        self._processed = Counter(
            "hookrelay_events_processed_total",
            "Webhook events whose handler completed successfully.",
            ["source"],
            registry=registry,
        )
        self._retried = Counter(
            "hookrelay_events_retried_total",
            "Webhook events whose handler failed and was rescheduled for retry.",
            ["source"],
            registry=registry,
        )
        self._dead_lettered = Counter(
            "hookrelay_events_dead_lettered_total",
            "Webhook events whose handler failed after exhausting all retries.",
            ["source"],
            registry=registry,
        )
        self._handler_duration = Histogram(
            "hookrelay_handler_duration_seconds",
            "Time spent inside the webhook handler, per attempt.",
            ["source"],
            registry=registry,
        )

    def record_processed(self, source: str) -> None:
        self._processed.labels(source=source).inc()

    def record_retried(self, source: str) -> None:
        self._retried.labels(source=source).inc()

    def record_dead_lettered(self, source: str) -> None:
        self._dead_lettered.labels(source=source).inc()

    def observe_handler_duration(self, source: str, seconds: float) -> None:
        self._handler_duration.labels(source=source).observe(seconds)
