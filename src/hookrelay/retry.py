from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Exponential backoff with jitter.

    The delay before attempt `n` (1-indexed) is `base_delay * multiplier ** (n - 1)`,
    capped at `max_delay` and randomized by +/- `jitter` percent to avoid retry
    storms when many events fail at once.
    """

    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 300.0
    multiplier: float = 2.0
    jitter: float = 0.1

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")

    def delay_for_attempt(self, attempt: int) -> float:
        """Seconds to wait before retrying, given the attempt number that just failed."""
        raw_delay = self.base_delay * (self.multiplier ** max(attempt - 1, 0))
        capped_delay = min(raw_delay, self.max_delay)
        jitter_amount = capped_delay * self.jitter
        return capped_delay + random.uniform(-jitter_amount, jitter_amount)
