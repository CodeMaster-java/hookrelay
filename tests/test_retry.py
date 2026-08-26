import pytest

from hookrelay.retry import RetryPolicy


def test_delay_grows_exponentially_without_jitter():
    policy = RetryPolicy(base_delay=1.0, multiplier=2.0, max_delay=1000.0, jitter=0.0)
    assert policy.delay_for_attempt(1) == pytest.approx(1.0)
    assert policy.delay_for_attempt(2) == pytest.approx(2.0)
    assert policy.delay_for_attempt(3) == pytest.approx(4.0)


def test_delay_is_capped_at_max_delay():
    policy = RetryPolicy(base_delay=1.0, multiplier=10.0, max_delay=5.0, jitter=0.0)
    assert policy.delay_for_attempt(5) == pytest.approx(5.0)


def test_jitter_stays_within_bounds():
    policy = RetryPolicy(base_delay=10.0, multiplier=1.0, max_delay=10.0, jitter=0.2)
    for attempt in range(1, 20):
        delay = policy.delay_for_attempt(attempt)
        assert 8.0 <= delay <= 12.0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"base_delay": 0},
        {"max_delay": 0.5, "base_delay": 1.0},
        {"jitter": 1.5},
    ],
)
def test_invalid_policy_raises(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)
