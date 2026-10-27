from orchestrator import RetryPolicy


def test_exponential_backoff():
    policy = RetryPolicy(max_retries=3, backoff_seconds=2.0, backoff_factor=2.0)
    assert policy.delay_for_attempt(1) == 2.0
    assert policy.delay_for_attempt(2) == 4.0
    assert policy.delay_for_attempt(3) == 8.0


def test_backoff_capped():
    policy = RetryPolicy(backoff_seconds=100.0, backoff_factor=10.0, max_backoff_seconds=150.0)
    assert policy.delay_for_attempt(2) == 150.0


def test_invalid_attempt_rejected():
    import pytest

    with pytest.raises(ValueError):
        RetryPolicy().delay_for_attempt(0)
