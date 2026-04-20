"""Unit tests for the shared Graph retry classifier."""
from types import SimpleNamespace

from shared._graph_retry import _is_retryable, _retry_after_seconds


class _FakeHTTPErr(Exception):
    def __init__(self, code, headers=None):
        self.response = SimpleNamespace(status_code=code, headers=headers or {})


def test_is_retryable_classifies_429_and_5xx():
    assert _is_retryable(_FakeHTTPErr(429)) is True
    assert _is_retryable(_FakeHTTPErr(500)) is True
    assert _is_retryable(_FakeHTTPErr(502)) is True
    assert _is_retryable(_FakeHTTPErr(504)) is True
    assert _is_retryable(_FakeHTTPErr(400)) is False
    assert _is_retryable(_FakeHTTPErr(404)) is False
    assert _is_retryable(ValueError("x")) is False


def test_retry_after_seconds_reads_header():
    assert _retry_after_seconds(_FakeHTTPErr(429, {"Retry-After": "3"})) == 3.0
    assert _retry_after_seconds(_FakeHTTPErr(429, {"retry-after": "1.5"})) == 1.5
    assert _retry_after_seconds(_FakeHTTPErr(429, {})) is None
    assert _retry_after_seconds(ValueError("x")) is None
