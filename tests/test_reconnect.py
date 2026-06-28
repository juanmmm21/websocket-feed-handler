from __future__ import annotations

from websocket_feed_handler.models import ReconnectPolicy
from websocket_feed_handler.reconnect import compute_backoff_delay


def test_backoff_grows_with_attempts() -> None:
    policy = ReconnectPolicy(initial_delay_seconds=1.0, max_delay_seconds=30.0, jitter_ratio=0.0)

    first = compute_backoff_delay(1, policy)
    second = compute_backoff_delay(2, policy)
    third = compute_backoff_delay(3, policy)

    assert first == 1.0
    assert second == 2.0
    assert third == 4.0
