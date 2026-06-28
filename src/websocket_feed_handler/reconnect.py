from __future__ import annotations

import random

from websocket_feed_handler.models import ReconnectPolicy


def compute_backoff_delay(attempt: int, policy: ReconnectPolicy) -> float:
    """Calcula el retardo exponencial con jitter para evitar reconexiones sincronizadas."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")

    base_delay = policy.initial_delay_seconds * (policy.backoff_multiplier ** (attempt - 1))
    capped_delay = min(base_delay, policy.max_delay_seconds)

    if policy.jitter_ratio <= 0.0:
        return capped_delay

    jitter_span = capped_delay * policy.jitter_ratio
    return max(0.0, capped_delay + random.uniform(-jitter_span, jitter_span))
