"""Stale-data detection.

A socket can stay open while the data behind it stops (exchange incident, silent proxy, stuck
stream) or while it arrives late (backlog). Trading on either is trading on the past. The monitor
tracks, per stream:

* **silence** — seconds since the last message was received (monotonic clock);
* **lag** — receipt time minus the exchange's own event time.

A stream is stale when silence exceeds ``stale_after_seconds`` or lag exceeds
``max_event_lag_seconds``. Only transitions are reported, so consumers can halt on ``stale`` and
resume on ``fresh`` without being flooded.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class StalenessPolicy:
    stale_after_seconds: float = 10.0
    max_event_lag_seconds: float | None = 5.0
    reconnect_on_stale: bool = True

    def __post_init__(self) -> None:
        if self.stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        if self.max_event_lag_seconds is not None and self.max_event_lag_seconds <= 0:
            raise ValueError("max_event_lag_seconds must be positive")


@dataclass(frozen=True, slots=True)
class StalenessTransition:
    stream: str
    stale: bool
    reason: str
    silence_seconds: float
    lag_seconds: float | None


@dataclass(slots=True)
class _StreamState:
    last_received: float
    lag_seconds: float | None = None
    stale: bool = False
    messages: int = 0


@dataclass(slots=True)
class StalenessMonitor:
    policy: StalenessPolicy
    clock: Callable[[], float] = time.monotonic
    wall_clock: Callable[[], datetime] = field(default=lambda: datetime.now(tz=UTC))
    _streams: dict[str, _StreamState] = field(default_factory=dict)
    _started: float | None = None

    def start(self, streams: tuple[str, ...]) -> None:
        """Begin watching ``streams``; silence counts from now (a stream that never speaks
        becomes stale too)."""
        now = self.clock()
        self._started = now
        previous = self._streams
        # Keep the stale flag across reconnects so that recovery is reported as "fresh".
        self._streams = {
            name: _StreamState(
                last_received=now,
                stale=previous[name].stale if name in previous else False,
                messages=previous[name].messages if name in previous else 0,
            )
            for name in streams
        }

    def observe(self, stream: str, event_time: datetime | None = None) -> None:
        state = self._streams.get(stream)
        if state is None:
            state = self._streams[stream] = _StreamState(last_received=self.clock())
        state.last_received = self.clock()
        state.messages += 1
        if event_time is not None:
            state.lag_seconds = (self.wall_clock() - event_time).total_seconds()

    def check(self) -> list[StalenessTransition]:
        now = self.clock()
        transitions: list[StalenessTransition] = []
        for name, state in self._streams.items():
            silence = now - state.last_received
            reasons = []
            if silence > self.policy.stale_after_seconds:
                reasons.append(f"no message for {silence:.1f}s")
            lag_limit = self.policy.max_event_lag_seconds
            lag = state.lag_seconds
            if lag_limit is not None and lag is not None and lag > lag_limit:
                reasons.append(f"exchange time lags by {lag:.1f}s")
            stale = bool(reasons)
            if stale != state.stale:
                state.stale = stale
                transitions.append(
                    StalenessTransition(
                        stream=name,
                        stale=stale,
                        reason="; ".join(reasons) if reasons else "data flowing again",
                        silence_seconds=silence,
                        lag_seconds=state.lag_seconds,
                    )
                )
        return transitions

    def max_silence(self) -> float:
        now = self.clock()
        return max((now - state.last_received for state in self._streams.values()), default=0.0)

    @property
    def any_stale(self) -> bool:
        return any(state.stale for state in self._streams.values())

    def health(self) -> dict[str, object]:
        now = self.clock()
        return {
            "stale": self.any_stale,
            "checked_at": self.wall_clock().isoformat(),
            "streams": {
                name: {
                    "stale": state.stale,
                    "silence_seconds": round(now - state.last_received, 3),
                    "lag_seconds": _rounded(state.lag_seconds),
                    "messages": state.messages,
                }
                for name, state in self._streams.items()
            },
        }


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 3)
