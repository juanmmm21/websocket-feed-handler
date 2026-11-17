from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import websockets

from websocket_feed_handler.exchanges import BinanceFeedAdapter
from websocket_feed_handler.feed_client import ResilientWebSocketFeed
from websocket_feed_handler.models import ConnectionEvent, ExchangeId, FeedConfig, TradeEvent
from websocket_feed_handler.staleness import StalenessMonitor, StalenessPolicy

NOW = datetime(2026, 11, 17, 23, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


def _monitor(clock: FakeClock, lag: float | None = 2.0) -> StalenessMonitor:
    return StalenessMonitor(
        StalenessPolicy(stale_after_seconds=5, max_event_lag_seconds=lag),
        clock=clock,
        wall_clock=lambda: NOW,
    )


def test_silence_makes_a_stream_stale_and_messages_make_it_fresh() -> None:
    clock = FakeClock()
    monitor = _monitor(clock)
    monitor.start(("BTCUSDT@trade",))
    clock.t += 4
    assert monitor.check() == []
    clock.t += 2
    [stale] = monitor.check()
    assert stale.stale and "no message for 6.0s" in stale.reason
    assert monitor.check() == []  # only transitions are reported
    monitor.observe("BTCUSDT@trade", NOW - timedelta(milliseconds=50))
    [fresh] = monitor.check()
    assert not fresh.stale and not monitor.any_stale


def test_lagging_exchange_time_is_stale_even_when_messages_flow() -> None:
    clock = FakeClock()
    monitor = _monitor(clock)
    monitor.start(("BTCUSDT@depth",))
    monitor.observe("BTCUSDT@depth", NOW - timedelta(seconds=9))
    [transition] = monitor.check()
    assert transition.stale and "lags by 9.0s" in transition.reason
    assert monitor.health()["streams"]["BTCUSDT@depth"]["lag_seconds"] == 9.0  # type: ignore[index]


def test_stale_flag_survives_reconnect_so_recovery_is_reported() -> None:
    clock = FakeClock()
    monitor = _monitor(clock, lag=None)
    monitor.start(("s",))
    clock.t += 10
    assert monitor.check()[0].stale
    monitor.start(("s",))  # reconnect
    monitor.observe("s")
    assert monitor.check()[0].stale is False


def test_policy_validation() -> None:
    with pytest.raises(ValueError):
        StalenessPolicy(stale_after_seconds=0)


def _trade(trade_id: int, at: datetime) -> str:
    ms = int(at.timestamp() * 1000)
    return json.dumps({"e": "trade", "E": ms, "s": "BTCUSDT", "t": trade_id, "p": "65000.00",
                       "q": "0.01", "T": ms, "m": False})


async def test_silent_socket_is_flagged_closed_and_reconnected(tmp_path: Path) -> None:
    connections = 0

    async def server(connection: websockets.ServerConnection) -> None:
        nonlocal connections
        connections += 1
        await connection.send(_trade(connections, datetime.now(tz=UTC)))
        if connections == 1:
            await connection.wait_closed()  # silent but open until the client gives up
            return
        while True:
            await asyncio.sleep(0.05)
            await connection.send(_trade(connections, datetime.now(tz=UTC)))

    events: list[ConnectionEvent] = []
    status = tmp_path / "feed_status.json"
    async with websockets.serve(server, "127.0.0.1", 0) as ws_server:
        port = ws_server.sockets[0].getsockname()[1]
        config = FeedConfig(
            exchange=ExchangeId.BINANCE,
            symbols=("BTCUSDT",),
            websocket_base_url=f"ws://127.0.0.1:{port}",
            staleness=StalenessPolicy(stale_after_seconds=0.4, max_event_lag_seconds=None),
            status_path=str(status),
        )
        feed = ResilientWebSocketFeed(config, BinanceFeedAdapter())
        fresh_again = asyncio.Event()

        async def on_trade(_trade_event: TradeEvent) -> None:
            return None

        async def on_event(event: ConnectionEvent) -> None:
            events.append(event)
            if event.event_type == "fresh":
                fresh_again.set()

        started = time.monotonic()
        task = asyncio.create_task(feed.run(on_trade, on_connection_event=on_event))
        await asyncio.wait_for(fresh_again.wait(), timeout=10)
        elapsed = time.monotonic() - started
        await feed.stop()
        await asyncio.wait_for(task, timeout=5)

    kinds = [event.event_type for event in events]
    assert kinds.index("stale") < kinds.index("reconnecting") < kinds.index("fresh")
    assert connections >= 2 and feed.stale_reconnects >= 1
    assert elapsed < 8  # detected in ~0.4 s + backoff, not left hanging
    assert json.loads(status.read_text(encoding="utf-8"))["streams"]["BTCUSDT@trade"]
