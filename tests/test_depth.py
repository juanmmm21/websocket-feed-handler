from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path

import pytest
import websockets

from websocket_feed_handler.exchanges import BinanceFeedAdapter, CoinbaseFeedAdapter
from websocket_feed_handler.exchanges.binance import BinanceDepthDiffParser
from websocket_feed_handler.feed_client import ResilientWebSocketFeed
from websocket_feed_handler.models import (
    DepthDiffEvent,
    ExchangeId,
    FeedConfig,
    StreamChannel,
    TradeEvent,
)
from websocket_feed_handler.serialization import depth_diff_to_record

DEPTH_UPDATE = {
    "e": "depthUpdate",
    "E": 1700000000123,
    "s": "BTCUSDT",
    "U": 157,
    "u": 160,
    "b": [["0.0024", "10"], ["0.0023", "0"]],
    "a": [["0.0026", "100"]],
}

TRADE = {
    "e": "trade",
    "E": 1700000000124,
    "s": "BTCUSDT",
    "t": 1,
    "p": "0.0025",
    "q": "2",
    "T": 1700000000124,
    "m": False,
}


def test_depth_parser_normalizes_diff() -> None:
    event = BinanceDepthDiffParser().parse(DEPTH_UPDATE)
    assert event is not None
    assert (event.first_update_id, event.final_update_id) == (157, 160)
    assert event.bids == ((Decimal("0.0024"), Decimal("10")), (Decimal("0.0023"), Decimal("0")))
    assert event.asks == ((Decimal("0.0026"), Decimal("100")),)
    record = depth_diff_to_record(event)
    assert record["type"] == "depth_diff"
    assert record["bids"][1] == ["0.0023", "0"]


def test_depth_parser_rejects_malformed_and_ignores_others() -> None:
    parser = BinanceDepthDiffParser()
    assert parser.parse(TRADE) is None
    with pytest.raises(ValueError):
        parser.parse({**DEPTH_UPDATE, "b": [["0.1"]]})
    with pytest.raises(ValueError):
        parser.parse({**DEPTH_UPDATE, "U": 200})


def test_binance_url_combines_trade_and_depth_streams() -> None:
    config = FeedConfig(
        exchange=ExchangeId.BINANCE,
        symbols=("BTCUSDT",),
        channels=(StreamChannel.TRADE, StreamChannel.DEPTH),
        websocket_base_url="wss://data-stream.binance.vision",
    )
    url = BinanceFeedAdapter().build_websocket_url(config)
    assert url == (
        "wss://data-stream.binance.vision/stream?streams=btcusdt@trade/btcusdt@depth@100ms"
    )
    slow = FeedConfig(
        exchange=ExchangeId.BINANCE,
        symbols=("BTCUSDT",),
        channels=(StreamChannel.DEPTH,),
        depth_speed_ms=1000,
    )
    assert BinanceFeedAdapter().build_websocket_url(slow).endswith("/ws/btcusdt@depth")


def test_depth_rejected_for_coinbase() -> None:
    config = FeedConfig(
        exchange=ExchangeId.COINBASE, symbols=("BTC-USD",), channels=(StreamChannel.DEPTH,)
    )
    with pytest.raises(ValueError):
        CoinbaseFeedAdapter().build_websocket_url(config)


async def test_feed_dispatches_trades_and_depth_from_one_socket() -> None:
    async def handler(connection: websockets.ServerConnection) -> None:
        for payload in (TRADE, DEPTH_UPDATE, {"result": None, "id": 1}):
            await connection.send(json.dumps({"stream": "x", "data": payload}))
        await asyncio.sleep(0.5)

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        config = FeedConfig(
            exchange=ExchangeId.BINANCE,
            symbols=("BTCUSDT",),
            channels=(StreamChannel.TRADE, StreamChannel.DEPTH),
            websocket_base_url=f"ws://127.0.0.1:{port}",
        )
        feed = ResilientWebSocketFeed(config, BinanceFeedAdapter())
        trades: list[TradeEvent] = []
        diffs: list[DepthDiffEvent] = []
        done = asyncio.Event()

        async def on_trade(event: TradeEvent) -> None:
            trades.append(event)

        async def on_depth(event: DepthDiffEvent) -> None:
            diffs.append(event)
            done.set()

        task = asyncio.create_task(feed.run(on_trade, on_depth=on_depth))
        await asyncio.wait_for(done.wait(), timeout=5)
        await feed.stop()
        await asyncio.wait_for(task, timeout=5)

    assert [t.trade_id for t in trades] == ["1"]
    assert [d.final_update_id for d in diffs] == [160]


async def test_depth_channel_requires_a_handler() -> None:
    config = FeedConfig(
        exchange=ExchangeId.BINANCE, symbols=("BTCUSDT",), channels=(StreamChannel.DEPTH,)
    )
    feed = ResilientWebSocketFeed(config, BinanceFeedAdapter())

    async def on_trade(event: TradeEvent) -> None:
        return None

    with pytest.raises(ValueError):
        await feed.run(on_trade)


def test_rotating_sink_rotates_before_exceeding_the_limit(tmp_path: Path) -> None:
    from websocket_feed_handler.serialization import RotatingJsonlSink

    path = tmp_path / "ticks.jsonl"
    sink = RotatingJsonlSink(path, max_bytes=60)
    for index in range(5):
        sink.write({"i": index, "pad": "xxxxxxxxxxxxxxxx"})
    sink.close()
    assert sink.rotations >= 1
    rotated = (tmp_path / "ticks.jsonl.1").read_text(encoding="utf-8").splitlines()
    current = path.read_text(encoding="utf-8").splitlines()
    assert all(len(line) + 1 <= 60 for line in rotated + current)
    assert json.loads(current[-1])["i"] == 4
