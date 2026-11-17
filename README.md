# websocket-feed-handler

High-speed WebSocket client with automatic reconnection for receiving **real-time trades** from cryptocurrency exchanges. It's the **first module** of the [quant-core-infra](https://github.com/juanmmm21/quant-core-infra) ecosystem and acts as the ingestion layer: everything that happens downstream (order book, lakehouse, signals, backtest) depends on the normalized ticks this project emits.

Repository: [github.com/juanmmm21/websocket-feed-handler](https://github.com/juanmmm21/websocket-feed-handler)

---

## What it is and what problem it solves

Exchanges publish trades in different JSON formats, with proprietary fields, precision, and symbol conventions. Building trading logic directly on top of those payloads couples the system to a specific provider and makes it impossible to switch exchanges without rewriting code.

This module solves that problem with a **per-exchange adapter** that translates raw messages into a single domain type: `TradeEvent`. The WebSocket client handles the persistent connection, backoff reconnection, and hot-path parsing; the rest of the ecosystem only ever sees normalized events.

---

## Role in quant-core-infra

```text
[Binance / Coinbase WebSocket]
            │
            ▼
   websocket-feed-handler  ◄── you are here (trade ingestion)
            │
            ├──► order-book-reconstructor  (L2 depth, separate WS channel)
            ├──► market-data-lakehouse     (OHLCV aggregation + Parquet)
            └──► event-driven-backtester   (historical replay from JSONL)
```

It's the **real-time source of truth** for executed price and volume. It doesn't persist data itself: storage is delegated to `market-data-lakehouse` or to its own consumers.

---

## Purpose

This project demonstrates proficiency in:

- Advanced asynchronous programming (`asyncio` + `websockets`)
- Fault-tolerant persistent connections
- Structured JSON message parsing on the hot path
- Decoupled design via adapters and interfaces (`Protocol`)

---

## How it works (lifecycle)

1. **Configuration:** exchange, symbols, and reconnection policy are defined in `FeedConfig`.
2. **Connection:** `ResilientWebSocketFeed` opens the WebSocket using the adapter's URL and subscription messages.
3. **Reception:** each JSON message goes through the exchange's parser (`TradeMessageParser`).
4. **Normalization:** the parser returns a `TradeEvent`, or `None` if the message isn't a trade.
5. **Delivery:** the `on_trade` callback receives the typed event, with price and quantity as `Decimal`.
6. **Network failure:** the error is logged, the connection is closed, and reconnection is scheduled with exponential backoff + jitter.
7. **Clean shutdown:** `await feed.stop()` or `SIGINT`/`SIGTERM` in the CLI end the loop without leaving orphaned sockets.

---

## Supported exchanges

| Exchange | Stream / Channel | Base URL | Symbol format |
|----------|----------------|----------|--------------------|
| **Binance** | `<symbol>@trade` | `wss://stream.binance.com:9443` | `BTCUSDT` |
| **Coinbase** | `matches` channel | `wss://ws-feed.exchange.coinbase.com` | `BTC-USD` |

Official Binance documentation: [Binance Spot WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

---

## L2 depth diffs (Binance)

Besides trades, the client can subscribe to Binance's **diff-depth** stream
(`<symbol>@depth@100ms`, or `@depth` for 1 s) on the same socket and emit `DepthDiffEvent`s:

| Field | Description |
|-------|-------------|
| `first_update_id` / `final_update_id` | Binance `U` / `u`; the next event must start at `u + 1` |
| `bids` / `asks` | `(price, quantity)` as `Decimal`; quantity `0` removes the level |
| `event_time` | exchange time (`E`) |
| `received_at` | local receipt time (UTC), used to measure feed latency |

Sequence checking, snapshots and resynchronisation are the job of `order-book-reconstructor`;
this module only delivers the diffs faithfully (parse errors are logged, never silently fixed).

```bash
# record 90 minutes of trades + depth diffs as JSONL (for market-data-lakehouse ingest-depth)
websocket-feed-handler --symbols BTCUSDT --channels trade depth \
  --base-url wss://data-stream.binance.vision \
  --output stream.jsonl --duration-seconds 5400
```

`wss://data-stream.binance.vision` is Binance's market-data-only endpoint (same payloads); use it
where `stream.binance.com` is not reachable.

```python
config = FeedConfig(
    exchange=ExchangeId.BINANCE,
    symbols=("BTCUSDT",),
    channels=(StreamChannel.TRADE, StreamChannel.DEPTH),
)
await ResilientWebSocketFeed(config, create_exchange_adapter(ExchangeId.BINANCE)).run(
    on_trade=handle_trade, on_depth=handle_diff
)
```

---

## Stale-data detection

An open socket is not proof of live data. `StalenessMonitor` watches every subscribed stream:

| Condition | Meaning |
|-----------|---------|
| silence > `stale_after_seconds` | nothing received (stuck stream, silent proxy, exchange incident) |
| receipt time − exchange `event_time` > `max_event_lag_seconds` | data arrives but late (backlog) |

Only transitions are emitted, as `ConnectionEvent`s of type `stale` and `fresh`, so a consumer can
stop trading on `stale` and resume on `fresh`. With `reconnect_on_stale` a silent socket is closed
and reconnected with the usual backoff; `feed.health()` and `--status-file` expose per-stream
silence, lag and message counts (the file is rewritten atomically every second).

```bash
websocket-feed-handler --symbols BTCUSDT --stale-after-seconds 15 --max-event-lag-seconds 5 \
  --status-file data/live/feed_status.json
```

The CLI enables detection by default (15 s / 5 s); the library leaves it off unless
`FeedConfig(staleness=StalenessPolicy(...))` is given. v0.2.0 also fixes a crash: after a
cleanly closed session the backoff was computed for attempt 0 and raised instead of reconnecting.

---

## Architecture

```text
┌─────────────────────┐
│   CLI / Consumer    │
└──────────┬──────────┘
           │ TradeEventHandler (async callback)
           ▼
┌─────────────────────┐
│ ResilientWebSocket  │  exponential reconnection + jitter
│       Feed          │
└──────────┬──────────┘
           │ ExchangeFeedAdapter (Protocol)
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌──────────┐
│ Binance │ │ Coinbase │
│ Adapter │ │ Adapter  │
└─────────┘ └──────────┘
```

### Main components

| Module | Responsibility |
|--------|-----------------|
| `models.py` | `TradeEvent`, `FeedConfig`, `ReconnectPolicy`, `ExchangeId` |
| `protocols.py` | Contracts: `ExchangeFeedAdapter`, `TradeMessageParser`, `FeedHandler` |
| `feed_client.py` | Generic client with reconnection, ping, and callbacks |
| `exchanges/binance.py` | Parser and adapter for the `@trade` stream |
| `exchanges/coinbase.py` | Parser and adapter for the `matches` channel |
| `reconnect.py` | Exponential backoff calculation with random jitter |

### Technical decisions

- **Financial precision:** prices and quantities are `Decimal`, never `float`.
- **Interfaces first:** adding a new exchange = implementing an adapter; the base client doesn't change.
- **Explicit errors:** network and parsing failures are logged and trigger reconnection; never `except: pass`.
- **Memory:** the parser is reused per session; unnecessary object creation per message is avoided.

---

## Data model: `TradeEvent`

| Field | Type | Description |
|-------|------|-------------|
| `exchange` | `ExchangeId` | `binance` or `coinbase` |
| `symbol` | `str` | Exchange-normalized symbol |
| `trade_id` | `str` | Unique trade identifier on the exchange |
| `price` | `Decimal` | Execution price |
| `quantity` | `Decimal` | Executed quantity |
| `side` | `TradeSide` | `buy` or `sell` (aggressor side) |
| `event_time` | `datetime` | Exchange UTC timestamp |

Example JSONL compatible with `market-data-lakehouse`:

```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "trade_id": "12345",
  "price": "67234.50",
  "quantity": "0.015",
  "side": "buy",
  "event_time": "2024-01-01T12:00:01Z"
}
```

---

## Requirements

- Python **3.11+**
- `pip` compatible with `pyproject.toml`

---

## Installation

```bash
cd websocket-feed-handler
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## CLI usage

### Binance

```bash
websocket-feed-handler --exchange binance --symbols BTCUSDT ETHUSDT
```

### Coinbase

```bash
websocket-feed-handler --exchange coinbase --symbols BTC-USD ETH-USD
```

### Useful options

| Flag | Description |
|------|-------------|
| `--exchange` | `binance` or `coinbase` |
| `--symbols` | Space-separated list of symbols |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

Expected output:

```text
2026-06-28 12:00:01 INFO trade exchange=binance symbol=BTCUSDT id=123456 price=67234.50 qty=0.015 side=buy
```

---

## Programmatic usage

```python
import asyncio
import logging

from websocket_feed_handler import ExchangeId, FeedConfig, ResilientWebSocketFeed, TradeEvent
from websocket_feed_handler.exchanges import create_exchange_adapter

logging.basicConfig(level=logging.INFO)


async def on_trade(trade: TradeEvent) -> None:
    print(trade.symbol, trade.price, trade.quantity, trade.side.value)


async def main() -> None:
    config = FeedConfig(
        exchange=ExchangeId.BINANCE,
        symbols=("BTCUSDT",),
    )
    adapter = create_exchange_adapter(config.exchange)
    feed = ResilientWebSocketFeed(config=config, adapter=adapter)
    await feed.run(on_trade=on_trade)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Reconnection policy

Default `ReconnectPolicy`:

| Parameter | Value | Effect |
|-----------|-------|--------|
| `initial_delay_seconds` | 1.0 s | First retry after disconnection |
| `max_delay_seconds` | 60.0 s | Backoff cap |
| `backoff_multiplier` | 2.0 | Doubles the delay on each attempt |
| `jitter_ratio` | 0.1 | ±10% randomness to avoid a thundering herd |

Reconnection stops with `await feed.stop()` or `SIGINT` / `SIGTERM` signals in the CLI.

---

## Development

```bash
pytest -q
ruff check src tests
mypy src
```

---

## Module roadmap

- [ ] Advanced Binance streams (`aggTrade`, `bookTicker`)
- [ ] Latency metrics (exchange timestamp vs. local reception)
- [ ] Optional publishing to a queue (`asyncio.Queue` / Redis / NATS)
- [ ] Integration tests with a mock WebSocket server

---

## License

MIT
