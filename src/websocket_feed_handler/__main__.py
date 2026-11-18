from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections.abc import Sequence
from typing import Any

from websocket_feed_handler.exchanges import create_exchange_adapter
from websocket_feed_handler.feed_client import ResilientWebSocketFeed
from websocket_feed_handler.models import (
    ConnectionEvent,
    DepthDiffEvent,
    ExchangeId,
    FeedConfig,
    StreamChannel,
    TradeEvent,
)
from websocket_feed_handler.serialization import (
    RotatingJsonlSink,
    depth_diff_to_record,
    trade_to_record,
)
from websocket_feed_handler.staleness import StalenessPolicy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cliente WebSocket de trades en tiempo real para exchanges soportados.",
    )
    parser.add_argument(
        "--exchange",
        choices=[exchange.value for exchange in ExchangeId],
        default=ExchangeId.BINANCE.value,
        help="Exchange a conectar.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="Símbolos a suscribir (ej: BTCUSDT para Binance, BTC-USD para Coinbase).",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        choices=[channel.value for channel in StreamChannel],
        default=[StreamChannel.TRADE.value],
        help="Streams a suscribir: trade y/o depth (diffs L2, solo Binance).",
    )
    parser.add_argument("--depth-speed-ms", type=int, choices=[100, 1000], default=100)
    parser.add_argument(
        "--base-url",
        default=None,
        help="URL base del WebSocket (ej: wss://data-stream.binance.vision).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Graba los eventos en JSON Lines (append) en lugar de solo loguearlos.",
    )
    parser.add_argument(
        "--rotate-bytes",
        type=int,
        default=0,
        help="Rota --output a <fichero>.1 al superar N bytes (0 = sin rotación).",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=None,
        help="Detiene la grabación tras N segundos.",
    )
    parser.add_argument(
        "--stale-after-seconds",
        type=float,
        default=15.0,
        help="Marca el stream como obsoleto tras N s sin mensajes (0 = desactivado).",
    )
    parser.add_argument(
        "--max-event-lag-seconds",
        type=float,
        default=5.0,
        help="Obsoleto también si la hora del exchange va más de N s por detrás.",
    )
    parser.add_argument(
        "--reconnect-on-stale",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Cierra y reconecta un socket silencioso.",
    )
    parser.add_argument(
        "--status-file",
        default=None,
        help="Escribe aquí el estado (stale, silencio, lag) en JSON cada segundo.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging del proceso.",
    )
    return parser


async def _run_feed(
    exchange: ExchangeId,
    symbols: Sequence[str],
    *,
    channels: Sequence[str] = ("trade",),
    depth_speed_ms: int = 100,
    base_url: str | None = None,
    output: str | None = None,
    duration_seconds: float | None = None,
    staleness: StalenessPolicy | None = None,
    status_file: str | None = None,
    rotate_bytes: int = 0,
) -> None:
    config = FeedConfig(
        exchange=exchange,
        symbols=tuple(symbols),
        channels=tuple(StreamChannel(channel) for channel in channels),
        depth_speed_ms=depth_speed_ms,
        websocket_base_url=base_url,
        staleness=staleness,
        status_path=status_file,
    )
    sink = RotatingJsonlSink(output, max_bytes=rotate_bytes) if output is not None else None

    def _write(record: dict[str, Any]) -> None:
        if sink is not None:
            sink.write(record)
    adapter = create_exchange_adapter(exchange)
    feed = ResilientWebSocketFeed(config=config, adapter=adapter)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_shutdown)

    async def on_depth(event: DepthDiffEvent) -> None:
        if sink is not None:
            _write(depth_diff_to_record(event))
            return
        logging.getLogger(__name__).info(
            "depth symbol=%s U=%s u=%s bids=%s asks=%s",
            event.symbol,
            event.first_update_id,
            event.final_update_id,
            len(event.bids),
            len(event.asks),
        )

    async def on_trade(trade: TradeEvent) -> None:
        if sink is not None:
            _write(trade_to_record(trade))
            return
        logging.getLogger(__name__).info(
            "trade exchange=%s symbol=%s id=%s price=%s qty=%s side=%s",
            trade.exchange.value,
            trade.symbol,
            trade.trade_id,
            trade.price,
            trade.quantity,
            trade.side.value,
        )

    async def on_connection_event(event: ConnectionEvent) -> None:
        logging.getLogger(__name__).info(
            "connection exchange=%s type=%s message=%s attempt=%s",
            event.exchange.value,
            event.event_type,
            event.message,
            event.attempt,
        )

    feed_task = asyncio.create_task(
        feed.run(
            on_trade=on_trade,
            on_connection_event=on_connection_event,
            on_depth=on_depth if StreamChannel.DEPTH in config.channels else None,
        ),
    )
    stop_task = asyncio.create_task(stop_event.wait())
    if duration_seconds is not None:
        loop.call_later(duration_seconds, stop_event.set)

    try:
        done, _ = await asyncio.wait(
            {feed_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in done:
            if task is feed_task and feed_task.exception() is not None:
                raise feed_task.exception()  # type: ignore[misc]

        await feed.stop()
        if not feed_task.done():
            feed_task.cancel()
            await asyncio.gather(feed_task, return_exceptions=True)
    finally:
        stop_task.cancel()
        if sink is not None:
            sink.close()


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    exchange = ExchangeId(args.exchange)
    asyncio.run(
        _run_feed(
            exchange=exchange,
            symbols=args.symbols,
            channels=args.channels,
            depth_speed_ms=args.depth_speed_ms,
            base_url=args.base_url,
            output=args.output,
            duration_seconds=args.duration_seconds,
            staleness=StalenessPolicy(
                stale_after_seconds=args.stale_after_seconds,
                max_event_lag_seconds=args.max_event_lag_seconds or None,
                reconnect_on_stale=args.reconnect_on_stale,
            )
            if args.stale_after_seconds > 0
            else None,
            status_file=args.status_file,
            rotate_bytes=args.rotate_bytes,
        )
    )


if __name__ == "__main__":
    main()
