from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections.abc import Sequence

from websocket_feed_handler.exchanges import create_exchange_adapter
from websocket_feed_handler.feed_client import ResilientWebSocketFeed
from websocket_feed_handler.models import ConnectionEvent, ExchangeId, FeedConfig, TradeEvent


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
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging del proceso.",
    )
    return parser


async def _run_feed(exchange: ExchangeId, symbols: Sequence[str]) -> None:
    config = FeedConfig(exchange=exchange, symbols=tuple(symbols))
    adapter = create_exchange_adapter(exchange)
    feed = ResilientWebSocketFeed(config=config, adapter=adapter)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_shutdown)

    async def on_trade(trade: TradeEvent) -> None:
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
        feed.run(on_trade=on_trade, on_connection_event=on_connection_event),
    )
    stop_task = asyncio.create_task(stop_event.wait())

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


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    exchange = ExchangeId(args.exchange)
    asyncio.run(_run_feed(exchange=exchange, symbols=args.symbols))


if __name__ == "__main__":
    main()
