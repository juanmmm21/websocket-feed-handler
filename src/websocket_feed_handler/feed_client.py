from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection

from websocket_feed_handler.models import (
    ConnectionEvent,
    ConnectionState,
    FeedConfig,
    TradeEvent,
)
from websocket_feed_handler.protocols import (
    ConnectionEventHandler,
    ExchangeFeedAdapter,
    TradeEventHandler,
)
from websocket_feed_handler.reconnect import compute_backoff_delay

logger = logging.getLogger(__name__)


class ResilientWebSocketFeed:
    """Cliente WebSocket genérico con reconexión automática y parseo delegado al adapter."""

    def __init__(self, config: FeedConfig, adapter: ExchangeFeedAdapter) -> None:
        self._config = config
        self._adapter = adapter
        self._state = ConnectionState.DISCONNECTED
        self._stop_event = asyncio.Event()
        self._trade_queue: asyncio.Queue[TradeEvent | None] = asyncio.Queue()
        self._connection: ClientConnection | None = None

    @property
    def state(self) -> ConnectionState:
        return self._state

    async def run(
        self,
        on_trade: TradeEventHandler,
        on_connection_event: ConnectionEventHandler | None = None,
    ) -> None:
        attempt = 0
        self._stop_event.clear()

        while not self._stop_event.is_set():
            attempt += 1
            try:
                await self._connect_session(on_trade, on_connection_event, attempt)
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    "feed session failed exchange=%s attempt=%s",
                    self._config.exchange.value,
                    attempt,
                )
                await self._emit_connection_event(
                    on_connection_event,
                    "error",
                    f"session error: {exc}",
                    attempt,
                )

            if self._stop_event.is_set():
                break

            self._state = ConnectionState.RECONNECTING
            delay = compute_backoff_delay(attempt, self._config.reconnect_policy)
            await self._emit_connection_event(
                on_connection_event,
                "reconnecting",
                f"retrying in {delay:.2f}s",
                attempt,
            )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except TimeoutError:
                continue

        self._state = ConnectionState.STOPPED
        await self._trade_queue.put(None)
        await self._emit_connection_event(on_connection_event, "stopped", "feed stopped")

    async def stop(self) -> None:
        self._stop_event.set()
        connection = self._connection
        if connection is not None:
            await connection.close()

    async def trades(self) -> AsyncIterator[TradeEvent]:
        while True:
            trade = await self._trade_queue.get()
            if trade is None:
                break
            yield trade

    async def _connect_session(
        self,
        on_trade: TradeEventHandler,
        on_connection_event: ConnectionEventHandler | None,
        attempt: int,
    ) -> None:
        url = self._adapter.build_websocket_url(self._config)
        self._state = ConnectionState.CONNECTING
        logger.info("connecting exchange=%s url=%s", self._config.exchange.value, url)

        async with websockets.connect(
            url,
            ping_interval=self._config.ping_interval_seconds,
            ping_timeout=self._config.ping_timeout_seconds,
            close_timeout=self._config.close_timeout_seconds,
            open_timeout=self._config.close_timeout_seconds,
            max_queue=None,
        ) as connection:
            self._connection = connection
            self._state = ConnectionState.CONNECTED
            await self._emit_connection_event(
                on_connection_event,
                "connected",
                "websocket session established",
                attempt if attempt > 1 else None,
            )

            await self._send_subscriptions(connection)
            parser = self._adapter.create_parser()

            async for raw_message in connection:
                if self._stop_event.is_set():
                    break

                try:
                    trade = self._parse_trade_message(parser, raw_message)
                except Exception:
                    logger.exception(
                        "failed to parse message exchange=%s",
                        self._config.exchange.value,
                    )
                    continue

                if trade is None:
                    continue

                await on_trade(trade)
                await self._trade_queue.put(trade)

    async def _send_subscriptions(self, connection: ClientConnection) -> None:
        for message in self._adapter.build_subscribe_messages(self._config):
            await connection.send(json.dumps(message))

    def _parse_trade_message(
        self,
        parser: Any,
        raw_message: str | bytes,
    ) -> TradeEvent | None:
        decoded = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message

        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return None

        normalized = self._adapter.unwrap_payload(payload)
        return parser.parse(normalized)

    async def _emit_connection_event(
        self,
        handler: ConnectionEventHandler | None,
        event_type: str,
        message: str,
        attempt: int | None = None,
    ) -> None:
        if handler is None:
            return

        event = ConnectionEvent(
            exchange=self._config.exchange,
            event_type=event_type,  # type: ignore[arg-type]
            message=message,
            timestamp=datetime.now(tz=UTC),
            attempt=attempt,
        )
        try:
            await handler(event)
        except Exception:
            logger.exception("connection event handler failed")
