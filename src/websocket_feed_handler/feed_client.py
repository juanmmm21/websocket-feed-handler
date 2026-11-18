from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import websockets
from websockets.asyncio.client import ClientConnection

from websocket_feed_handler.models import (
    ConnectionEvent,
    ConnectionState,
    DepthDiffEvent,
    FeedConfig,
    StreamChannel,
    TradeEvent,
)
from websocket_feed_handler.protocols import (
    ConnectionEventHandler,
    DepthEventHandler,
    DepthMessageParser,
    ExchangeFeedAdapter,
    TradeEventHandler,
    TradeMessageParser,
)
from websocket_feed_handler.reconnect import compute_backoff_delay
from websocket_feed_handler.staleness import StalenessMonitor

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
        self._monitor = StalenessMonitor(config.staleness) if config.staleness else None
        self.stale_reconnects = 0

    @property
    def state(self) -> ConnectionState:
        return self._state

    def health(self) -> dict[str, object]:
        """Connection state plus per-stream silence and lag (empty if staleness is off)."""
        payload: dict[str, object] = {"state": self._state.value}
        if self._monitor is not None:
            payload.update(self._monitor.health())
        return payload

    def _stream_names(self) -> tuple[str, ...]:
        return tuple(
            f"{symbol.upper()}@{channel.value}"
            for symbol in self._config.symbols
            for channel in self._config.channels
        )

    async def run(
        self,
        on_trade: TradeEventHandler,
        on_connection_event: ConnectionEventHandler | None = None,
        on_depth: DepthEventHandler | None = None,
    ) -> None:
        if StreamChannel.DEPTH in self._config.channels and on_depth is None:
            raise ValueError("on_depth is required when the depth channel is subscribed")
        attempt = 0
        self._stop_event.clear()

        while not self._stop_event.is_set():
            attempt += 1
            try:
                await self._connect_session(on_trade, on_connection_event, on_depth, attempt)
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
            delay = compute_backoff_delay(max(attempt, 1), self._config.reconnect_policy)
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
        on_depth: DepthEventHandler | None,
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
            watchdog: asyncio.Task[None] | None = None
            if self._monitor is not None:
                self._monitor.start(self._stream_names())
                watchdog = asyncio.create_task(
                    self._watch_staleness(connection, on_connection_event)
                )
            try:
                await self._consume(connection, on_trade, on_depth)
            finally:
                if watchdog is not None:
                    watchdog.cancel()
                    await asyncio.gather(watchdog, return_exceptions=True)

    async def _consume(
        self,
        connection: ClientConnection,
        on_trade: TradeEventHandler,
        on_depth: DepthEventHandler | None,
    ) -> None:
        parser = self._adapter.create_parser()
        depth_parser = (
            self._adapter.create_depth_parser()
            if StreamChannel.DEPTH in self._config.channels
            else None
        )

        async for raw_message in connection:
            if self._stop_event.is_set():
                break

            try:
                event = self._parse_message(parser, depth_parser, raw_message)
            except Exception:
                logger.exception(
                    "failed to parse message exchange=%s",
                    self._config.exchange.value,
                )
                continue

            if isinstance(event, TradeEvent):
                if self._monitor is not None:
                    self._monitor.observe(f"{event.symbol.upper()}@trade", event.event_time)
                await on_trade(event)
                await self._trade_queue.put(event)
            elif isinstance(event, DepthDiffEvent) and on_depth is not None:
                if self._monitor is not None:
                    self._monitor.observe(f"{event.symbol.upper()}@depth", event.event_time)
                await on_depth(event)

    async def _watch_staleness(
        self,
        connection: ClientConnection,
        on_connection_event: ConnectionEventHandler | None,
    ) -> None:
        monitor = self._monitor
        policy = self._config.staleness
        assert monitor is not None and policy is not None
        interval = min(1.0, policy.stale_after_seconds / 4)
        while True:
            await asyncio.sleep(interval)
            for transition in monitor.check():
                kind = "stale" if transition.stale else "fresh"
                logger.warning(
                    "feed %s stream=%s reason=%s", kind, transition.stream, transition.reason
                )
                await self._emit_connection_event(
                    on_connection_event, kind, f"{transition.stream}: {transition.reason}"
                )
            self._write_status()
            if policy.reconnect_on_stale and monitor.max_silence() > policy.stale_after_seconds:
                self.stale_reconnects += 1
                logger.warning("closing silent connection to force a reconnect")
                await connection.close()
                return

    def _write_status(self) -> None:
        path = self._config.status_path
        if path is None:
            return
        try:
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(self.health(), handle)
            os.replace(tmp, path)
        except OSError:
            logger.exception("could not write feed status to %s", path)

    async def _send_subscriptions(self, connection: ClientConnection) -> None:
        for message in self._adapter.build_subscribe_messages(self._config):
            await connection.send(json.dumps(message))

    def _parse_message(
        self,
        parser: TradeMessageParser,
        depth_parser: DepthMessageParser | None,
        raw_message: str | bytes,
    ) -> TradeEvent | DepthDiffEvent | None:
        decoded = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message

        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return None

        normalized = self._adapter.unwrap_payload(payload)
        trade = parser.parse(normalized)
        if trade is not None or depth_parser is None:
            return trade
        return depth_parser.parse(normalized)

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
