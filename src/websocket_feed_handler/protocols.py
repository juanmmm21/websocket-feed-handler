from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

from websocket_feed_handler.models import ConnectionEvent, FeedConfig, TradeEvent


class TradeMessageParser(Protocol):
    """Contrato para transformar payloads crudos de un exchange en eventos normalizados."""

    def parse(self, payload: dict[str, object]) -> TradeEvent | None:
        """Retorna None si el mensaje no corresponde a un trade ejecutado."""
        ...


class ExchangeFeedAdapter(Protocol):
    """Contrato que encapsula la lógica específica de cada exchange."""

    exchange_id: str

    def build_websocket_url(self, config: FeedConfig) -> str:
        ...

    def build_subscribe_messages(self, config: FeedConfig) -> list[dict[str, object]]:
        ...

    def create_parser(self) -> TradeMessageParser:
        ...

    def unwrap_payload(self, raw_message: dict[str, object]) -> dict[str, object]:
        ...


TradeEventHandler = Callable[[TradeEvent], Awaitable[None]]
ConnectionEventHandler = Callable[[ConnectionEvent], Awaitable[None]]


class FeedHandler(Protocol):
    """Interfaz pública del cliente de ingesta en tiempo real."""

    async def run(
        self,
        on_trade: TradeEventHandler,
        on_connection_event: ConnectionEventHandler | None = None,
    ) -> None:
        ...

    async def stop(self) -> None:
        ...

    def trades(self) -> AsyncIterator[TradeEvent]:
        ...
