from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal


class ExchangeId(StrEnum):
    BINANCE = "binance"
    COINBASE = "coinbase"


class ConnectionState(StrEnum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"


class StreamChannel(StrEnum):
    TRADE = "trade"
    DEPTH = "depth"


class TradeSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class TradeEvent:
    """Evento normalizado de trade en tiempo real proveniente de un exchange."""

    exchange: ExchangeId
    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: TradeSide
    event_time: datetime
    is_buyer_maker: bool
    raw_payload: dict[str, object]

    def __post_init__(self) -> None:
        if self.price <= Decimal("0"):
            raise ValueError("price must be positive")
        if self.quantity <= Decimal("0"):
            raise ValueError("quantity must be positive")


DepthLevel = tuple[Decimal, Decimal]


@dataclass(frozen=True, slots=True)
class DepthDiffEvent:
    """Incremental L2 update (Binance ``depthUpdate``): absolute quantities, 0 removes a level.

    ``first_update_id``/``final_update_id`` (``U``/``u``) let the order book check that no
    update was lost: each event must start at the previous one's ``final_update_id + 1``.
    """

    exchange: ExchangeId
    symbol: str
    first_update_id: int
    final_update_id: int
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    event_time: datetime
    received_at: datetime

    def __post_init__(self) -> None:
        if self.final_update_id < self.first_update_id:
            raise ValueError("final_update_id must be >= first_update_id")


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    jitter_ratio: float = 0.1

    def __post_init__(self) -> None:
        if self.initial_delay_seconds <= 0:
            raise ValueError("initial_delay_seconds must be positive")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must be >= initial_delay_seconds")
        if self.backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")
        if not 0.0 <= self.jitter_ratio <= 1.0:
            raise ValueError("jitter_ratio must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class FeedConfig:
    exchange: ExchangeId
    symbols: tuple[str, ...]
    reconnect_policy: ReconnectPolicy = ReconnectPolicy()
    ping_interval_seconds: float = 20.0
    ping_timeout_seconds: float = 20.0
    close_timeout_seconds: float = 10.0
    channels: tuple[StreamChannel, ...] = (StreamChannel.TRADE,)
    depth_speed_ms: int = 100
    websocket_base_url: str | None = None

    def __post_init__(self) -> None:
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        if not self.channels:
            raise ValueError("channels must not be empty")
        if self.depth_speed_ms not in {100, 1000}:
            raise ValueError("depth_speed_ms must be 100 or 1000")
        if self.ping_interval_seconds <= 0:
            raise ValueError("ping_interval_seconds must be positive")
        if self.ping_timeout_seconds <= 0:
            raise ValueError("ping_timeout_seconds must be positive")


ConnectionEventType = Literal["connected", "disconnected", "reconnecting", "stopped", "error"]


@dataclass(frozen=True, slots=True)
class ConnectionEvent:
    exchange: ExchangeId
    event_type: ConnectionEventType
    message: str
    timestamp: datetime
    attempt: int | None = None


def utc_from_millis(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=UTC)


def utc_from_iso8601(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
