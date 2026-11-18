from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from websocket_feed_handler.models import (
    DepthDiffEvent,
    DepthLevel,
    ExchangeId,
    TradeEvent,
    TradeSide,
    utc_from_millis,
)


class BinanceTradeParser:
    """Parser del stream `<symbol>@trade` documentado en Binance Spot WebSocket Streams."""

    def parse(self, payload: dict[str, object]) -> TradeEvent | None:
        event_type = payload.get("e")
        if event_type != "trade":
            return None

        symbol = _require_str(payload, "s")
        trade_id = str(_require_int(payload, "t"))
        price = Decimal(_require_str(payload, "p"))
        quantity = Decimal(_require_str(payload, "q"))
        trade_time_ms = _require_int(payload, "T")
        is_buyer_maker = _require_bool(payload, "m")

        side = TradeSide.SELL if is_buyer_maker else TradeSide.BUY

        return TradeEvent(
            exchange=ExchangeId.BINANCE,
            symbol=symbol,
            trade_id=trade_id,
            price=price,
            quantity=quantity,
            side=side,
            event_time=utc_from_millis(trade_time_ms),
            is_buyer_maker=is_buyer_maker,
            raw_payload=payload,
        )


class BinanceDepthDiffParser:
    """Parser del stream ``<symbol>@depth[@100ms]`` (diff depth) de Binance Spot."""

    def parse(self, payload: dict[str, object]) -> DepthDiffEvent | None:
        if payload.get("e") != "depthUpdate":
            return None
        return DepthDiffEvent(
            exchange=ExchangeId.BINANCE,
            symbol=_require_str(payload, "s"),
            first_update_id=_require_int(payload, "U"),
            final_update_id=_require_int(payload, "u"),
            bids=_require_levels(payload, "b"),
            asks=_require_levels(payload, "a"),
            event_time=utc_from_millis(_require_int(payload, "E")),
            received_at=datetime.now(tz=UTC),
        )


def _require_levels(payload: dict[str, object], key: str) -> tuple[DepthLevel, ...]:
    raw = payload.get(key)
    if not isinstance(raw, list):
        raise ValueError(f"expected list field '{key}'")
    levels: list[DepthLevel] = []
    for entry in raw:
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError(f"malformed level in '{key}'")
        price, quantity = entry
        if not isinstance(price, str) or not isinstance(quantity, str):
            raise ValueError(f"level values in '{key}' must be strings")
        parsed_price = Decimal(price)
        parsed_quantity = Decimal(quantity)
        if parsed_price <= 0 or parsed_quantity < 0:
            raise ValueError(f"invalid level in '{key}'")
        levels.append((parsed_price, parsed_quantity))
    return tuple(levels)


def _require_str(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"expected string field '{key}'")
    return value


def _require_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"expected integer field '{key}'")
    return value


def _require_bool(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"expected boolean field '{key}'")
    return value
