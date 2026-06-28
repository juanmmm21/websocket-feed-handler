from __future__ import annotations

from decimal import Decimal

from websocket_feed_handler.models import ExchangeId, TradeEvent, TradeSide, utc_from_millis


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
