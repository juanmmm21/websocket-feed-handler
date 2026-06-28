from __future__ import annotations

from decimal import Decimal

from websocket_feed_handler.models import ExchangeId, TradeEvent, TradeSide, utc_from_iso8601


class CoinbaseTradeParser:
    """Parser del canal `matches` del WebSocket feed de Coinbase Exchange."""

    def parse(self, payload: dict[str, object]) -> TradeEvent | None:
        message_type = payload.get("type")
        if message_type != "match":
            return None

        symbol = _require_str(payload, "product_id")
        trade_id = str(_require_int(payload, "trade_id"))
        price = Decimal(_require_str(payload, "price"))
        quantity = Decimal(_require_str(payload, "size"))
        side = TradeSide(_require_str(payload, "side"))
        event_time = utc_from_iso8601(_require_str(payload, "time"))
        is_buyer_maker = side == TradeSide.SELL

        return TradeEvent(
            exchange=ExchangeId.COINBASE,
            symbol=symbol,
            trade_id=trade_id,
            price=price,
            quantity=quantity,
            side=side,
            event_time=event_time,
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
