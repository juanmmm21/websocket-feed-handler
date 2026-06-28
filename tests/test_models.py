from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from websocket_feed_handler.models import ExchangeId, TradeEvent, TradeSide


def test_trade_event_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError):
        TradeEvent(
            exchange=ExchangeId.BINANCE,
            symbol="BTCUSDT",
            trade_id="1",
            price=Decimal("0"),
            quantity=Decimal("1"),
            side=TradeSide.BUY,
            event_time=datetime.now(tz=UTC),
            is_buyer_maker=False,
            raw_payload={},
        )
