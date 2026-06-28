from __future__ import annotations

from decimal import Decimal

from websocket_feed_handler.exchanges.binance import BinanceTradeParser
from websocket_feed_handler.models import ExchangeId, TradeSide


def test_binance_trade_parser_normalizes_event() -> None:
    parser = BinanceTradeParser()
    payload = {
        "e": "trade",
        "E": 1672515782136,
        "s": "BNBBTC",
        "t": 12345,
        "p": "0.001",
        "q": "100",
        "T": 1672515782136,
        "m": True,
        "M": True,
    }

    trade = parser.parse(payload)

    assert trade is not None
    assert trade.exchange == ExchangeId.BINANCE
    assert trade.symbol == "BNBBTC"
    assert trade.trade_id == "12345"
    assert trade.price == Decimal("0.001")
    assert trade.quantity == Decimal("100")
    assert trade.side == TradeSide.SELL
    assert trade.is_buyer_maker is True


def test_binance_parser_ignores_non_trade_messages() -> None:
    parser = BinanceTradeParser()
    assert parser.parse({"e": "heartbeat"}) is None
