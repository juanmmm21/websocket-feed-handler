from __future__ import annotations

from decimal import Decimal

from websocket_feed_handler.exchanges.coinbase import CoinbaseTradeParser
from websocket_feed_handler.models import ExchangeId, TradeSide


def test_coinbase_trade_parser_normalizes_event() -> None:
    parser = CoinbaseTradeParser()
    payload = {
        "type": "match",
        "trade_id": 10,
        "maker_order_id": "maker",
        "taker_order_id": "taker",
        "side": "buy",
        "size": "5.23512",
        "price": "400.23",
        "product_id": "BTC-USD",
        "time": "2014-11-07T08:19:27.028459Z",
    }

    trade = parser.parse(payload)

    assert trade is not None
    assert trade.exchange == ExchangeId.COINBASE
    assert trade.symbol == "BTC-USD"
    assert trade.trade_id == "10"
    assert trade.price == Decimal("400.23")
    assert trade.quantity == Decimal("5.23512")
    assert trade.side == TradeSide.BUY
    assert trade.is_buyer_maker is False


def test_coinbase_parser_ignores_non_trade_messages() -> None:
    parser = CoinbaseTradeParser()
    assert parser.parse({"type": "subscriptions"}) is None
