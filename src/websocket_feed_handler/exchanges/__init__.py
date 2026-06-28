from __future__ import annotations

from websocket_feed_handler.exchanges.binance import BinanceTradeParser
from websocket_feed_handler.exchanges.coinbase import CoinbaseTradeParser
from websocket_feed_handler.models import ExchangeId, FeedConfig
from websocket_feed_handler.protocols import ExchangeFeedAdapter, TradeMessageParser


class BinanceFeedAdapter:
    exchange_id = ExchangeId.BINANCE.value

    def build_websocket_url(self, config: FeedConfig) -> str:
        if len(config.symbols) == 1:
            stream = f"{_normalize_binance_symbol(config.symbols[0])}@trade"
            return f"wss://stream.binance.com:9443/ws/{stream}"

        stream_names = (
            f"{_normalize_binance_symbol(symbol)}@trade" for symbol in config.symbols
        )
        streams = "/".join(stream_names)
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    def build_subscribe_messages(self, config: FeedConfig) -> list[dict[str, object]]:
        return []

    def create_parser(self) -> TradeMessageParser:
        return BinanceTradeParser()

    def unwrap_payload(self, raw_message: dict[str, object]) -> dict[str, object]:
        data = raw_message.get("data")
        if isinstance(data, dict):
            return data
        return raw_message


class CoinbaseFeedAdapter:
    exchange_id = ExchangeId.COINBASE.value

    def build_websocket_url(self, config: FeedConfig) -> str:
        return "wss://ws-feed.exchange.coinbase.com"

    def build_subscribe_messages(self, config: FeedConfig) -> list[dict[str, object]]:
        return [
            {
                "type": "subscribe",
                "product_ids": list(config.symbols),
                "channels": ["matches"],
            }
        ]

    def create_parser(self) -> TradeMessageParser:
        return CoinbaseTradeParser()

    def unwrap_payload(self, raw_message: dict[str, object]) -> dict[str, object]:
        return raw_message


def create_exchange_adapter(exchange: ExchangeId) -> ExchangeFeedAdapter:
    if exchange == ExchangeId.BINANCE:
        return BinanceFeedAdapter()
    if exchange == ExchangeId.COINBASE:
        return CoinbaseFeedAdapter()
    raise ValueError(f"unsupported exchange: {exchange.value}")


def _normalize_binance_symbol(symbol: str) -> str:
    return symbol.replace("-", "").replace("/", "").lower()
