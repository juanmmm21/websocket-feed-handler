from __future__ import annotations

from websocket_feed_handler.exchanges.binance import BinanceDepthDiffParser, BinanceTradeParser
from websocket_feed_handler.exchanges.coinbase import CoinbaseTradeParser
from websocket_feed_handler.models import ExchangeId, FeedConfig, StreamChannel
from websocket_feed_handler.protocols import (
    DepthMessageParser,
    ExchangeFeedAdapter,
    TradeMessageParser,
)

BINANCE_WS_BASE_URL = "wss://stream.binance.com:9443"
# Market-data-only mirror documented by Binance; same payloads, no trading endpoints.
BINANCE_MARKET_DATA_WS_URL = "wss://data-stream.binance.vision"


class BinanceFeedAdapter:
    exchange_id = ExchangeId.BINANCE.value

    def build_websocket_url(self, config: FeedConfig) -> str:
        base = (config.websocket_base_url or BINANCE_WS_BASE_URL).rstrip("/")
        streams = [
            stream
            for symbol in config.symbols
            for stream in _binance_streams(_normalize_binance_symbol(symbol), config)
        ]
        if len(streams) == 1:
            return f"{base}/ws/{streams[0]}"
        return f"{base}/stream?streams={'/'.join(streams)}"

    def create_depth_parser(self) -> DepthMessageParser | None:
        return BinanceDepthDiffParser()

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
        if StreamChannel.DEPTH in config.channels:
            raise ValueError("depth diffs are only implemented for Binance")
        return config.websocket_base_url or "wss://ws-feed.exchange.coinbase.com"

    def create_depth_parser(self) -> DepthMessageParser | None:
        return None

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


def _binance_streams(symbol: str, config: FeedConfig) -> list[str]:
    depth = "depth@100ms" if config.depth_speed_ms == 100 else "depth"
    names = {StreamChannel.TRADE: "trade", StreamChannel.DEPTH: depth}
    return [f"{symbol}@{names[channel]}" for channel in config.channels]


def _normalize_binance_symbol(symbol: str) -> str:
    return symbol.replace("-", "").replace("/", "").lower()
