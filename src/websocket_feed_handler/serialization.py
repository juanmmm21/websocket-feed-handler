"""JSONL records for recorded streams (consumed by market-data-lakehouse and replays)."""

from __future__ import annotations

from typing import Any

from websocket_feed_handler.models import DepthDiffEvent, TradeEvent


def trade_to_record(trade: TradeEvent) -> dict[str, Any]:
    return {
        "type": "trade",
        "exchange": trade.exchange.value,
        "symbol": trade.symbol,
        "trade_id": trade.trade_id,
        "price": str(trade.price),
        "quantity": str(trade.quantity),
        "side": trade.side.value,
        "event_time": trade.event_time.isoformat(),
    }


def depth_diff_to_record(event: DepthDiffEvent) -> dict[str, Any]:
    return {
        "type": "depth_diff",
        "exchange": event.exchange.value,
        "symbol": event.symbol,
        "first_update_id": event.first_update_id,
        "final_update_id": event.final_update_id,
        "bids": [[str(price), str(quantity)] for price, quantity in event.bids],
        "asks": [[str(price), str(quantity)] for price, quantity in event.asks],
        "event_time": event.event_time.isoformat(),
        "received_at": event.received_at.isoformat(),
    }
