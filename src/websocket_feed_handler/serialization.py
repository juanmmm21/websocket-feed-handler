"""JSONL records for recorded streams (consumed by market-data-lakehouse and replays)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import IO, Any

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


class RotatingJsonlSink:
    """Append-only JSONL writer, line-buffered so tailing readers see whole lines at once.

    With ``max_bytes`` the writer itself rotates (``name`` → ``name.1``, replacing the previous
    ``.1``) before a write would exceed the limit; a reader that follows inodes loses nothing.
    """

    def __init__(self, path: str | Path, *, max_bytes: int = 0) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        self._handle: IO[str] = open(self._path, "a", encoding="utf-8", buffering=1)  # noqa: SIM115
        self.rotations = 0

    def write(self, record: dict[str, Any]) -> None:
        line = json.dumps(record) + "\n"
        if self._max_bytes and self._handle.tell() + len(line) > self._max_bytes:
            self._handle.close()
            os.replace(self._path, self._path.with_name(self._path.name + ".1"))
            self._handle = open(self._path, "a", encoding="utf-8", buffering=1)  # noqa: SIM115
            self.rotations += 1
        self._handle.write(line)

    def close(self) -> None:
        self._handle.close()
