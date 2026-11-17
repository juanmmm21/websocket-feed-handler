"""Hot-path parsing of raw Binance messages, with an optional Rust accelerator.

``parse_depth_raw`` and ``parse_trade_raw`` return plain tuples whose decimal fields are the
validated **original strings** (no ``Decimal`` built yet), so a latency-sensitive consumer can
keep strings end to end. If the ``websocket-feed-handler-rs`` wheel is installed (extra
``[rust]``) the Rust implementation is used; otherwise the pure-Python one below. Both accept
the same grammar and fail the same way (``None`` for other message kinds, ``ValueError`` for
malformed ones); property tests check that they agree. ``QUANT_DISABLE_RUST=1`` forces Python.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

Level = tuple[str, str]
DepthTuple = tuple[str, int, int, int, list[Level], list[Level]]
TradeTuple = tuple[str, int, str, str, int, bool]

_DECIMAL = re.compile(r"[0-9]{1,15}(\.[0-9]{1,12})?")
_I64 = (-(2**63), 2**63 - 1)


def is_plain_decimal(text: str) -> bool:
    return _DECIMAL.fullmatch(text) is not None


def _reject_constant(name: str) -> Any:
    raise ValueError(f"invalid json: {name} is not allowed")


def _object(raw: str | bytes) -> dict[str, Any]:
    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    try:
        value = json.loads(text, parse_constant=_reject_constant)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid json: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError("message must be a JSON object")
    inner = value.get("data")
    return inner if isinstance(inner, dict) else value


def _text(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"expected string field '{key}'")
    return value


def _integer(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not _I64[0] <= value <= _I64[1]:
        raise ValueError(f"expected integer field '{key}'")
    return value


def _decimal(payload: dict[str, Any], key: str) -> str:
    value = _text(payload, key)
    if not is_plain_decimal(value):
        raise ValueError(f"invalid decimal in '{key}'")
    return value


def _levels(payload: dict[str, Any], key: str) -> list[Level]:
    items = payload.get(key)
    if not isinstance(items, list):
        raise ValueError(f"expected list field '{key}'")
    out: list[Level] = []
    for item in items:
        if not isinstance(item, list) or len(item) != 2:
            raise ValueError(f"malformed level in '{key}'")
        price, quantity = item
        if not (
            isinstance(price, str)
            and isinstance(quantity, str)
            and is_plain_decimal(price)
            and is_plain_decimal(quantity)
        ):
            raise ValueError(f"invalid level in '{key}'")
        out.append((price, quantity))
    return out


def py_parse_depth(raw: str | bytes) -> DepthTuple | None:
    payload = _object(raw)
    if payload.get("e") != "depthUpdate":
        return None
    symbol = _text(payload, "s")
    first = _integer(payload, "U")
    last = _integer(payload, "u")
    if last < first:
        raise ValueError("final_update_id must be >= first_update_id")
    event_time = _integer(payload, "E")
    return symbol, first, last, event_time, _levels(payload, "b"), _levels(payload, "a")


def py_parse_trade(raw: str | bytes) -> TradeTuple | None:
    payload = _object(raw)
    if payload.get("e") != "trade":
        return None
    maker = payload.get("m")
    if not isinstance(maker, bool):
        raise ValueError("expected boolean field 'm'")
    return (
        _text(payload, "s"),
        _integer(payload, "t"),
        _decimal(payload, "p"),
        _decimal(payload, "q"),
        _integer(payload, "T"),
        maker,
    )


def _load_rust() -> Any:
    if os.environ.get("QUANT_DISABLE_RUST") == "1":
        return None
    try:
        import websocket_feed_handler_rs
    except ImportError:
        return None
    return websocket_feed_handler_rs


_RUST = _load_rust()
BACKEND = "rust" if _RUST is not None else "python"


def parse_depth_raw(raw: str | bytes) -> DepthTuple | None:
    if _RUST is not None:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        result: DepthTuple | None = _RUST.parse_depth(text)
        return result
    return py_parse_depth(raw)


def parse_trade_raw(raw: str | bytes) -> TradeTuple | None:
    if _RUST is not None:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        result: TradeTuple | None = _RUST.parse_trade(text)
        return result
    return py_parse_trade(raw)
