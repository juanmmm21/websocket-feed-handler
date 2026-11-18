"""The Rust accelerator and the Python fallback must give identical results."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from websocket_feed_handler import fast
from websocket_feed_handler.exchanges.binance import BinanceDepthDiffParser, BinanceTradeParser

rust = pytest.importorskip("websocket_feed_handler_rs", reason="Rust accelerator not built")

decimals = st.one_of(
    st.from_regex(r"[0-9]{1,15}(\.[0-9]{1,12})?", fullmatch=True),
    st.sampled_from(["", ".5", "5.", "-1", "1e3", "NaN", "1.2.3", " 1", "1234567890123456",
                     "1.1234567890123"]),
)
ints = st.one_of(
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.integers(min_value=2**63, max_value=2**70),
    st.booleans(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=3),
)
level = st.one_of(st.lists(decimals, min_size=2, max_size=2), st.lists(decimals, max_size=3),
                  st.integers())
depth = st.fixed_dictionaries(
    {"e": st.sampled_from(["depthUpdate", "trade", "other"]), "s": st.one_of(st.text(), ints),
     "U": ints, "u": ints, "E": ints, "b": st.lists(level, max_size=4),
     "a": st.lists(level, max_size=4)},
)
trade = st.fixed_dictionaries(
    {"e": st.sampled_from(["trade", "depthUpdate"]), "s": st.text(max_size=8), "t": ints,
     "p": decimals, "q": decimals, "T": ints, "m": st.one_of(st.booleans(), ints)},
)


def _outcome(func: Any, raw: str) -> Any:
    try:
        return ("ok", func(raw))
    except ValueError:
        return ("error", None)


def _raw(payload: dict[str, Any], wrap: bool, drop: str | None) -> str:
    if drop:
        payload = {k: v for k, v in payload.items() if k != drop}
    return json.dumps({"stream": "s", "data": payload} if wrap else payload)


@settings(max_examples=600, deadline=None)
@given(depth, st.booleans(), st.sampled_from([None, "U", "b", "E", "s"]))
def test_property_depth_parity(payload: dict[str, Any], wrap: bool, drop: str | None) -> None:
    raw = _raw(payload, wrap, drop)
    assert _outcome(rust.parse_depth, raw) == _outcome(fast.py_parse_depth, raw)


@settings(max_examples=600, deadline=None)
@given(trade, st.booleans(), st.sampled_from([None, "p", "m", "t"]))
def test_property_trade_parity(payload: dict[str, Any], wrap: bool, drop: str | None) -> None:
    raw = _raw(payload, wrap, drop)
    assert _outcome(rust.parse_trade, raw) == _outcome(fast.py_parse_trade, raw)


@pytest.mark.parametrize(
    "raw",
    ["", "[]", "nope", '{"e":"trade"} trailing', '{"e":"x","v":NaN}', '{"e":"x","v":Infinity}',
     '{"e":"depthUpdate","E":1,"s":"X","U":1,"u":1,"b":[],"a":[],"e":"other"}'],
)
def test_parity_on_awkward_json(raw: str) -> None:
    assert _outcome(rust.parse_depth, raw) == _outcome(fast.py_parse_depth, raw)
    assert _outcome(rust.parse_trade, raw) == _outcome(fast.py_parse_trade, raw)


@settings(max_examples=300, deadline=None)
@given(
    st.lists(st.tuples(st.from_regex(r"[1-9][0-9]{0,6}\.[0-9]{2,8}", fullmatch=True),
                       st.from_regex(r"[0-9]{1,3}\.[0-9]{1,8}", fullmatch=True)), max_size=6),
    st.integers(1, 10**12), st.integers(0, 50),
)
def test_property_fast_path_agrees_with_the_decimal_parser(
    bids: list[tuple[str, str]], first: int, span: int
) -> None:
    message = {"e": "depthUpdate", "E": 1700000000123, "s": "BTCUSDT", "U": first,
               "u": first + span, "b": [list(b) for b in bids], "a": [["1.00", "2"]]}
    event = BinanceDepthDiffParser().parse(message)
    parsed = fast.parse_depth_raw(json.dumps(message))
    assert event is not None and parsed is not None
    symbol, u_first, u_last, _, fast_bids, fast_asks = parsed
    assert (symbol, u_first, u_last) == (event.symbol, event.first_update_id,
                                         event.final_update_id)
    assert [(Decimal(p), Decimal(q)) for p, q in fast_bids] == list(event.bids)
    assert [(Decimal(p), Decimal(q)) for p, q in fast_asks] == list(event.asks)


def test_trade_fast_path_agrees_with_the_decimal_parser() -> None:
    message = {"e": "trade", "E": 1, "s": "BTCUSDT", "t": 9, "p": "65000.10", "q": "0.002",
               "T": 1700000000000, "m": True}
    event = BinanceTradeParser().parse(message)
    parsed = fast.parse_trade_raw(json.dumps(message))
    assert event is not None and parsed is not None
    assert (Decimal(parsed[2]), Decimal(parsed[3]), parsed[5]) == (event.price, event.quantity,
                                                                    event.is_buyer_maker)
