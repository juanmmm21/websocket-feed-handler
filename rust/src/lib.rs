//! Binance message parsing for the hot path.
//!
//! Mirrors `websocket_feed_handler/fast.py` exactly: same accepted grammar, same `None` for
//! messages of another kind, same `ValueError` for malformed ones. Decimal fields are validated
//! and returned as the original strings, so callers decide when (and whether) to build Decimals.

// pyo3 0.22 #[pyfunction]/#[pymethods] expansions trip this lint on `PyResult` returns.
#![allow(clippy::useless_conversion)]

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde_json::{Map, Value};

type Level = (String, String);
type DepthTuple = (String, i64, i64, i64, Vec<Level>, Vec<Level>);
type TradeTuple = (String, i64, String, String, i64, bool);

const MAX_INT_DIGITS: usize = 15;
const MAX_FRAC_DIGITS: usize = 12;

/// `^[0-9]{1,15}(\.[0-9]{1,12})?$` — the decimal strings Binance sends.
pub fn is_plain_decimal(text: &str) -> bool {
    let (int_part, frac_part) = match text.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (text, None),
    };
    let digits = |s: &str, max: usize| {
        !s.is_empty() && s.len() <= max && s.bytes().all(|b| b.is_ascii_digit())
    };
    digits(int_part, MAX_INT_DIGITS) && frac_part.is_none_or(|f| digits(f, MAX_FRAC_DIGITS))
}

fn err(message: String) -> PyErr {
    PyValueError::new_err(message)
}

fn object(raw: &str) -> PyResult<Map<String, Value>> {
    let value: Value = serde_json::from_str(raw).map_err(|e| err(format!("invalid json: {e}")))?;
    match value {
        Value::Object(mut map) => {
            // combined streams wrap the payload: {"stream": ..., "data": {...}}
            if let Some(Value::Object(inner)) = map.remove("data") {
                return Ok(inner);
            }
            Ok(map)
        }
        _ => Err(err("message must be a JSON object".into())),
    }
}

fn text<'a>(map: &'a Map<String, Value>, key: &str) -> PyResult<&'a str> {
    match map.get(key) {
        Some(Value::String(s)) => Ok(s.as_str()),
        _ => Err(err(format!("expected string field '{key}'"))),
    }
}

fn integer(map: &Map<String, Value>, key: &str) -> PyResult<i64> {
    match map.get(key) {
        Some(Value::Number(n)) if n.is_i64() => Ok(n.as_i64().unwrap_or_default()),
        _ => Err(err(format!("expected integer field '{key}'"))),
    }
}

fn decimal(map: &Map<String, Value>, key: &str) -> PyResult<String> {
    let value = text(map, key)?;
    if !is_plain_decimal(value) {
        return Err(err(format!("invalid decimal in '{key}'")));
    }
    Ok(value.to_string())
}

fn levels(map: &Map<String, Value>, key: &str) -> PyResult<Vec<Level>> {
    let Some(Value::Array(items)) = map.get(key) else {
        return Err(err(format!("expected list field '{key}'")));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::Array(pair) if pair.len() == 2 => match (&pair[0], &pair[1]) {
                (Value::String(p), Value::String(q))
                    if is_plain_decimal(p) && is_plain_decimal(q) =>
                {
                    out.push((p.clone(), q.clone()))
                }
                _ => return Err(err(format!("invalid level in '{key}'"))),
            },
            _ => return Err(err(format!("malformed level in '{key}'"))),
        }
    }
    Ok(out)
}

/// (symbol, first_update_id, final_update_id, event_time_ms, bids, asks) or None.
#[pyfunction]
fn parse_depth(raw: &str) -> PyResult<Option<DepthTuple>> {
    let map = object(raw)?;
    if map.get("e") != Some(&Value::String("depthUpdate".into())) {
        return Ok(None);
    }
    let symbol = text(&map, "s")?.to_string();
    let first = integer(&map, "U")?;
    let last = integer(&map, "u")?;
    if last < first {
        return Err(err("final_update_id must be >= first_update_id".into()));
    }
    let event_time = integer(&map, "E")?;
    Ok(Some((
        symbol,
        first,
        last,
        event_time,
        levels(&map, "b")?,
        levels(&map, "a")?,
    )))
}

/// (symbol, trade_id, price, quantity, trade_time_ms, buyer_is_maker) or None.
#[pyfunction]
fn parse_trade(raw: &str) -> PyResult<Option<TradeTuple>> {
    let map = object(raw)?;
    if map.get("e") != Some(&Value::String("trade".into())) {
        return Ok(None);
    }
    let maker = match map.get("m") {
        Some(Value::Bool(b)) => *b,
        _ => return Err(err("expected boolean field 'm'".into())),
    };
    Ok(Some((
        text(&map, "s")?.to_string(),
        integer(&map, "t")?,
        decimal(&map, "p")?,
        decimal(&map, "q")?,
        integer(&map, "T")?,
        maker,
    )))
}

#[pymodule]
fn websocket_feed_handler_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(parse_depth, module)?)?;
    module.add_function(wrap_pyfunction!(parse_trade, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_plain_decimal;

    #[test]
    fn grammar() {
        assert!(is_plain_decimal("65000.12000000"));
        assert!(is_plain_decimal("0"));
        for bad in [
            "",
            ".5",
            "5.",
            "-1",
            "1e3",
            "NaN",
            "1.2.3",
            " 1",
            "1234567890123456",
        ] {
            assert!(!is_plain_decimal(bad), "{bad}");
        }
    }
}
