"""Market feature definitions shared by the streaming detector [R3] and batch Gold jobs [R4].

Both engines MUST call these functions (Flink via a ProcessWindowFunction, Spark via applyInPandas)
so that features used for training equal features used live. Owner of the definitions: R3.
Changing a definition = PR reviewed by R3, R4 and R5.
"""

from collections.abc import Iterable, Mapping

SCHEMA_VERSION = "1.0"


def window_features(
    trades: Iterable[Mapping],
    *,
    symbol: str,
    exchange: str,
    window_seconds: int,
    window_start_ts_us: int,
    computed_at_ts_us: int,
    is_complete: bool = True,
    replay_run_id: str | None = None,
) -> dict | None:
    """Aggregate canonical trades of one window into a SymbolFeatures1m record (without baseline).

    Returns None for an empty window. Trades are ordered by (event_ts_us, trade_id) for open/close.
    volume_usd is None if any trade lacks quote_qty_usd. taker_buy_ratio ignores 'unknown' sides.
    """
    rows = sorted(trades, key=lambda t: (t["event_ts_us"], str(t["trade_id"])))
    if not rows:
        return None

    prices = [float(t["price"]) for t in rows]
    volume_base = sum(float(t["qty"]) for t in rows)
    quote_volume = sum(float(t["quote_qty"]) for t in rows)
    usd_values = [t.get("quote_qty_usd") for t in rows]
    volume_usd = None if any(v is None for v in usd_values) else sum(float(v) for v in usd_values)

    buy_qty = sum(float(t["qty"]) for t in rows if t["taker_side"] == "buy")
    known_qty = sum(float(t["qty"]) for t in rows if t["taker_side"] in ("buy", "sell"))

    open_, close = prices[0], prices[-1]
    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": symbol,
        "exchange": exchange,
        "window_seconds": window_seconds,
        "window_start_ts_us": window_start_ts_us,
        "window_end_ts_us": window_start_ts_us + window_seconds * 1_000_000,
        "trade_count": len(rows),
        "volume_base": volume_base,
        "volume_usd": volume_usd,
        "open": open_,
        "high": max(prices),
        "low": min(prices),
        "close": close,
        "vwap": quote_volume / volume_base if volume_base > 0 else close,
        "price_change_pct": (close - open_) / open_ * 100.0 if open_ > 0 else 0.0,
        "taker_buy_ratio": buy_qty / known_qty if known_qty > 0 else None,
        "volume_z": None,
        "baseline_volume_usd": None,
        "is_complete": is_complete,
        "computed_at_ts_us": computed_at_ts_us,
        "replay_run_id": replay_run_id,
    }


def volume_zscore(
    volume_usd: float | None,
    baseline_mean: float | None,
    baseline_std: float | None,
    min_std_ratio: float = 0.05,
) -> float | None:
    """z-score of window volume vs baseline. std is floored at min_std_ratio*mean so quiet coins don't explode."""
    if volume_usd is None or baseline_mean is None or baseline_std is None or baseline_mean <= 0:
        return None
    std = max(baseline_std, baseline_mean * min_std_ratio)
    return (volume_usd - baseline_mean) / std


def severity(risk_score: float) -> str:
    """Mapping fixed by alert.v1 description."""
    if risk_score >= 0.9:
        return "critical"
    if risk_score >= 0.7:
        return "high"
    if risk_score >= 0.5:
        return "medium"
    return "low"
