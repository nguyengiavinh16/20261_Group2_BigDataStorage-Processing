"""Build market features from a detector window."""

from collections import defaultdict

from cryptoguard_common.features import window_features
from cryptoguard_common.timeutil import US_PER_SECOND
from detector.window_data import TradeWindow


def feature_key(feature: dict) -> tuple[str, str, str]:
    return feature.get("replay_run_id") or "", feature["symbol"], feature["exchange"]


def features_for_window(window: TradeWindow, *, computed_at_ts_us: int) -> list[dict]:
    """Return a feature per active exchange and an ALL feature from the raw trades."""
    # The shared helper sorts by (event time, trade ID). Its stable sort uses this
    # input order as a tie-break when different exchanges reuse the same ID.
    trades = sorted(window.trades, key=lambda trade: (trade["exchange"], trade["event_id"]))
    by_exchange = defaultdict(list, {"ALL": trades})
    for trade in trades:
        by_exchange[trade["exchange"]].append(trade)

    window_seconds = (window.window_end_ts_us - window.window_start_ts_us) // US_PER_SECOND
    result = []
    for exchange, rows in by_exchange.items():
        complete = (
            window.is_complete if exchange == "ALL" else exchange not in window.late_exchanges
        )
        feature = window_features(
            rows,
            symbol=window.symbol,
            exchange=exchange,
            window_seconds=window_seconds,
            window_start_ts_us=window.window_start_ts_us,
            computed_at_ts_us=computed_at_ts_us,
            is_complete=complete,
            replay_run_id=window.replay_run_id,
        )
        if feature is not None:
            result.append(feature)
    return result
