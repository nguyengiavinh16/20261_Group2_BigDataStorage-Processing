import pytest

from cryptoguard_common.contracts import validate
from cryptoguard_common.timeutil import iso_to_us
from detector.demo import equivalence_key
from detector.historical_data import (
    MINUTE_US,
    baseline_snapshots,
    convert_trade,
    load_historical_data,
)


def test_btc_trade_uses_previous_closed_minute_for_usd():
    minute = iso_to_us("2020-09-17T17:59:00Z")
    rates = {minute - MINUTE_US: 10800.0, minute: 12000.0}
    row = ["123", "0.001", "5", "0.005", str((minute + 2000) // 1000), "false", "true"]
    trade = convert_trade(row, rates) | {"ingest_ts_us": minute + 10_000, "replay_run_id": "demo"}
    validate(trade, "canonical_trade.v1")
    assert trade["event_id"] == "binance:GVTBTC:123"
    assert trade["event_ts_us"] == minute + 2000
    assert trade["taker_side"] == "buy"
    assert trade["quote_qty_usd"] == 54.0


def test_baseline_counts_populated_minutes_with_complete_usd_only():
    hour = iso_to_us("2020-09-15T17:00:00Z")
    trades = []
    for index, quote in enumerate((100.0, 200.0, None)):
        stamp = hour + index * MINUTE_US
        trades.append(
            {
                "event_ts_us": stamp,
                "trade_id": str(index),
                "price": 0.01,
                "qty": 100.0,
                "quote_qty": 1.0,
                "quote_qty_usd": quote,
                "taker_side": "sell",
            }
        )
    snapshots = baseline_snapshots(trades)
    assert snapshots == {
        f"baseline:GVT-BTC:{venue}:17": {
            "mean_volume_usd": 150.0,
            "std_volume_usd": 50.0,
            "n_windows": 2,
            "as_of_date": "2020-09-15",
        }
        for venue in ("binance", "ALL")
    }


def test_run_comparison_ignores_run_id_clock_and_uuid_and_keeps_case():
    record = {
        "case_id": "case",
        "risk_score": 1.0,
        "alert_id": "first",
        "replay_run_id": "one",
        "detected_at_ts_us": 1000,
    }
    second = record | {"alert_id": "second", "replay_run_id": "two", "detected_at_ts_us": 2000}
    assert equivalence_key([{"record": record}]) == equivalence_key([{"record": second}])
    assert equivalence_key([{"record": record}]) != equivalence_key(
        [{"record": second | {"case_id": "other"}}]
    )


def test_historical_data_respects_configured_budget(tmp_path):
    with pytest.raises(ValueError, match="local data budget"):
        load_historical_data(tmp_path, {"max_days_history": 2, "max_symbols": 10})
