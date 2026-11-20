"""Offline BTC-USDT fixture for DET-02, DET-05 and DET-06."""

import json
from collections import defaultdict
from pathlib import Path
from uuid import UUID

import pytest

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import validate
from cryptoguard_common.features import volume_zscore, window_features
from cryptoguard_common.timeutil import US_PER_SECOND, floor_to_window, iso_to_us
from detector.cases import CaseTracker, build_alert
from detector.rules import evaluate_dump, evaluate_pump

FIXTURES = Path(__file__).with_name("fixtures")
WINDOW_SECONDS = 60
WINDOW_US = WINDOW_SECONDS * US_PER_SECOND
PUMP_START_US = iso_to_us("2026-01-01T10:01:00Z")
CLOCK_US = iso_to_us("2026-01-01T10:03:05Z")
RUN_ID = "synthetic-btc-pump-dump"
BASELINES = {"binance": (1000, 200), "ALL": (1200, 200)}


def _read_jsonl(name):
    text = (FIXTURES / f"pump_dump_{name}.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines()]


def _run_offline(trades):
    """Wire the existing helpers for one synthetic replay, with no services."""
    config = load_config("local", env={})["detector"]
    tracker = CaseTracker(config["case_merge_minutes"])
    grouped = defaultdict(list)
    seen = set()
    for trade in trades:
        validate(trade, "canonical_trade.v1")
        assert trade["replay_run_id"] == RUN_ID
        event_id = trade["event_id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        start_us = floor_to_window(trade["event_ts_us"], WINDOW_SECONDS)
        for exchange in (trade["exchange"], "ALL"):
            grouped[start_us, trade["symbol"], exchange].append(trade)

    features = []
    alerts = []
    for (start_us, symbol, exchange), rows in sorted(grouped.items()):
        feature = window_features(
            rows,
            symbol=symbol,
            exchange=exchange,
            window_seconds=WINDOW_SECONDS,
            window_start_ts_us=start_us,
            computed_at_ts_us=CLOCK_US,
            replay_run_id=RUN_ID,
        )
        assert feature is not None
        mean, std = BASELINES.get(exchange, (None, None))
        feature["baseline_volume_usd"] = mean
        feature["volume_z"] = volume_zscore(feature["volume_usd"], mean, std)
        validate(feature, "symbol_features_1m.v1")
        features.append(feature)

        if feature["volume_z"] is None:
            continue
        hit = evaluate_pump(feature, config["pump_rule"])
        if hit is None:
            open_case = tracker.open_pump_case(symbol, exchange, start_us)
            hit = evaluate_dump(feature, config["dump_rule"], pump_case_open=open_case)
        if hit is not None:
            case_id = tracker.assign(
                symbol, exchange, start_us, feature["window_end_ts_us"], hit.alert_type
            )
            alert = build_alert(hit, feature, case_id, detected_at_ts_us=CLOCK_US)
            validate(alert, "alert.v1")
            alerts.append(alert)

    return features, alerts


@pytest.mark.parametrize("variant", ["original", "reversed", "duplicate"])
def test_pump_dump_offline(variant):
    trades = _read_jsonl("trades")
    assert len({trade["event_id"] for trade in trades}) == len(trades) == 26
    if variant == "reversed":
        trades.reverse()
    elif variant == "duplicate":
        last_pump_trade = next(
            trade for trade in trades if trade["event_ts_us"] == PUMP_START_US + WINDOW_US - 1
        )
        trades.append(last_pump_trade.copy())

    features, alerts = _run_offline(trades)
    expected_features = _read_jsonl("features")
    expected_alerts = _read_jsonl("alerts")
    assert len(features) == len(expected_features) == 8
    assert len(alerts) == len(expected_alerts) == 4

    for feature, expected in zip(features, expected_features, strict=True):
        assert {field: feature[field] for field in expected} == {
            field: pytest.approx(value, rel=0, abs=1e-9) if isinstance(value, float) else value
            for field, value in expected.items()
        }
        assert feature["symbol"] == "BTC-USDT"
        assert feature["window_seconds"] == WINDOW_SECONDS
        assert feature["window_end_ts_us"] == expected["window_start_ts_us"] + WINDOW_US
        assert feature["computed_at_ts_us"] == CLOCK_US
        assert feature["is_complete"] is True
        assert feature["replay_run_id"] == RUN_ID

    by_window = {(row["exchange"], row["window_start_ts_us"]): row for row in features}
    for alert, expected in zip(alerts, expected_alerts, strict=True):
        assert {field: alert[field] for field in expected} == expected
        assert UUID(alert["alert_id"]).version == 4
        assert alert["symbol"] == "BTC-USDT"
        assert alert["detected_at_ts_us"] == CLOCK_US
        assert alert["replay_run_id"] == RUN_ID
        feature = by_window[alert["exchange"], alert["window_start_ts_us"]]
        assert alert["window_end_ts_us"] == feature["window_end_ts_us"]
        assert alert["evidence"] == {
            field: feature[field]
            for field in (
                "trade_count",
                "volume_usd",
                "baseline_volume_usd",
                "volume_z",
                "open",
                "close",
                "price_change_pct",
                "taker_buy_ratio",
                "is_complete",
            )
        }
    assert len({alert["case_id"] for alert in alerts}) == 2
    assert len({alert["alert_id"] for alert in alerts}) == 4


@pytest.mark.parametrize(
    ("updates", "missing"),
    [
        ({"quote_qty_usd": None}, ("volume_usd", "volume_z")),
        ({"taker_side": "unknown"}, ("taker_buy_ratio",)),
    ],
    ids=["unknown-usd", "unknown-side"],
)
def test_pump_window_with_missing_trade_data(updates, missing):
    trades = _read_jsonl("trades")
    for trade in trades:
        if PUMP_START_US <= trade["event_ts_us"] < PUMP_START_US + WINDOW_US:
            trade.update(updates)

    features, alerts = _run_offline(trades)
    pump_features = [row for row in features if row["window_start_ts_us"] == PUMP_START_US]
    assert {row["exchange"] for row in pump_features} == {"binance", "okx", "ALL"}
    assert all(row[field] is None for row in pump_features for field in missing)
    assert alerts == []
