import json
from dataclasses import replace

import pytest

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import validate
from cryptoguard_common.paths import contracts_dir
from detector.baseline import ScoredFeature
from detector.cases import CaseTracker, build_alert
from detector.rules import evaluate_dump, evaluate_pump, evaluate_scored_feature

CFG = load_config("local", env={})["detector"]


def _pump_features() -> dict:
    line = (
        (contracts_dir() / "samples" / "symbol_features_1m.valid.jsonl").read_text().splitlines()[0]
    )
    return json.loads(line)


def test_pump_sample_fires_and_alert_is_valid():
    feats = _pump_features()
    hit = evaluate_pump(feats, CFG["pump_rule"])
    assert hit is not None and hit.alert_type == "PUMP_SUSPECTED"
    assert 0.9 <= hit.risk_score <= 1.0
    tracker = CaseTracker(CFG["case_merge_minutes"])
    case_id = tracker.assign(
        feats["symbol"],
        feats["exchange"],
        feats["window_start_ts_us"],
        feats["window_end_ts_us"],
        hit.alert_type,
    )
    alert = build_alert(hit, feats, case_id)
    validate(alert, "alert.v1")
    assert alert["severity"] == "critical"
    assert {r["code"] for r in alert["reasons"]} >= {
        "VOLUME_Z_HIGH",
        "PRICE_SPIKE",
        "TAKER_BUY_DOMINANT",
    }


def test_pump_needs_all_conditions():
    feats = _pump_features() | {"taker_buy_ratio": 0.6}
    assert evaluate_pump(feats, CFG["pump_rule"]) is None
    assert evaluate_pump(_pump_features() | {"volume_z": None}, CFG["pump_rule"]) is None


def test_dump_joins_open_pump_case():
    feats = _pump_features()
    tracker = CaseTracker(CFG["case_merge_minutes"])
    case1 = tracker.assign(
        "ABC-USDT",
        "binance",
        feats["window_start_ts_us"],
        feats["window_end_ts_us"],
        "PUMP_SUSPECTED",
    )
    later_start = feats["window_end_ts_us"] + 4 * 60_000_000
    dump_feats = feats | {
        "window_start_ts_us": later_start,
        "window_end_ts_us": later_start + 60_000_000,
        "price_change_pct": -35.0,
    }
    assert tracker.open_pump_case("ABC-USDT", "binance", later_start)
    hit = evaluate_dump(dump_feats, CFG["dump_rule"], pump_case_open=True)
    assert hit is not None
    case2 = tracker.assign(
        "ABC-USDT", "binance", later_start, later_start + 60_000_000, hit.alert_type
    )
    assert case1 == case2
    validate(build_alert(hit, dump_feats, case2), "alert.v1")


def test_new_case_after_gap():
    tracker = CaseTracker(30)
    a = tracker.assign("X-USDT", "binance", 0, 60_000_000, "PUMP_SUSPECTED")
    b = tracker.assign(
        "X-USDT",
        "binance",
        60_000_000 + 31 * 60_000_000,
        60_000_000 + 32 * 60_000_000,
        "PUMP_SUSPECTED",
    )
    assert a != b


def _pump_at_thresholds():
    rule = CFG["pump_rule"]
    return _pump_features() | {
        "volume_z": rule["volume_z_min"],
        "price_change_pct": rule["price_change_pct_min"],
        "taker_buy_ratio": rule["taker_buy_ratio_min"],
        "trade_count": rule["min_trades"],
    }


def _eval(features, *, pump_case_open=False, baseline_source="redis"):
    return evaluate_scored_feature(
        ScoredFeature(
            features, baseline_source, "hit" if baseline_source == "redis" else "missing"
        ),
        CFG["pump_rule"],
        CFG["dump_rule"],
        pump_case_open=pump_case_open,
    )


@pytest.mark.parametrize(
    ("field", "step"),
    [
        ("volume_z", 0.001),
        ("price_change_pct", 0.001),
        ("taker_buy_ratio", 0.0001),
        ("trade_count", 1),
    ],
)
def test_each_pump_threshold_is_inclusive(field, step):
    features = _pump_at_thresholds()
    value = features[field]
    assert _eval(features | {field: value - step}) is None
    for allowed in (value, value + step):
        hit = _eval(features | {field: allowed})
        assert hit.alert_type == "PUMP_SUSPECTED"
        assert hit.rule_version == CFG["pump_rule"]["version"]
        assert 0 <= hit.risk_score <= 1


def test_dump_requires_open_case_and_accepts_threshold():
    limit = CFG["dump_rule"]["price_change_pct_max"]
    feature = _pump_at_thresholds()
    assert _eval(feature | {"price_change_pct": limit}, pump_case_open=False) is None
    assert _eval(feature | {"price_change_pct": limit + 0.001}, pump_case_open=True) is None
    for allowed in (limit, limit - 0.001):
        hit = _eval(feature | {"price_change_pct": allowed}, pump_case_open=True)
        assert hit.alert_type == "DUMP_PHASE"
        assert hit.rule_version == CFG["dump_rule"]["version"]
        assert 0 <= hit.risk_score <= 1


@pytest.mark.parametrize("missing", ["source", "zscore"])
def test_baseline_gate_applies_to_both_rules(missing):
    feature = _pump_at_thresholds()
    source = None if missing == "source" else "redis"
    if missing == "zscore":
        feature["volume_z"] = None
    assert _eval(feature, baseline_source=source) is None
    dump = feature | {"price_change_pct": CFG["dump_rule"]["price_change_pct_max"]}
    assert _eval(dump, pump_case_open=True, baseline_source=source) is None


def test_unknown_taker_side_blocks_pump():
    assert _eval(_pump_at_thresholds() | {"taker_buy_ratio": None}) is None


@pytest.mark.parametrize("phase", ["pump", "dump"])
@pytest.mark.parametrize("is_complete", [True, False])
def test_alert_explains_incomplete_windows_and_ema_source(phase, is_complete):
    feature = _pump_at_thresholds() | {"is_complete": is_complete}
    if phase == "dump":
        feature["price_change_pct"] = CFG["dump_rule"]["price_change_pct_max"]
    hit = _eval(feature, pump_case_open=(phase == "dump"), baseline_source="ema")
    case_id = f"{feature['symbol']}:{feature['exchange']}:{feature['window_start_ts_us']}"
    alert = build_alert(hit, feature, case_id)
    validate(alert, "alert.v1")
    assert alert["evidence"]["baseline_source"] == "ema"
    assert alert["evidence"]["is_complete"] == is_complete
    assert sum(reason["code"] == "INCOMPLETE_WINDOW" for reason in alert["reasons"]) == (
        0 if is_complete else 1
    )
    assert all({"code", "value", "threshold"} <= reason.keys() for reason in alert["reasons"])


@pytest.mark.parametrize(
    ("risk", "expected"),
    [
        (0.4999, "low"),
        (0.5, "medium"),
        (0.6999, "medium"),
        (0.7, "high"),
        (0.8999, "high"),
        (0.9, "critical"),
    ],
)
def test_alert_severity_boundaries(risk, expected):
    feature = _pump_at_thresholds()
    hit = replace(_eval(feature), risk_score=risk)
    case_id = f"{feature['symbol']}:{feature['exchange']}:{feature['window_start_ts_us']}"
    alert = build_alert(hit, feature, case_id)
    validate(alert, "alert.v1")
    assert alert["severity"] == expected
