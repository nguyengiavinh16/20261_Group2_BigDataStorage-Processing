import json

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import validate
from cryptoguard_common.paths import contracts_dir
from detector.cases import CaseTracker, build_alert
from detector.rules import evaluate_dump, evaluate_pump

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
