"""Test cho src/evaluation/feature_reference.py theo tests/fixtures/feature_cases.json (W2, owner: Thái).

Chạy từ thư mục gốc project:  python -m pytest tests/test_feature_reference.py -v
Fixture là nguồn sự thật của docs/feature-test-plan.md; Đạt (batch) và Công Anh (streaming) dùng lại đúng các event và
giá trị kỳ vọng này ở W4 để kiểm tra batch/stream nhất quán.
"""
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.evaluation import feature_reference as fr  # noqa: E402

FIXTURES = json.loads((ROOT / "tests" / "fixtures" / "feature_cases.json").read_text(encoding="utf-8"))
T0 = datetime.fromisoformat(FIXTURES["t0_for_tests"].replace("Z", "+00:00"))


def decode(x):
    """Đổi {"$float": "nan"|"inf"} thành số thực; JSON chuẩn không có NaN/Infinity."""
    if isinstance(x, dict):
        return float(x["$float"]) if set(x) == {"$float"} else {k: decode(v) for k, v in x.items()}
    if isinstance(x, list):
        return [decode(v) for v in x]
    return x


def case_ids(cases):
    return [c["id"] for c in cases]


def assert_error(excinfo, expect):
    assert excinfo.value.code == expect["error"]["code"]
    assert list(excinfo.value.fields) == expect["error"]["fields"]


def expected_features(spec):
    spec = dict(spec)
    ttype = spec.pop("type")
    return {**{f"type_{t}": 1.0 if t == ttype else 0.0 for t in fr.TRANSACTION_TYPES}, **spec}


# ---------------------------------------------------------------- feature không trạng thái

@pytest.mark.parametrize("case", FIXTURES["stateless"], ids=case_ids(FIXTURES["stateless"]))
def test_stateless_case(case):
    event, expect = decode(case["event"]), decode(case["expect"])
    if "error" in expect:
        with pytest.raises(fr.FeatureInputError) as excinfo:
            fr.stateless_features(event, case["feature_set"], t0=T0)
        assert_error(excinfo, expect)
        return
    got = fr.stateless_features(event, case["feature_set"], t0=T0)
    want = expected_features(expect["features"])
    assert list(got) == list(want), "thứ tự feature là một phần của đặc tả"
    assert got == pytest.approx(want, rel=1e-12, abs=1e-12)
    assert list(fr.quality_flags(event)) == expect.get("flags", [])


def test_feature_name_constants_match_output():
    event = {"transaction_type": "TRANSFER", "amount": 100.0, "step": 7,
             "source_balance_before": 500.0, "destination_balance_before": 0.0}
    assert tuple(fr.stateless_features(event, "A")) == fr.CORE_FEATURES
    assert tuple(fr.stateless_features(event, "A+T")) == fr.CORE_FEATURES + fr.TIME_FEATURES
    assert tuple(fr.stateless_features(event, "B")) == fr.CORE_FEATURES + fr.BALANCE_FEATURES
    assert tuple(fr.stateless_features(event, "A+H")) == fr.CORE_FEATURES  # phần lịch sử tính riêng


def test_larger_sets_extend_core_values():
    event = {"transaction_type": "CASH_OUT", "amount": 12345.67, "step": 100,
             "source_balance_before": 20000.0, "destination_balance_before": 5.0}
    core = fr.stateless_features(event, "A")
    for name in ("A+T", "B"):
        got = fr.stateless_features(event, name)
        assert {k: got[k] for k in core} == core


def test_hour_of_day_equals_utc_hour_when_t0_is_midnight():
    rng = random.Random(7)
    for _ in range(200):
        when = T0 + timedelta(seconds=rng.randrange(0, 743 * 3600))
        event = {"transaction_type": "TRANSFER", "amount": 1.0, "event_time": when.strftime("%Y-%m-%dT%H:%M:%SZ")}
        assert fr.stateless_features(event, "A+T", t0=T0)["hour_of_day"] == when.hour


@pytest.mark.parametrize("key", ["isFraud", "is_fraud", "IS_FRAUD", "isFlaggedFraud", "newbalanceOrig", "newbalance_dest",
                                 "new_balance_source", "source_balance_after", "Label", "fraud-label"])
def test_forbidden_key_variants(key):
    assert fr.forbidden_fields({"transaction_type": "TRANSFER", "amount": 1.0, key: 0}) == (key,)


def test_allowed_keys_are_not_flagged():
    event = {"transaction_id": "t", "transaction_type": "TRANSFER", "amount": 1.0, "source_account": "C1",
             "destination_account": "C2", "event_time": "2026-10-11T00:00:00Z", "source": "paysim_replay",
             "source_balance_before": 1.0, "destination_balance_before": 0.0}
    assert fr.forbidden_fields(event) == ()


# ---------------------------------------------------------------- feature lịch sử

@pytest.mark.parametrize("case", FIXTURES["history"], ids=case_ids(FIXTURES["history"]))
def test_history_case(case):
    events, expect = decode(case["events"]), decode(case["expect"])
    if "error" in expect:
        with pytest.raises(fr.FeatureInputError) as excinfo:
            fr.historical_features(events, t0=T0)
        assert_error(excinfo, expect)
        return
    got = fr.historical_features(events, t0=T0)
    assert set(got) == set(expect)
    for tid, feats in expect.items():
        assert list(got[tid]) == list(fr.HISTORY_FEATURES)
        assert got[tid] == pytest.approx(feats, abs=1e-9)


@pytest.mark.parametrize("seed", range(5))
def test_history_does_not_depend_on_arrival_order(seed):
    events = decode(next(c for c in FIXTURES["history"] if c["id"] == "HW-01")["events"])
    baseline = fr.historical_features(events)
    shuffled = events[:]
    random.Random(seed).shuffle(shuffled)
    assert fr.historical_features(shuffled) == baseline


def test_history_cold_start_undercounts():
    """Streaming khởi động nguội (không có trạng thái trước step 379) cho giá trị thấp hơn batch: cần warm-up 24 step."""
    prior = {"transaction_id": "p", "destination_account": "C1", "step": 370, "amount": 50.0}
    target = {"transaction_id": "t", "destination_account": "C1", "step": 380, "amount": 10.0}
    assert fr.historical_features([prior, target])["t"]["dest_txn_count_past_24h"] == 1.0
    assert fr.historical_features([target])["t"]["dest_txn_count_past_24h"] == 0.0


# ---------------------------------------------------------------- threshold

@pytest.mark.parametrize("case", FIXTURES["threshold_alert"], ids=case_ids(FIXTURES["threshold_alert"]))
def test_alert_case(case):
    score, threshold, expect = decode(case["score"]), decode(case["threshold"]), decode(case["expect"])
    if "error" in expect:
        with pytest.raises(fr.FeatureInputError) as excinfo:
            fr.alert_triggered(score, threshold)
        assert_error(excinfo, expect)
    else:
        assert fr.alert_triggered(score, threshold) is expect["alert"]


def build_select_inputs(case):
    if "scenario" not in case:
        return decode(case["y"]), decode(case["scores"])
    sc = case["scenario"]
    neg = [sc["neg_start"] - sc["neg_step"] * i for i in range(sc["neg_count"])]
    return ([1] * len(sc["pos_top"]) + [0] * len(neg) + [1] * len(sc["pos_bottom"]),
            sc["pos_top"] + neg + sc["pos_bottom"])


@pytest.mark.parametrize("case", FIXTURES["threshold_select"], ids=case_ids(FIXTURES["threshold_select"]))
def test_select_threshold_case(case):
    y, scores = build_select_inputs(case)
    kwargs = {"min_recall": case["min_recall"], "min_precision": case.get("min_precision")}
    expect = decode(case["expect"])
    if "error" in expect or "raises" in expect:
        exc = fr.FeatureInputError if "error" in expect else ValueError
        with pytest.raises(exc) as excinfo:
            fr.select_threshold(y, scores, **kwargs)
        if "error" in expect:
            assert_error(excinfo, expect)
        return
    order = np.random.default_rng(0).permutation(len(y))  # kết quả không phụ thuộc thứ tự mẫu
    for yy, ss in ((y, scores), (np.asarray(y)[order], np.asarray(scores)[order])):
        got = fr.select_threshold(yy, ss, **kwargs)
        for key in ("threshold", "precision", "recall"):
            assert got[key] == pytest.approx(expect[key], rel=1e-12)
        for key in ("alerts", "tp", "fp", "fn", "rule"):
            assert got[key] == expect[key]


def brute_force_threshold(y, s, min_recall, min_precision):
    n_pos = sum(y)
    rows = []
    for t in sorted(set(s), reverse=True):
        tp = sum(1 for yi, si in zip(y, s) if si >= t and yi == 1)
        fp = sum(1 for yi, si in zip(y, s) if si >= t and yi == 0)
        p, r = tp / (tp + fp), tp / n_pos
        rows.append((t, p, r, 5 * p * r / (4 * p + r) if 4 * p + r > 0 else 0.0))
    ok = [r for r in rows if r[2] >= min_recall and (min_precision is None or r[1] >= min_precision)]
    return max(ok, key=lambda r: (r[1], r[2], r[0])) if ok else max(rows, key=lambda r: (r[3], r[2], r[0]))


@pytest.mark.parametrize("seed", range(25))
@pytest.mark.parametrize("min_precision", [None, 0.3])
def test_select_threshold_matches_brute_force(seed, min_precision):
    rng = np.random.default_rng(seed)
    n = int(rng.integers(40, 160))
    y = (rng.random(n) < 0.2).astype(int)
    y[0] = 1
    s = np.round(np.clip(rng.normal(0.35 + 0.35 * y, 0.25), 0, 1), 1)  # làm tròn → nhiều giá trị trùng
    got = fr.select_threshold(y, s, min_recall=0.8, min_precision=min_precision)
    want = brute_force_threshold(y.tolist(), s.tolist(), 0.8, min_precision)
    assert got["threshold"] == pytest.approx(want[0])
    assert got["precision"] == pytest.approx(want[1])
    assert got["recall"] == pytest.approx(want[2])
    assert got["alerts"] == int((s >= got["threshold"]).sum())
