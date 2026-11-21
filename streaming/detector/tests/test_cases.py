import json
from contextlib import closing

import pytest

from cryptoguard_common.config import load_config
from cryptoguard_common.contracts import validate
from cryptoguard_common.paths import contracts_dir
from cryptoguard_common.timeutil import US_PER_SECOND
from detector.baseline import ScoredFeature
from detector.cases import advance_case
from detector.job import main

CFG = load_config("local", env={})["detector"]
WINDOW_SECONDS = 60
WINDOW_US = WINDOW_SECONDS * US_PER_SECOND
GAP_US = CFG["case_merge_minutes"] * 60 * US_PER_SECOND
SAMPLE = json.loads(
    (contracts_dir() / "samples" / "symbol_features_1m.valid.jsonl")
    .read_text(encoding="utf-8")
    .splitlines()[0]
)
START_US = SAMPLE["window_start_ts_us"]


def scored(start_us=START_US, *, price_change_pct=None, **fields):
    feature = (
        SAMPLE
        | {
            "window_start_ts_us": start_us,
            "window_end_ts_us": start_us + WINDOW_US,
            "price_change_pct": (
                SAMPLE["price_change_pct"] if price_change_pct is None else price_change_pct
            ),
            "replay_run_id": "first-run",
        }
        | fields
    )
    validate(feature, "symbol_features_1m.v1")
    return ScoredFeature(feature, "redis", "hit")


def advance(previous, feature):
    return advance_case(previous, feature, CFG["pump_rule"], CFG["dump_rule"], gap_us=GAP_US)


@pytest.mark.parametrize("setting", ["-1", "1.5", "true"])
def test_invalid_case_gap_config(setting, monkeypatch, capsys):
    monkeypatch.setenv("CG_DETECTOR__CASE_MERGE_MINUTES", setting)
    with pytest.raises(SystemExit, match="2"):
        main(["--mode", "replay"])
    assert "detector.case_merge_minutes" in capsys.readouterr().err


def test_dump_at_merge_boundary_renews_case():
    opened, pump = advance(None, scored())
    assert pump.alert_type == "PUMP_SUSPECTED"
    boundary_us = START_US + WINDOW_US + GAP_US
    merged, dump = advance(opened, scored(boundary_us, price_change_pct=-25.0))
    assert dump.alert_type == "DUMP_PHASE"
    assert merged.case.case_id == opened.case.case_id
    assert merged.case.last_window_end_ts_us == boundary_us + WINDOW_US

    renewed, dump = advance(
        merged, scored(boundary_us + WINDOW_US + GAP_US, price_change_pct=-25.0)
    )
    assert dump.alert_type == "DUMP_PHASE"
    assert renewed.case.case_id == opened.case.case_id


def test_case_expires_one_microsecond_after_merge_boundary():
    opened, _ = advance(None, scored())
    expired_us = START_US + WINDOW_US + GAP_US + 1
    closed, dump = advance(opened, scored(expired_us, price_change_pct=-25.0))
    assert closed.case is None
    assert dump is None

    restarted, pump = advance(closed, scored(expired_us))
    assert pump.alert_type == "PUMP_SUSPECTED"
    assert restarted.case.case_id == f"{SAMPLE['symbol']}:{SAMPLE['exchange']}:{expired_us}"


def test_revisions_can_emit_first_alert_for_latest_window_only():
    quiet, hit = advance(None, scored(price_change_pct=0.0))
    assert hit is None
    opened, hit = advance(quiet, scored(is_complete=False))
    assert hit.alert_type == "PUMP_SUSPECTED"
    assert "INCOMPLETE_WINDOW" in {reason["code"] for reason in hit.reasons}
    assert advance(opened, scored()) == (opened, None)

    end_us = START_US + WINDOW_US
    newer, hit = advance(opened, scored(end_us, price_change_pct=0.0))
    assert hit is None
    assert advance(newer, scored(START_US, price_change_pct=-25.0)) == (newer, None)
    assert newer.case.last_window_end_ts_us == end_us


def test_historical_pump_does_not_reopen_case_after_dump_without_pump():
    dump, hit = advance(None, scored(START_US + WINDOW_US, price_change_pct=-25.0))
    assert hit is None
    assert dump.case is None
    assert advance(dump, scored()) == (dump, None)


@pytest.mark.integration
def test_flink_cases_are_isolated_by_run_exchange_and_symbol():
    pytest.importorskip("pyflink")
    from pyflink.datastream import StreamExecutionEnvironment

    from detector.case_stream import case_alerts

    later_us = START_US + WINDOW_US
    inputs = [
        scored(exchange="binance"),
        scored(exchange="binance"),
        scored(exchange="ALL", replay_run_id=None),
        scored(later_us, exchange="binance", price_change_pct=-25.0),
        scored(later_us, exchange="binance", symbol="ETH-USDT", price_change_pct=-25.0),
        scored(later_us, exchange="binance", replay_run_id="second-run", price_change_pct=-25.0),
        scored(later_us, exchange="binance", replay_run_id=None, price_change_pct=-25.0),
        scored(later_us, exchange="okx", price_change_pct=-25.0),
        scored(later_us, exchange="ALL", price_change_pct=-25.0),
        scored(later_us, exchange="ALL", replay_run_id=None, price_change_pct=-25.0),
    ]
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    alerts = case_alerts(
        env.from_collection(inputs),
        CFG["pump_rule"],
        CFG["dump_rule"],
        case_merge_minutes=CFG["case_merge_minutes"],
        window_seconds=WINDOW_SECONDS,
        allowed_lateness_seconds=CFG["allowed_lateness_seconds"],
    )
    with closing(alerts.execute_and_collect()) as reader:
        output = list(reader)

    for alert in output:
        validate(alert, "alert.v1")
        assert alert["evidence"]["baseline_source"] == "redis"
        assert alert["symbol"] == SAMPLE["symbol"]
        assert alert["case_id"] == f"{SAMPLE['symbol']}:{alert['exchange']}:{START_US}"
    assert len(output) == 4
    assert {(a["replay_run_id"], a["exchange"], a["alert_type"]) for a in output} == {
        ("first-run", "binance", "PUMP_SUSPECTED"),
        ("first-run", "binance", "DUMP_PHASE"),
        (None, "ALL", "PUMP_SUSPECTED"),
        (None, "ALL", "DUMP_PHASE"),
    }
