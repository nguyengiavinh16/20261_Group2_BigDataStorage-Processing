import json
from contextlib import closing
from pathlib import Path

import pytest

from cryptoguard_common.timeutil import US_PER_SECOND, floor_to_window
from detector.job import main
from detector.validation import SourceRecord, validate_trade


@pytest.mark.parametrize(
    "setting",
    ["WATERMARK_SECONDS", "ALLOWED_LATENESS_SECONDS", "WATERMARK_IDLE_TIMEOUT_SECONDS"],
)
def test_invalid_time_config_is_rejected_before_flink_starts(setting, monkeypatch, capsys):
    monkeypatch.setenv(f"CG_DETECTOR__{setting}", "-1")
    with pytest.raises(SystemExit, match="2"):
        main(["--mode", "replay"])
    assert setting.lower() in capsys.readouterr().err


@pytest.mark.integration
def test_bounded_replay_groups_by_event_minute_and_flushes_final_window():
    pytest.importorskip("pyflink")
    from pyflink.datastream import StreamExecutionEnvironment

    from detector.windows import assign_trade_watermarks, window_trades

    fixture = Path(__file__).with_name("fixtures") / "pump_dump_trades.jsonl"
    payloads = fixture.read_bytes().splitlines()
    last_pump_trade, first_dump_trade = payloads[-3:-1]
    pump = json.loads(last_pump_trade)
    dump = json.loads(first_dump_trade)
    second_run = json.dumps(pump | {"replay_run_id": "second-run"}).encode()
    # The dump arrives first even though it belongs in the following minute.
    incoming = [first_dump_trade, payloads[-4], last_pump_trade, second_run]
    trades = [
        validate_trade(
            SourceRecord(b"BTC-USDT", payload, "replay.canonical.trades.v1", 0, offset), "replay"
        )
        for offset, payload in enumerate(incoming)
    ]
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    timestamped = assign_trade_watermarks(
        env.from_collection(trades), watermark_seconds=5, idle_timeout_seconds=10
    )
    windows, dlq = window_trades(timestamped, allowed_lateness_seconds=30)
    output = windows.map(lambda window: ("window", window)).union(
        dlq.map(lambda error: ("dlq", error))
    )
    with closing(output.execute_and_collect()) as reader:
        rows = list(reader)

    assert len(rows) == 3
    assert all(kind == "window" for kind, _ in rows)
    by_window = {(window.replay_run_id, window.window_start_ts_us): window for _, window in rows}
    run_id = pump["replay_run_id"]
    pump_start = floor_to_window(pump["event_ts_us"], 60)
    dump_start = floor_to_window(dump["event_ts_us"], 60)
    assert dump_start - pump_start == 60 * US_PER_SECOND
    assert pump["event_ts_us"] == dump_start - 1
    assert dump["event_ts_us"] == dump_start
    assert set(by_window) == {
        (run_id, pump_start),
        (run_id, dump_start),
        ("second-run", pump_start),
    }
    combined = by_window[run_id, pump_start]
    assert combined.window_end_ts_us == dump_start
    assert {trade["exchange"] for trade in combined.trades} == {"binance", "okx"}
    assert {trade["event_ts_us"] for trade in combined.trades} == {
        json.loads(payloads[-4])["event_ts_us"],
        dump_start - 1,
    }
    assert by_window[run_id, dump_start].trades == (dump,)
    assert all(window.is_complete for window in by_window.values())
