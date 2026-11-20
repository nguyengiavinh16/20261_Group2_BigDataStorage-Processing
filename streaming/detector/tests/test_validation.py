import json
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

from cryptoguard_common.contracts import validate
from detector.validation import (
    SourceRecord,
    TradeInputError,
    input_dlq_record,
    trade_dedup_key,
    validate_trade,
)


@pytest.fixture
def replay_record():
    trades = Path(__file__).with_name("fixtures") / "pump_dump_trades.jsonl"
    # The last pump trade is one microsecond before the next window.
    payload = trades.read_bytes().splitlines()[-3]
    return SourceRecord(b"BTC-USDT", payload, "replay.canonical.trades.v1", 2, 89)


def _update_payload(record, **fields):
    return replace(
        record, record_value=json.dumps(json.loads(record.record_value) | fields).encode()
    )


@pytest.mark.parametrize(
    ("value", "error_type"),
    [
        (None, "PARSE_ERROR"),
        (b"\xff", "PARSE_ERROR"),
        (b"{", "PARSE_ERROR"),
        (b'{"price":NaN}', "PARSE_ERROR"),
        (b'{"price":1e999}', "PARSE_ERROR"),
        (b"[]", "SCHEMA_VIOLATION"),
        (b"{}", "SCHEMA_VIOLATION"),
    ],
)
def test_rejects_malformed_payload_and_preserves_source(replay_record, value, error_type):
    record = replace(replay_record, record_value=value)
    with pytest.raises(TradeInputError) as exc:
        validate_trade(record, "replay")

    dlq = input_dlq_record(record, exc.value, step="validation")
    validate(dlq, "dlq_record.v1")
    assert dlq["error_type"] == error_type
    assert dlq["source_topic"] == replay_record.source_topic
    assert dlq["source_partition"] == 2
    assert dlq["source_offset"] == 89
    assert dlq["source_key"] == "BTC-USDT"
    assert dlq["raw_value"] == (
        "null" if value is None else value.decode("utf-8", errors="backslashreplace")
    )


@pytest.mark.parametrize(
    ("key", "error_type"),
    [(b"ETH-USDT", "INVALID_VALUE"), (None, "INVALID_VALUE"), (b"\xff", "PARSE_ERROR")],
)
def test_rejects_missing_or_inconsistent_key(replay_record, key, error_type):
    with pytest.raises(TradeInputError) as exc:
        validate_trade(replace(replay_record, record_key=key), "replay")
    assert exc.value.error_type == error_type


@pytest.mark.parametrize("run_id", [None, "", "  "])
def test_replay_requires_nonblank_run_id(replay_record, run_id):
    with pytest.raises(TradeInputError, match="require a non-empty replay_run_id"):
        validate_trade(_update_payload(replay_record, replay_run_id=run_id), "replay")


def test_live_requires_absent_or_null_replay_id(replay_record):
    with pytest.raises(TradeInputError, match="null or absent"):
        validate_trade(replay_record, "live")
    payload = json.loads(replay_record.record_value)
    payload.pop("replay_run_id")
    absent = replace(replay_record, record_value=json.dumps(payload).encode())
    for record in (absent, _update_payload(absent, replay_run_id=None)):
        assert trade_dedup_key(validate_trade(record, "live")) == ("", payload["event_id"])


def test_dedup_key_scopes_replay_runs_and_preserves_trade(replay_record):
    first = validate_trade(replay_record, "replay")
    second = validate_trade(_update_payload(replay_record, replay_run_id="another-run"), "replay")
    assert first.trade == json.loads(replay_record.record_value)
    assert type(first.trade["event_ts_us"]) is int
    assert first.source == replay_record
    assert trade_dedup_key(first)[1] == trade_dedup_key(second)[1]
    assert trade_dedup_key(first) != trade_dedup_key(second)


def test_dlq_preview_respects_utf8_size_limits(replay_record):
    record = replace(replay_record, record_value=b"\xff" * 65536)
    error = TradeInputError("PARSE_ERROR", "X" * 3000)
    payload = input_dlq_record(record, error, step="validation")
    validate(payload, "dlq_record.v1")
    assert len(payload["error_message"]) == 2000
    assert len(payload["raw_value"].encode("utf-8")) == 65536
    assert payload["raw_value"].startswith(r"\xff")


@pytest.mark.integration
def test_input_stream_routes_errors_and_deduplicates_per_run(replay_record):
    pytest.importorskip("pyflink")
    from pyflink.common import Row
    from pyflink.datastream import StreamExecutionEnvironment

    from detector.input_stream import prepare_trades

    other_run = _update_payload(replay_record, replay_run_id="another-run")
    incoming = [
        replace(replay_record, record_key=b"ETH-USDT", source_offset=0),
        replace(replay_record, source_offset=1),
        replace(replay_record, source_offset=2),
        replace(other_run, source_offset=3),
        replace(replay_record, record_value=b"\xff", source_offset=4),
    ]
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    records = env.from_collection([Row(**vars(record)) for record in incoming])
    accepted, dlq = prepare_trades(records, "replay", ttl_seconds=600)
    output = accepted.map(lambda record: ("accepted", record)).union(
        dlq.map(lambda error: ("rejected", error))
    )

    with closing(output.execute_and_collect()) as reader:
        rows = list(reader)

    good = {record.source.source_offset: record for status, record in rows if status == "accepted"}
    bad = {error["source_offset"]: error for status, error in rows if status == "rejected"}
    assert len(rows) == len(incoming)
    assert set(good) == {1, 3}
    assert good[1].trade["event_id"] == good[3].trade["event_id"]
    assert good[1].trade["replay_run_id"] != good[3].trade["replay_run_id"]
    assert good[1].trade["event_ts_us"] == good[3].trade["event_ts_us"]
    assert {offset: error["error_type"] for offset, error in bad.items()} == {
        0: "INVALID_VALUE",
        2: "DUPLICATE",
        4: "PARSE_ERROR",
    }
    for record in good.values():
        validate(record.trade, "canonical_trade.v1")
    for error in bad.values():
        validate(error, "dlq_record.v1")
