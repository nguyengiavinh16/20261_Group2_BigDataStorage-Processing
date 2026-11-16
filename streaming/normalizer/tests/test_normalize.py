import json

from cryptoguard_common.contracts import validate
from cryptoguard_common.paths import contracts_dir
from normalizer.normalize import normalize_envelope


def _raw_samples() -> list[str]:
    path = contracts_dir() / "samples" / "raw_envelope.valid.jsonl"
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_binance_sample_normalizes_to_buy():
    trades, dlq = normalize_envelope(_raw_samples()[0], source_topic="raw.trades.binance.v1")
    assert not dlq
    (t,) = trades
    validate(t, "canonical_trade.v1")
    assert t["symbol"] == "BTC-USDT" and t["taker_side"] == "buy"
    assert t["event_ts_us"] == 1794812400121000
    assert t["event_id"] == "binance:BTCUSDT:5123349010"


def test_okx_sample_normalizes_to_sell():
    trades, dlq = normalize_envelope(_raw_samples()[1], source_topic="raw.trades.okx.v1")
    assert not dlq and trades[0]["taker_side"] == "sell" and trades[0]["exchange"] == "okx"


def test_unparseable_payload_goes_to_dlq():
    trades, dlq = normalize_envelope(
        _raw_samples()[2], source_topic="raw.trades.binance.v1", offset=7
    )
    assert not trades
    validate(dlq[0], "dlq_record.v1")
    assert dlq[0]["error_type"] == "PARSE_ERROR" and dlq[0]["source_offset"] == 7


def test_unknown_symbol_goes_to_dlq():
    env = json.loads(_raw_samples()[0])
    env["payload"]["data"]["s"] = "FOOBARUSDT"
    trades, dlq = normalize_envelope(json.dumps(env), source_topic="raw.trades.binance.v1")
    assert not trades and dlq[0]["error_type"] == "UNKNOWN_SYMBOL"


def test_broken_envelope_goes_to_dlq():
    trades, dlq = normalize_envelope("{not json", source_topic="raw.trades.binance.v1")
    assert not trades and dlq[0]["error_type"] == "PARSE_ERROR"
