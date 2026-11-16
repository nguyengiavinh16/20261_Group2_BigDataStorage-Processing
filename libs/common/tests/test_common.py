import json

import pytest

from cryptoguard_common import binance_vision, contracts, features, timeutil
from cryptoguard_common.config import load_config
from cryptoguard_common.reference import native_symbols, symbol_map, token_registry


# ---------- timeutil ----------
@pytest.mark.parametrize(
    "value,expected",
    [
        (1794812400, 1794812400000000),  # seconds
        (1794812400121, 1794812400121000),  # ms (Binance WS)
        ("1794812400201", 1794812400201000),  # ms as string (OKX)
        (1794812400121000, 1794812400121000),  # µs (binance.vision 2025+)
        (1794812400121000123, 1794812400121000),  # ns
    ],
)
def test_epoch_to_us(value, expected):
    assert timeutil.epoch_to_us(value) == expected


def test_iso_roundtrip_keeps_microseconds():
    ts = timeutil.iso_to_us("2026-11-16T07:00:00.121234Z")
    assert ts == 1794812400121234
    assert timeutil.us_to_iso(ts) == "2026-11-16T07:00:00.121234Z"


def test_iso_nanoseconds_truncated():
    assert timeutil.iso_to_us("2026-11-16T07:00:00.121234999Z") == 1794812400121234


def test_floor_to_window():
    assert timeutil.floor_to_window(1794812459999999, 60) == 1794812400000000


# ---------- config ----------
def test_load_config_local_profile_and_env_override():
    cfg = load_config("local", env={"CG_DETECTOR__PUMP_RULE__VOLUME_Z_MIN": "4.5"})
    assert cfg["profile"] == "local"
    assert cfg["kafka"]["replication_factor"] == 1
    assert cfg["detector"]["pump_rule"]["volume_z_min"] == 4.5
    assert cfg["detector"]["pump_rule"]["taker_buy_ratio_min"] == 0.75  # untouched base value


def test_load_config_cloud_profile():
    cfg = load_config("cloud", env={})
    assert cfg["kafka"]["replication_factor"] == 3
    assert "coinbase" in cfg["exchanges"]["enabled"]


# ---------- contracts ----------
def test_validate_raises_on_bad_record():
    with pytest.raises(contracts.ContractError):
        contracts.validate({"schema_version": "1.0"}, "alert.v1")


def test_topic_registry_lookup():
    assert contracts.topic("canonical.trades.v1")["key"] == "symbol"
    assert contracts.replay_topic("alerts.v1") == "replay.alerts.v1"
    with pytest.raises(KeyError):
        contracts.topic("canonical.trade.v1")


# ---------- reference ----------
def test_reference_data():
    assert symbol_map()[("kraken", "BTC/USD")].symbol == "BTC-USD"
    usdt = token_registry()["0xdac17f958d2ee523a2206206994597c13d831ec7"]
    assert usdt.decimals == 6 and usdt.usd_pegged


def test_native_symbols():
    assert native_symbols("binance", ["BTC-USDT", "ETH-USDT"]) == ["BTCUSDT", "ETHUSDT"]
    assert native_symbols("coinbase", ["BTC-USDT"]) == []


# ---------- binance.vision parser ----------
@pytest.mark.parametrize(
    "time_value", ["1794812400121", "1794812400121000"]
)  # ms (old) and µs (2025+)
def test_binance_vision_row(time_value):
    row = ["5123349010", "91234.56", "0.012", "1094.81472", time_value, "True", "True"]
    rec = binance_vision.trade_row_to_canonical(
        row, "BTCUSDT", ingest_ts_us=1794812500000000, replay_run_id="r1"
    )
    contracts.validate(rec, "canonical_trade.v1")
    assert rec["taker_side"] == "sell"
    assert rec["event_ts_us"] == 1794812400121000


# ---------- features ----------
def _trades():
    from cryptoguard_common.paths import contracts_dir

    lines = (contracts_dir() / "samples" / "canonical_trade.valid.jsonl").read_text().splitlines()
    return [json.loads(x) for x in lines if x.strip()]


def test_window_features_matches_contract():
    rec = features.window_features(
        _trades(),
        symbol="BTC-USDT",
        exchange="ALL",
        window_seconds=60,
        window_start_ts_us=1794812400000000,
        computed_at_ts_us=1794812465000000,
    )
    contracts.validate(rec, "symbol_features_1m.v1")
    assert rec["trade_count"] == 2
    assert rec["open"] == 91234.56 and rec["close"] == 91230.1
    assert rec["taker_buy_ratio"] == pytest.approx(0.012 / 0.062)


def test_window_features_empty():
    assert (
        features.window_features(
            [],
            symbol="X-Y",
            exchange="ALL",
            window_seconds=60,
            window_start_ts_us=0,
            computed_at_ts_us=0,
        )
        is None
    )


def test_volume_zscore_and_severity():
    assert features.volume_zscore(None, 1, 1) is None
    assert features.volume_zscore(200.0, 100.0, 0.0) == pytest.approx(
        20.0
    )  # std floored at 5% of mean
    assert features.severity(0.93) == "critical"
    assert features.severity(0.5) == "medium"
