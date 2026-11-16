import json

import pytest

pytest.importorskip("prometheus_client")

from collectors.base import make_envelope  # noqa: E402
from collectors.binance import BinanceCollector  # noqa: E402
from collectors.okx import OkxCollector  # noqa: E402
from cryptoguard_common.contracts import validate  # noqa: E402


def _collector(cls, cfg, symbols):
    published = []
    c = cls(
        cfg, symbols, lambda t, k, e: published.append((t, k, e)), f"raw.trades.{cls.source}.v1"
    )
    return c, published


def test_make_envelope_matches_contract():
    env = make_envelope(
        source="binance",
        channel="trade",
        native_symbol="BTCUSDT",
        payload={"a": 1},
        collector_id="c",
        conn_id="x",
        seq=0,
        ingest_ts_us=1794812400190000,
    )
    validate(env, "raw_envelope.v1")


def test_binance_url_and_data_message():
    c, published = _collector(
        BinanceCollector, {"ws_url": "wss://stream.binance.com:9443/stream"}, ["BTCUSDT", "ETHUSDT"]
    )
    assert c.url().endswith("?streams=btcusdt@trade/ethusdt@trade")
    frame = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 1,
            "p": "1",
            "q": "1",
            "T": 1794812400121,
            "m": False,
        },
    }
    assert c.handle_raw(json.dumps(frame), "b-1", 0)
    assert not c.handle_raw(json.dumps({"result": None, "id": 1}), "b-1", 1)  # subscribe ack
    topic, key, env = published[0]
    assert (topic, key) == ("raw.trades.binance.v1", "BTCUSDT")
    validate(env, "raw_envelope.v1")


def test_invalid_json_is_kept_for_dlq():
    c, published = _collector(BinanceCollector, {"ws_url": "x"}, ["BTCUSDT"])
    assert c.handle_raw("{broken", "b-1", 0)
    assert published[0][2]["payload"] == "{broken"


def test_okx_subscribe_and_symbol():
    c, published = _collector(
        OkxCollector, {"ws_url": "wss://ws.okx.com:8443/ws/v5/public"}, ["BTC-USDT"]
    )
    assert c.subscribe_messages()[0]["args"] == [{"channel": "trades", "instId": "BTC-USDT"}]
    frame = {"arg": {"channel": "trades", "instId": "BTC-USDT"}, "data": [{"tradeId": "1"}]}
    assert c.handle_raw(json.dumps(frame), "o-1", 0)
    assert published[0][1] == "BTC-USDT"
