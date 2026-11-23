import json
import os
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest

from cryptoguard_common.contracts import validate
from cryptoguard_common.features import window_features
from cryptoguard_common.timeutil import floor_to_window
from detector.baseline import BaselineLookup, BaselineSettings, RedisBaselineReader, score_feature

MINUTE_US = 60_000_000


@pytest.fixture
def feature():
    fixture = Path(__file__).with_name("fixtures") / "pump_dump_trades.jsonl"
    trade = json.loads(fixture.read_bytes().splitlines()[-3])
    start = floor_to_window(trade["event_ts_us"], 60)
    return window_features(
        [trade],
        symbol=trade["symbol"],
        exchange=trade["exchange"],
        window_seconds=60,
        window_start_ts_us=start,
        computed_at_ts_us=start + MINUTE_US,
        replay_run_id=trade["replay_run_id"],
    )


def _snapshot(**updates):
    return json.dumps(
        {
            "mean_volume_usd": 100,
            "std_volume_usd": 10,
            "n_windows": 10,
            "as_of_date": "2025-12-31",
        }
        | updates
    )


def _reader(get, settings=None, **kwargs):
    return RedisBaselineReader(
        get, settings or BaselineSettings(), redis_errors=(TimeoutError,), **kwargs
    )


@pytest.mark.parametrize(
    ("payload", "status"),
    [
        (None, "missing"),
        ("{", "invalid"),
        (_snapshot(mean_volume_usd=True), "invalid"),
        (_snapshot(std_volume_usd=float("inf")), "invalid"),
        (_snapshot(n_windows=1), "insufficient_samples"),
        (_snapshot(as_of_date="2025-12-28"), "stale"),
        (_snapshot(as_of_date="2026-01-01"), "future"),
        (_snapshot(as_of_date="2026-01-02"), "future"),
    ],
)
def test_lookup_rejects_unusable_replay_snapshots(feature, payload, status):
    lookup = _reader(lambda key: payload).lookup(feature)
    assert lookup == BaselineLookup(None, status)
    scored, _ = score_feature(feature, lookup, None, BaselineSettings())
    validate(scored.feature, "symbol_features_1m.v1")
    assert scored.baseline_source is None
    assert scored.feature["volume_z"] is None


def test_redis_utc_key_and_std_floor_preserve_schema(feature):
    keys = []

    def get(key):
        keys.append(key)
        return _snapshot(std_volume_usd=0, as_of_date="2026-01-01")

    live_all = feature | {"replay_run_id": None, "exchange": "ALL"}
    lookup = _reader(get).lookup(live_all)
    scored, _ = score_feature(live_all, lookup, None, BaselineSettings())
    assert keys == ["baseline:BTC-USDT:ALL:10"]
    assert scored.baseline_source == "redis"
    assert scored.feature["baseline_volume_usd"] == 100
    assert scored.feature["volume_z"] == 4
    validate(scored.feature, "symbol_features_1m.v1")

    missing_volume, _ = score_feature(
        live_all | {"volume_usd": None}, lookup, None, BaselineSettings()
    )
    validate(missing_volume.feature, "symbol_features_1m.v1")
    assert missing_volume.feature["volume_z"] is None


def test_cache_is_bounded_expires_and_isolates_runs(feature):
    keys = []
    now = 0

    def get(key):
        keys.append(key)
        return _snapshot()

    reader = _reader(get, BaselineSettings(cache_seconds=2, cache_max_entries=2), clock=lambda: now)
    assert reader.lookup(feature).status == reader.lookup(feature).status == "hit"
    assert len(keys) == 1
    assert reader.lookup(feature | {"replay_run_id": "another-run"}).status == "hit"
    assert keys == ["baseline:BTC-USDT:binance:10"] * 2
    assert reader.lookup(feature | {"exchange": "ALL"}).status == "hit"
    assert len(reader.cache) == 2
    reader.lookup(feature)
    assert len(keys) == 4  # The original entry was evicted.
    now += 2
    reader.lookup(feature)
    assert len(keys) == 5


def test_lookup_distinguishes_timeout_and_recovers(feature):
    responses = iter([TimeoutError(), _snapshot()])

    def get(key):
        response = next(responses)
        if isinstance(response, TimeoutError):
            raise response
        return response

    reader = _reader(get)
    assert reader.lookup(feature).status == "redis_error"
    assert reader.lookup(feature).status == "hit"


def _at_minute(feature, minute, volume):
    return feature | {
        "window_start_ts_us": feature["window_start_ts_us"] + minute * MINUTE_US,
        "window_end_ts_us": feature["window_end_ts_us"] + minute * MINUTE_US,
        "computed_at_ts_us": feature["computed_at_ts_us"] + minute * MINUTE_US,
        "volume_usd": volume,
    }


def test_ema_uses_only_prior_windows_and_revises_latest_sample(feature):
    settings = BaselineSettings(ema_min_windows=2)
    missing = BaselineLookup(None, "missing")
    timed_out = BaselineLookup(None, "redis_error")
    state = None
    for minute in (0, 1):
        scored, state = score_feature(_at_minute(feature, minute, 20), missing, state, settings)
        validate(scored.feature, "symbol_features_1m.v1")
        assert scored.feature["volume_z"] is None
    scored, state = score_feature(_at_minute(feature, 2, 30), timed_out, state, settings)
    assert scored.baseline_source == "ema"
    assert scored.redis_status == "redis_error"
    assert scored.feature["volume_z"] == 10
    validate(scored.feature, "symbol_features_1m.v1")
    revised, state = score_feature(_at_minute(feature, 2, 35), missing, state, settings)
    assert revised.feature["volume_z"] == 15
    validate(revised.feature, "symbol_features_1m.v1")
    next_minute, advanced = score_feature(_at_minute(feature, 3, 40), missing, state, settings)
    assert next_minute.feature["baseline_volume_usd"] == 23
    assert advanced.history.n_windows == 3
    validate(next_minute.feature, "symbol_features_1m.v1")
    older, unchanged = score_feature(_at_minute(feature, 1, 100), missing, advanced, settings)
    assert older.feature["volume_z"] is None
    assert unchanged == advanced
    validate(older.feature, "symbol_features_1m.v1")


@pytest.mark.integration
def test_flink_scores_redis_and_scopes_ema_per_replay_run(feature):
    url = os.getenv("CG_DETECTOR_TEST_REDIS_URL")
    if not url:
        pytest.skip("Set CG_DETECTOR_TEST_REDIS_URL to a disposable Redis instance")
    redis = pytest.importorskip("redis")
    pytest.importorskip("pyflink")
    from pyflink.datastream import StreamExecutionEnvironment

    from detector.baseline_stream import enrich_features

    feature = feature | {"symbol": "M06" + uuid4().hex[:8].upper() + "-USDT"}
    key = f"baseline:{feature['symbol']}:ALL:10"
    with closing(redis.Redis.from_url(url)) as client:
        client.set(key, _snapshot(), ex=180)
    rows = [
        _at_minute(feature, 0, 20),
        _at_minute(feature, 1, 20),
        _at_minute(feature, 2, 30),
        _at_minute(feature | {"replay_run_id": "second-run"}, 2, 30),
        _at_minute(feature | {"exchange": "ALL"}, 0, 130),
    ]
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    scored = enrich_features(env.from_collection(rows), url, BaselineSettings(ema_min_windows=2))
    with closing(scored.execute_and_collect()) as reader:
        outputs = list(reader)
    assert len(outputs) == 5
    for output in outputs:
        validate(output.feature, "symbol_features_1m.v1")
    assert [(row.baseline_source, row.feature["volume_z"]) for row in outputs] == [
        (None, None),
        (None, None),
        ("ema", 10),
        (None, None),
        ("redis", 3),
    ]
