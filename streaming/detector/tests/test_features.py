import json
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

import pytest

from cryptoguard_common.contracts import validate
from cryptoguard_common.timeutil import US_PER_SECOND, floor_to_window, iso_to_us
from detector.features import features_for_window
from detector.window_data import TradeWindow

FIXTURES = Path(__file__).with_name("fixtures")
WINDOW_SECONDS = 60
WINDOW_US = WINDOW_SECONDS * US_PER_SECOND
CLOCK_US = iso_to_us("2026-01-01T10:03:05Z")


@pytest.fixture
def fixture_windows():
    trades = (FIXTURES / "pump_dump_trades.jsonl").read_text(encoding="utf-8").splitlines()
    by_minute = defaultdict(list)
    for line in trades:
        trade = json.loads(line)
        validate(trade, "canonical_trade.v1")
        by_minute[floor_to_window(trade["event_ts_us"], WINDOW_SECONDS)].append(trade)
    return [
        TradeWindow(
            symbol=rows[0]["symbol"],
            replay_run_id=rows[0]["replay_run_id"],
            window_start_ts_us=start,
            window_end_ts_us=start + WINDOW_US,
            trades=tuple(rows),
            late_exchanges=(),
        )
        for start, rows in sorted(by_minute.items())
    ]


def test_window_features_match_offline_fixture(fixture_windows):
    expected_file = FIXTURES / "pump_dump_features.jsonl"
    expected = [json.loads(line) for line in expected_file.read_text(encoding="utf-8").splitlines()]
    actual = [
        row
        for window in fixture_windows
        for row in features_for_window(window, computed_at_ts_us=CLOCK_US)
    ]
    assert len(actual) == len(expected) == 8
    for row, fixture_row in zip(actual, expected, strict=True):
        validate(row, "symbol_features_1m.v1")
        for field, value in fixture_row.items():
            if field in {"baseline_volume_usd", "volume_z"}:
                assert row[field] is None
            elif isinstance(value, float):
                assert row[field] == pytest.approx(value, rel=0, abs=1e-10)
            else:
                assert row[field] == value
        assert row["window_end_ts_us"] == row["window_start_ts_us"] + WINDOW_US
        assert row["window_seconds"] == WINDOW_SECONDS
        assert row["computed_at_ts_us"] == CLOCK_US
        assert row["replay_run_id"] == fixture_windows[0].replay_run_id
        assert row["symbol"] == "BTC-USDT"
        assert row["is_complete"] is True


def test_late_and_unknown_values_propagate_to_venue_and_all(fixture_windows):
    first = fixture_windows[0]
    binance, okx = first.trades
    revised = replace(
        first,
        trades=(
            binance | {"taker_side": "unknown"},
            okx | {"taker_side": "unknown", "quote_qty_usd": None},
        ),
        late_exchanges=("okx",),
    )
    features = features_for_window(revised, computed_at_ts_us=CLOCK_US)
    rows = {row["exchange"]: row for row in features}
    assert set(rows) == {"ALL", "binance", "okx"}
    for row in features:
        validate(row, "symbol_features_1m.v1")
        assert row["taker_buy_ratio"] is None
        assert row["is_complete"] == (row["exchange"] == "binance")
    assert rows["ALL"]["volume_usd"] is None
    assert rows["okx"]["volume_usd"] is None
    assert rows["binance"]["volume_usd"] == 100


def test_cross_venue_ties_do_not_depend_on_arrival_order(fixture_windows):
    first = fixture_windows[0]
    binance, okx = first.trades
    assert binance["trade_id"] == okx["trade_id"]
    okx = okx | {
        "event_ts_us": binance["event_ts_us"],
        "price": 110,
        "quote_qty": 110,
        "quote_qty_usd": 110,
    }
    first_order = features_for_window(
        replace(first, trades=(binance, okx)), computed_at_ts_us=CLOCK_US
    )
    reversed_order = features_for_window(
        replace(first, trades=(okx, binance)), computed_at_ts_us=CLOCK_US
    )
    assert first_order == reversed_order
    for row in first_order:
        validate(row, "symbol_features_1m.v1")
    all_venues = next(row for row in first_order if row["exchange"] == "ALL")
    assert (all_venues["open"], all_venues["close"]) == (100, 110)


def test_empty_window_emits_no_feature(fixture_windows):
    empty = replace(fixture_windows[0], trades=())
    assert features_for_window(empty, computed_at_ts_us=CLOCK_US) == []
