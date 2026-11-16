"""Replay data.binance.vision trades into replay.canonical.trades.v1 (ING-07).

    python -m replay.binance_vision --symbol BTCUSDT --date 2026-11-01 --speed 50 --run-id r20261116a

Design (keep these properties, they are what evaluation relies on):
- Input: Bronze copy of the daily zip (preferred) or a local file under data/ (never hard-code paths).
- Output: CanonicalTrade v1 on replay.canonical.trades.v1, key=symbol, replay_run_id set on every record.
  Live topics are never touched, so historical event-time never mixes with live watermarks.
- Ordering: strictly ascending event_ts_us per symbol (merge several symbols by event time).
- Pacing: sleep (Δevent_time / speed); speed=0 → as fast as possible (load test).
- Marks: prints first/last event_ts_us and count; writes a small manifest JSON (run_id, symbols, dates, speed).
"""

import argparse
import csv
import io
import zipfile
from collections.abc import Iterator

from cryptoguard_common.binance_vision import trade_row_to_canonical
from cryptoguard_common.timeutil import now_us


def iter_zip_rows(zip_bytes: bytes) -> Iterator[list[str]]:
    """Yield CSV rows from a binance.vision daily zip (one CSV inside, no header)."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as fh:
            for row in csv.reader(io.TextIOWrapper(fh, encoding="utf-8")):
                if row and row[0].strip().isdigit():  # skip a header if a future file adds one
                    yield row


def to_canonical(rows: Iterator[list[str]], native_symbol: str, run_id: str) -> Iterator[dict]:
    for row in rows:
        yield trade_row_to_canonical(
            row, native_symbol, ingest_ts_us=now_us(), replay_run_id=run_id
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", action="append", required=True)
    parser.add_argument("--date", action="append", required=True)
    parser.add_argument("--speed", type=float, default=None, help="default: config replay.speed")
    parser.add_argument("--run-id", required=True)
    parser.parse_args()
    raise NotImplementedError(
        "ING-07: read from Bronze (S3 API), merge by event time, pace, produce"
    )


if __name__ == "__main__":
    main()
