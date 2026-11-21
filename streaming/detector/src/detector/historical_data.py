"""Public historical data and causal demo baselines for DET-01."""

import csv
import hashlib
import io
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import fmean, pstdev
from urllib.request import urlopen
from uuid import uuid4
from zipfile import ZipFile

from cryptoguard_common.binance_vision import COLUMNS
from cryptoguard_common.features import window_features
from cryptoguard_common.timeutil import US_PER_SECOND, UTC, epoch_to_us, floor_to_window, iso_to_us

BASE, QUOTE = "GVT", "BTC"
SYMBOL = f"{BASE}-{QUOTE}"
NATIVE_SYMBOL = f"{BASE}{QUOTE}"
EVENT_DAY = date(2020, 9, 17)
CONTROL_DAY = EVENT_DAY - timedelta(days=1)
BASELINE_DAY = EVENT_DAY - timedelta(days=2)
LABELS_URL = (
    "https://raw.githubusercontent.com/SystemsLab-Sapienza/pump-and-dump-dataset/"
    "master/pump_telegram.csv"
)
MINUTE_US = 60 * US_PER_SECOND
ARCHIVE_LIMIT = 8 * 1024 * 1024


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temp.write_bytes(data)
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def _download(url: str, limit: int) -> bytes:
    with urlopen(url, timeout=25) as response:
        payload = response.read(limit + 1)
    if len(payload) > limit:
        raise ValueError(f"Download exceeds the demo limit for {url}")
    return payload


def _archive(cache: Path, directory: str, file: str) -> tuple[list[list[str]], dict]:
    url = f"https://data.binance.vision/data/spot/daily/{directory}/{file}"
    path, checksum_path = cache / file, cache / f"{file}.sha256"
    expected = (
        checksum_path.read_text().strip()
        if checksum_path.is_file()
        else _download(f"{url}.CHECKSUM", 1024).decode("ascii").split()[0]
    )
    payload = path.read_bytes() if path.is_file() else _download(url, ARCHIVE_LIMIT)
    if len(payload) > ARCHIVE_LIMIT or hashlib.sha256(payload).hexdigest() != expected:
        raise ValueError(f"Invalid cached or downloaded archive {path}")
    if not path.is_file():
        atomic_write(path, payload)
    if not checksum_path.is_file():
        atomic_write(checksum_path, expected.encode("ascii"))
    with ZipFile(io.BytesIO(payload)) as archive:
        (member,) = [entry for entry in archive.infolist() if entry.filename.endswith(".csv")]
        if member.file_size > 12 * ARCHIVE_LIMIT:
            raise ValueError(f"Uncompressed archive exceeds the demo limit for {url}")
        with archive.open(member) as csv_file:
            rows = [
                row for row in csv.reader(io.TextIOWrapper(csv_file)) if row and row[0].isdigit()
            ]
    return rows, {"url": url, "sha256": expected, "bytes": len(payload)}


def convert_trade(row: list[str], rates: dict[int, float]) -> dict:
    # Historical GVT-BTC is local to the demo and does not change the team symbol registry.
    fields = dict(zip(COLUMNS, row, strict=False))
    trade_id, timestamp = fields["trade_id"], epoch_to_us(fields["time"])
    price, qty, quote = (float(fields[key]) for key in ("price", "qty", "quote_qty"))
    maker = fields["is_buyer_maker"].lower()
    if maker not in {"true", "false", "0", "1"}:
        raise ValueError(f"Unknown Binance buyer-maker flag {maker!r}")
    # The BTCUSDT candle for the previous minute has already closed.
    rate = rates.get(floor_to_window(timestamp, 60) - MINUTE_US)
    return {
        "schema_version": "1.0",
        "event_id": f"binance:{NATIVE_SYMBOL}:{trade_id}",
        "exchange": "binance",
        "symbol": SYMBOL,
        "base": BASE,
        "quote": QUOTE,
        "native_symbol": NATIVE_SYMBOL,
        "trade_id": trade_id,
        "price": price,
        "qty": qty,
        "quote_qty": quote,
        "quote_qty_usd": quote * rate if rate is not None else None,
        "taker_side": "sell" if maker in {"true", "1"} else "buy",
        "event_ts_us": timestamp,
    }


def baseline_snapshots(trades: list[dict]) -> dict[str, dict]:
    windows = defaultdict(list)
    for trade in trades:
        windows[floor_to_window(trade["event_ts_us"], 60)].append(trade)
    by_hour = defaultdict(list)
    for start, rows in windows.items():
        feature = window_features(
            rows,
            symbol=SYMBOL,
            exchange="binance",
            window_seconds=60,
            window_start_ts_us=start,
            computed_at_ts_us=start + MINUTE_US,
        )
        if feature["volume_usd"] is not None:
            hour = datetime.fromtimestamp(start // US_PER_SECOND, tz=UTC).hour
            by_hour[hour].append(feature["volume_usd"])
    snapshots = {}
    for hour, values in by_hour.items():
        if len(values) >= 2 and fmean(values) > 0:
            snapshot = {
                "mean_volume_usd": fmean(values),
                "std_volume_usd": pstdev(values),
                "n_windows": len(values),
                "as_of_date": BASELINE_DAY.isoformat(),
            }
            for exchange in ("binance", "ALL"):
                snapshots[f"baseline:{SYMBOL}:{exchange}:{hour}"] = snapshot
    return snapshots


def load_historical_data(cache: Path, budget: dict) -> dict:
    days = (BASELINE_DAY, CONTROL_DAY, EVENT_DAY)
    if len(days) > budget["max_days_history"] or budget["max_symbols"] < 2:
        raise ValueError("The historical fixture exceeds the configured local data budget")
    label_path = cache / "pump_telegram.csv"
    if not label_path.is_file():
        atomic_write(label_path, _download(LABELS_URL, 256 * 1024))
    label_data = label_path.read_bytes()
    rows = csv.DictReader(io.StringIO(label_data.decode("utf-8-sig")))
    matches = [
        row for row in rows if row["symbol"] == BASE and row["exchange"].lower() == "binance"
    ]
    times = sorted({row["hour"] for row in matches if row["date"] == EVENT_DAY.isoformat()})
    if not times or any(row["date"] in {str(BASELINE_DAY), str(CONTROL_DAY)} for row in matches):
        raise ValueError(
            "Historical label or unlabeled calibration/control dates could not be verified"
        )
    sources = [{"url": LABELS_URL, "sha256": hashlib.sha256(label_data).hexdigest()}]
    rates = {}
    trade_rows = {}
    for day in days:
        candles, fx_source = _archive(cache, "klines/BTCUSDT/1m", f"BTCUSDT-1m-{day}.zip")
        rates.update({epoch_to_us(row[0]): float(row[4]) for row in candles})
        trade_rows[day], trade_source = _archive(
            cache, f"trades/{NATIVE_SYMBOL}", f"{NATIVE_SYMBOL}-trades-{day}.zip"
        )
        sources.extend((fx_source, trade_source))
    by_day = {
        day: sorted(
            (convert_trade(row, rates) for row in trade_rows[day]),
            key=lambda trade: (trade["event_ts_us"], int(trade["trade_id"])),
        )
        for day in days
    }
    if not all(by_day.values()):
        raise ValueError("A required day has no archive trades")
    return {
        "trades": [*by_day[CONTROL_DAY], *by_day[EVENT_DAY]],
        "trade_counts": {day.isoformat(): len(rows) for day, rows in by_day.items()},
        "snapshots": baseline_snapshots(by_day[BASELINE_DAY]),
        "label_ts_us": iso_to_us(f"{EVENT_DAY}T{times[0]}:00Z"),
        "label_times": times,
        "sources": sources,
    }
