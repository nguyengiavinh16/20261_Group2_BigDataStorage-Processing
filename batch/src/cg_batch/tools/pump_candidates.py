"""Which pump events of the dataset can be replayed from data.binance.vision? (BAT-01)

For every Binance event: HEAD request for the trades file of the pump day and of the
`detector.baseline_days` days before it (baseline for volume_z). Nothing is downloaded.

    python -m cg_batch.tools.pump_candidates --pump-csv data/raw/bat01/pump_telegram.csv

Writes contracts/reference/pump_events_candidates.csv (input of gold.pump_cases in BAT-06).
"""

import argparse
import csv
import logging
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path

from cg_batch.sources.binance_files import daily_trades_url
from cg_batch.sources.pump_dataset import (
    LATE_ANNOUNCE_HOUR,
    PumpEvent,
    announce_day,
    parse_pump_csv,
    replay_days,
)
from cryptoguard_common.config import load_config
from cryptoguard_common.log import get_logger, log_kv
from cryptoguard_common.paths import contracts_dir
from cryptoguard_common.timeutil import us_to_iso

FIELDS = [
    "pump_id",
    "symbol",
    "native_symbol",
    "exchange",
    "announce_ts_us",
    "announce_utc",
    "groups",
    "dataset_rows",
    "has_trade_data",
    "pump_day_file_bytes",
    "prior_days_available",
    "baseline_days",
    "baseline_files_bytes",
    "source",
    "checked_on",
    "notes",
]
log = get_logger("cg_batch.tools.pump_candidates")


def event_day(event: PumpEvent) -> date:
    return announce_day(event.announce_ts_us)


def days_to_check(event: PumpEvent, baseline_days: int) -> list[date]:
    """Pump day first, then the baseline days before it (most recent first)."""
    return replay_days(event.announce_ts_us, baseline_days)


def candidate_row(
    event: PumpEvent, sizes: dict[date, int | None], baseline_days: int, checked_on: str
) -> dict:
    """sizes: day → Content-Length of that day's trades file, None if the file does not exist."""
    days = days_to_check(event, baseline_days)
    pump_size = sizes.get(days[0])
    prior_sizes = [sizes[d] for d in days[1:] if sizes.get(d) is not None]
    notes = []
    if event.rows > 1:
        notes.append(f"merged {event.rows} dataset rows")
    announce_utc = us_to_iso(event.announce_ts_us)
    if int(announce_utc[11:13]) >= LATE_ANNOUNCE_HOUR:
        notes.append("announced near midnight: replay next day too")
    return {
        "pump_id": event.pump_id,
        "symbol": event.symbol,
        "native_symbol": event.native_symbol(),
        "exchange": event.exchange,
        "announce_ts_us": event.announce_ts_us,
        "announce_utc": announce_utc,
        "groups": "|".join(event.groups),
        "dataset_rows": event.rows,
        "has_trade_data": "true" if pump_size is not None else "false",
        "pump_day_file_bytes": "" if pump_size is None else pump_size,
        "prior_days_available": len(prior_sizes),
        "baseline_days": baseline_days,
        "baseline_files_bytes": sum(prior_sizes),
        "source": "pump_dataset",
        "checked_on": checked_on,
        "notes": "; ".join(notes),
    }


def head_size(url: str, timeout: float = 20.0, retries: int = 2) -> int | None:
    """Content-Length of `url`, None on 404/403 (Binance's CDN answers 404 for missing days)."""
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "cryptoguard-bat01"})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return int(resp.headers.get("Content-Length") or 0)
        except urllib.error.HTTPError as exc:
            if exc.code in (403, 404):
                return None
            if attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == retries:
                raise
    return None


def main(argv: list[str] | None = None) -> None:
    cfg = load_config()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pump-csv", required=True, help="local copy of pump_telegram.csv")
    parser.add_argument(
        "--out", default=str(contracts_dir() / "reference" / "pump_events_candidates.csv")
    )
    parser.add_argument("--baseline-days", type=int, default=cfg["detector"]["baseline_days"])
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args(argv)

    parsed = parse_pump_csv(Path(args.pump_csv).read_text(encoding="utf-8"))
    events = [e for e in parsed.events if e.exchange == "binance"]
    checks = {(e.native_symbol(), d) for e in events for d in days_to_check(e, args.baseline_days)}
    log_kv(log, logging.INFO, "checking", events=len(events), head_requests=len(checks))

    def check(key: tuple[str, date]) -> tuple[tuple[str, date], int | None]:
        return key, head_size(daily_trades_url(*key))

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = dict(pool.map(check, sorted(checks)))

    checked_on = datetime.now(tz=UTC).date().isoformat()
    rows = []
    for e in events:
        sizes = {d: results[(e.native_symbol(), d)] for d in days_to_check(e, args.baseline_days)}
        rows.append(candidate_row(e, sizes, args.baseline_days, checked_on))

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: r["announce_ts_us"]))
    log_kv(
        log,
        logging.INFO,
        "written",
        out=args.out,
        rows_in=parsed.rows_in,
        rows_rejected=len(parsed.rejected),
        binance_events=len(rows),
        with_trade_data=sum(r["has_trade_data"] == "true" for r in rows),
        with_full_baseline=sum(
            r["has_trade_data"] == "true" and r["prior_days_available"] == args.baseline_days
            for r in rows
        ),
    )


if __name__ == "__main__":
    main()
