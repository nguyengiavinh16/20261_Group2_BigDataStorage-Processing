"""data.binance.vision daily trades → Bronze (BAT-03). Planning is pure; I/O takes injectable clients.

Bronze layout (batch/airflow/dags/README.md): bronze/binance_vision/spot/trades/<SYMBOL>/<YYYY-MM-DD>.zip,
with object metadata `sha256` (from .CHECKSUM, verified on the downloaded bytes) and `source-url`.
- incremental: the same sha256 already in Bronze → skipped, nothing downloaded;
- immutable: a different sha256 already in Bronze → "conflict", never overwritten (team rule, R4 §7);
- bounded: local `data_budget` (max_symbols, max_days_history per symbol) unless a run explicitly sets
  allow_over_budget (e.g. pump day + 7 baseline days = 8 days, docs/source-notes.md §6 risk 2).
"""

import csv
import hashlib
import tempfile
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

from cg_batch.sources.binance_files import checksum_url, daily_trades_url, parse_checksum
from cg_batch.sources.pump_dataset import replay_days
from cryptoguard_common.paths import contracts_dir
from cryptoguard_common.reference import symbol_map

USER_AGENT = "cryptoguard-batch"
CHUNK = 1 << 20


class BudgetExceededError(ValueError):
    pass


class ChecksumMismatchError(RuntimeError):
    pass


@dataclass(frozen=True)
class BronzeItem:
    native_symbol: str
    day: str  # YYYY-MM-DD
    url: str
    key: str

    def as_dict(self) -> dict:
        return asdict(self)


def bronze_key(cfg: dict, native_symbol: str, day: str) -> str:
    return f"{cfg['storage']['bronze_prefix']}/binance_vision/spot/trades/{native_symbol}/{day}.zip"


def to_native(canonical: str) -> str:
    """BTC-USDT → BTCUSDT via symbol_map.csv (binance). Unknown symbols are an error, not a guess."""
    for (exchange, native), info in symbol_map().items():
        if exchange == "binance" and info.symbol == canonical:
            return native
    raise ValueError(f"{canonical} has no binance row in contracts/reference/symbol_map.csv")


def load_pump_candidates(path: Path | None = None) -> dict[str, dict]:
    path = path or contracts_dir() / "reference" / "pump_events_candidates.csv"
    with open(path, newline="", encoding="utf-8") as f:
        return {row["pump_id"]: row for row in csv.DictReader(f)}


def budget_violations(pairs: set[tuple[str, str]], budget: dict) -> list[str]:
    symbols = {s for s, _ in pairs}
    out = []
    if len(symbols) > budget["max_symbols"]:
        out.append(f"{len(symbols)} symbols > data_budget.max_symbols={budget['max_symbols']}")
    for s in sorted(symbols):
        days = sum(1 for sym, _ in pairs if sym == s)
        if days > budget["max_days_history"]:
            out.append(
                f"{s}: {days} days > data_budget.max_days_history={budget['max_days_history']}"
            )
    return out


def plan(
    cfg: dict,
    *,
    ds: str,
    symbols: list[str] | None = None,
    dates: list[str] | None = None,
    pump_ids: list[str] | None = None,
    pump_candidates: dict[str, dict] | None = None,
    allow_over_budget: bool = False,
) -> list[BronzeItem]:
    """Items to transfer. Default: the configured watchlist for the run date `ds`.

    symbols: canonical (BTC-USDT); dates: YYYY-MM-DD; pump_ids: rows of pump_events_candidates.csv →
    pump day + detector.baseline_days before it (+ next day when announced late).
    """
    pairs: set[tuple[str, str]] = set()
    if pump_ids:
        rows = pump_candidates if pump_candidates is not None else load_pump_candidates()
        baseline = cfg["detector"]["baseline_days"]
        for pid in pump_ids:
            if pid not in rows:
                raise ValueError(f"unknown pump_id {pid!r} (see pump_events_candidates.csv)")
            row = rows[pid]
            for d in replay_days(int(row["announce_ts_us"]), baseline, with_late_next_day=True):
                pairs.add((row["native_symbol"], d.isoformat()))
    if symbols or not pump_ids:
        natives = [to_native(s) for s in (symbols or cfg["symbols"]["watchlist"])]
        for day in dates or [ds]:
            date.fromisoformat(day)  # validate
            pairs.update((n, day) for n in natives)

    problems = budget_violations(pairs, cfg["data_budget"])
    if problems and not allow_over_budget:
        raise BudgetExceededError("; ".join(problems) + " (set allow_over_budget to run anyway)")
    return [
        BronzeItem(sym, day, daily_trades_url(sym, day), bronze_key(cfg, sym, day))
        for sym, day in sorted(pairs)
    ]


def _open(url: str, opener, timeout: float = 60.0):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    return opener(req, timeout=timeout)


def fetch_checksum(url: str, opener=urllib.request.urlopen) -> str | None:
    """sha256 from `<file>.CHECKSUM`, None if binance.vision has no file for that day (404)."""
    try:
        with _open(checksum_url(url), opener) as resp:
            return parse_checksum(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def bronze_sha256(s3, bucket: str, key: str) -> str | None:
    """sha256 metadata of an existing Bronze object, None if the object does not exist."""
    from botocore.exceptions import ClientError

    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return head.get("Metadata", {}).get("sha256", "")


def transfer(
    item: BronzeItem, s3, bucket: str, *, opener=urllib.request.urlopen, tmp_dir: str | None = None
) -> dict:
    """Download one day file into Bronze if needed. Returns {status, ..., key} for summarize()."""
    base = {"key": item.key, "native_symbol": item.native_symbol, "day": item.day}
    expected = fetch_checksum(item.url, opener)
    if expected is None:
        return base | {"status": "missing", "bytes": 0}
    existing = bronze_sha256(s3, bucket, item.key)
    if existing == expected:
        return base | {"status": "skipped", "bytes": 0, "sha256": expected}
    if existing is not None:
        return base | {
            "status": "conflict",
            "bytes": 0,
            "sha256": expected,
            "bronze_sha256": existing,
        }

    digest, size = hashlib.sha256(), 0
    with (
        tempfile.NamedTemporaryFile(dir=tmp_dir, suffix=".zip", delete=False) as tmp,
        _open(item.url, opener) as resp,
    ):
        while chunk := resp.read(CHUNK):
            digest.update(chunk)
            tmp.write(chunk)
            size += len(chunk)
    try:
        if digest.hexdigest() != expected:
            raise ChecksumMismatchError(f"{item.url}: sha256 {digest.hexdigest()} != {expected}")
        s3.upload_file(
            tmp.name,
            bucket,
            item.key,
            ExtraArgs={
                "Metadata": {"sha256": expected, "source-url": item.url},
                "ContentType": "application/zip",
            },
        )
    finally:
        Path(tmp.name).unlink(missing_ok=True)
    return base | {"status": "uploaded", "bytes": size, "sha256": expected}


def summarize(results: list[dict]) -> dict:
    counts = {s: 0 for s in ("uploaded", "skipped", "missing", "conflict")}
    for r in results:
        counts[r["status"]] += 1
    return counts | {
        "items": len(results),
        "bytes_uploaded": sum(r.get("bytes", 0) for r in results),
        "missing_keys": [r["key"] for r in results if r["status"] == "missing"],
        "conflict_keys": [r["key"] for r in results if r["status"] == "conflict"],
    }
