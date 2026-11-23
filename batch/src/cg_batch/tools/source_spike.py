"""BAT-01 spike: measure each downloaded source file and print the real schema / counts as JSON.

    python -m cg_batch.tools.source_spike --dir data/raw/bat01 --eth-date 2026-11-20 --out data/raw/bat01/summary.json

Reads local files in --dir (binance.vision zips, SDN.XML, addresses-darklist.json, pump_telegram.csv)
and only the Parquet footer of AWS token_transfers (range read, no full download).
The numbers go into docs/source-notes.md.
"""

import argparse
import csv
import hashlib
import io
import json
import zipfile
from collections import Counter
from pathlib import Path

from cg_batch.sources import aws_eth, darklist, ofac, pump_dataset
from cg_batch.sources.binance_files import parse_checksum, time_unit
from cg_batch.sources.evm import normalize_address
from cryptoguard_common.binance_vision import COLUMNS
from cryptoguard_common.timeutil import epoch_to_us, us_to_iso


def binance_zip_summary(path: Path) -> dict:
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    checksum_file = path.with_name(path.name + ".CHECKSUM")
    expected = parse_checksum(checksum_file.read_text()) if checksum_file.exists() else None
    units: Counter[str] = Counter()
    rows, first, last, ncols = 0, None, None, set()
    with zipfile.ZipFile(path) as zf, zf.open(zf.namelist()[0]) as raw:
        for row in csv.reader(io.TextIOWrapper(raw, encoding="utf-8")):
            rows += 1
            ncols.add(len(row))
            units[time_unit(row[4])] += 1
            ts = epoch_to_us(row[4])
            first = ts if first is None else min(first, ts)
            last = ts if last is None else max(last, ts)
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "sha256_ok": None if expected is None else sha == expected,
        "rows": rows,
        "columns_per_row": sorted(ncols),
        "expected_columns": list(COLUMNS),
        "time_units": dict(units),
        "first_trade_utc": us_to_iso(first) if first is not None else None,
        "last_trade_utc": us_to_iso(last) if last is not None else None,
    }


def ofac_summary(path: Path) -> dict:
    snap = ofac.parse_sdn_xml(str(path))
    evm = ofac.evm_addresses(snap)
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "publish_date": snap.publish_date,
        "record_count": snap.record_count,
        "entries": snap.entry_count,
        "digital_currency_ids": len(snap.addresses),
        "ids_by_currency": dict(Counter(a.currency for a in snap.addresses).most_common()),
        "evm_ids_by_currency": dict(
            Counter(a.currency for a in snap.addresses if a.evm_address).most_common()
        ),
        "evm_unique_addresses": len(evm),
        "evm_addresses_in_several_entries": sum(1 for v in evm.values() if len(v) > 1),
        "entries_with_evm_address": len({a.sdn_uid for v in evm.values() for a in v}),
    }


def darklist_summary(path: Path, ofac_evm: set[str]) -> dict:
    parsed = darklist.parse_darklist(path.read_text(encoding="utf-8"))
    dates = [e.reported_date for e in parsed.entries if e.reported_date]
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "items_in": parsed.items_in,
        "unique_addresses": len(parsed.entries),
        "duplicates": parsed.duplicates,
        "rejected": len(parsed.rejected),
        "without_date": sum(1 for e in parsed.entries if not e.reported_date),
        "reported_date_range": [min(dates), max(dates)] if dates else None,
        "overlap_with_ofac": len({e.address for e in parsed.entries} & ofac_evm),
    }


def pump_summary(path: Path) -> dict:
    parsed = pump_dataset.parse_pump_csv(path.read_text(encoding="utf-8"))
    binance = [e for e in parsed.events if e.exchange == "binance"]
    years = Counter(us_to_iso(e.announce_ts_us)[:4] for e in binance)
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "rows_in": parsed.rows_in,
        "rows_rejected": len(parsed.rejected),
        "events_after_merge": len(parsed.events),
        "events_by_exchange": dict(Counter(e.exchange for e in parsed.events).most_common()),
        "binance_events_by_year": dict(sorted(years.items())),
        "binance_rows_merged_away": sum(e.rows - 1 for e in binance),
    }


def eth_footer_summary(day: str) -> dict:
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    fs = pafs.S3FileSystem(anonymous=True, region=aws_eth.REGION)
    prefix = f"{aws_eth.BUCKET}/{aws_eth.partition_prefix('token_transfers', day)}"
    files = [f for f in fs.get_file_info(pafs.FileSelector(prefix)) if f.path.endswith(".parquet")]
    out = {"partition": prefix, "files": [], "total_bytes": sum(f.size for f in files)}
    for info in files:
        with fs.open_input_file(info.path) as fh:
            pf = pq.ParquetFile(fh)
            md = pf.metadata
            stats = {}
            for i in range(md.row_group(0).num_columns):
                col = md.row_group(0).column(i)
                s = col.statistics
                if col.path_in_schema in ("from_address", "to_address", "token_address") and s:
                    stats[col.path_in_schema] = {
                        "min_len": len(s.min),
                        "max_len": len(s.max),
                        "max_normalized": normalize_address(s.max),
                    }
            out["files"].append(
                {
                    "path": info.path,
                    "bytes": info.size,
                    "rows": md.num_rows,
                    "row_groups": md.num_row_groups,
                    "created_by": md.created_by,
                    "schema": {f.name: str(f.type) for f in pf.schema_arrow},
                    "physical_types": {
                        md.schema.column(i).name: md.schema.column(i).physical_type
                        for i in range(md.num_columns)
                    },
                    "address_stats_row_group_0": stats,
                }
            )
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dir", required=True)
    parser.add_argument("--eth-date", help="AWS token_transfers partition to inspect (footer only)")
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    d = Path(args.dir)

    summary: dict = {"binance_vision": [binance_zip_summary(p) for p in sorted(d.glob("*.zip"))]}
    if (d / "SDN.XML").exists():
        summary["ofac"] = ofac_summary(d / "SDN.XML")
        ofac_evm = set(ofac.evm_addresses(ofac.parse_sdn_xml(str(d / "SDN.XML"))))
    else:
        ofac_evm = set()
    if (d / "addresses-darklist.json").exists():
        summary["darklist"] = darklist_summary(d / "addresses-darklist.json", ofac_evm)
    if (d / "pump_telegram.csv").exists():
        summary["pump_dataset"] = pump_summary(d / "pump_telegram.csv")
    if args.eth_date:
        summary["aws_eth_token_transfers"] = eth_footer_summary(args.eth_date)

    text = json.dumps(summary, indent=2, ensure_ascii=False)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
