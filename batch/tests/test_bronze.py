import copy
import hashlib
import io
import urllib.error
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from cg_batch import bronze
from cryptoguard_common.config import load_config

BASE_CFG = load_config("local", env={})
PUMP_ROWS = {  # shape of contracts/reference/pump_events_candidates.csv
    "GTO-BTC:binance:1604419200000000": {
        "native_symbol": "GTOBTC",
        "announce_ts_us": "1604419200000000",  # 2020-11-03T16:00Z
    },
    "LATE-BTC:binance:1604446200000000": {
        "native_symbol": "GTOBTC",
        "announce_ts_us": "1604446200000000",  # 2020-11-03T23:30Z → next day too
    },
}


def cfg(**budget):
    c = copy.deepcopy(BASE_CFG)
    c["symbols"]["watchlist"] = ["BTC-USDT", "ETH-USDT"]
    c["detector"]["baseline_days"] = 7
    c["data_budget"] = {"max_symbols": 10, "max_days_history": 7} | budget
    return c


def test_plan_defaults_to_watchlist_for_run_date():
    items = bronze.plan(cfg(), ds="2026-11-22")
    assert [(i.native_symbol, i.day) for i in items] == [
        ("BTCUSDT", "2026-11-22"),
        ("ETHUSDT", "2026-11-22"),
    ]
    assert items[1].key == "bronze/binance_vision/spot/trades/ETHUSDT/2026-11-22.zip"
    assert items[1].url.endswith("/ETHUSDT/ETHUSDT-trades-2026-11-22.zip")


def test_plan_explicit_symbols_and_dates():
    items = bronze.plan(
        cfg(), ds="2026-11-22", symbols=["ETH-USDT"], dates=["2026-11-20", "2026-11-21"]
    )
    assert [i.day for i in items] == ["2026-11-20", "2026-11-21"]
    with pytest.raises(ValueError, match="symbol_map"):
        bronze.plan(cfg(), ds="2026-11-22", symbols=["NOPE-USDT"])
    with pytest.raises(ValueError):
        bronze.plan(cfg(), ds="2026-11-22", dates=["22/11/2026"])


def test_plan_pump_events_respects_budget_unless_allowed():
    pid = "GTO-BTC:binance:1604419200000000"
    with pytest.raises(bronze.BudgetExceededError, match="8 days > data_budget.max_days_history=7"):
        bronze.plan(cfg(), ds="2026-11-22", pump_ids=[pid], pump_candidates=PUMP_ROWS)
    items = bronze.plan(
        cfg(), ds="2026-11-22", pump_ids=[pid], pump_candidates=PUMP_ROWS, allow_over_budget=True
    )
    assert [i.day for i in items] == [f"2020-10-{d}" for d in range(27, 32)] + [
        "2020-11-01",
        "2020-11-02",
        "2020-11-03",
    ]
    assert {i.native_symbol for i in items} == {"GTOBTC"}  # watchlist not added in pump mode

    late = bronze.plan(
        cfg(max_days_history=9),
        ds="2026-11-22",
        pump_ids=["LATE-BTC:binance:1604446200000000"],
        pump_candidates=PUMP_ROWS,
    )
    assert late[-1].day == "2020-11-04"
    with pytest.raises(ValueError, match="unknown pump_id"):
        bronze.plan(cfg(), ds="2026-11-22", pump_ids=["X"], pump_candidates=PUMP_ROWS)


def test_budget_violations_symbols():
    pairs = {(f"S{i}USDT", "2026-11-22") for i in range(3)}
    assert bronze.budget_violations(pairs, {"max_symbols": 2, "max_days_history": 7}) == [
        "3 symbols > data_budget.max_symbols=2"
    ]


class FakeS3:
    def __init__(self):
        self.objects: dict[str, tuple[bytes, dict]] = {}

    def head_object(self, Bucket, Key):  # noqa: N803 (boto3 signature)
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"Metadata": self.objects[Key][1]}

    def upload_file(self, filename, bucket, key, ExtraArgs):  # noqa: N803
        self.objects[key] = (Path(filename).read_bytes(), ExtraArgs["Metadata"])


class FakeWeb:
    """urllib.request.urlopen stand-in: {url: bytes}; missing url → HTTP 404."""

    def __init__(self, files: dict[str, bytes]):
        self.files, self.calls = files, []

    def __call__(self, req, timeout=None):
        url = req.full_url
        self.calls.append(url)
        if url not in self.files:
            raise urllib.error.HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
        return io.BytesIO(self.files[url])


ZIP = b"PK\x03\x04 fake zip bytes" * 1000
SHA = hashlib.sha256(ZIP).hexdigest()
ITEM = bronze.plan(cfg(), ds="2026-11-22", symbols=["ETH-USDT"])[0]


def web(body=ZIP, sha=SHA):
    return FakeWeb({ITEM.url: body, ITEM.url + ".CHECKSUM": f"{sha}  x.zip\n".encode()})


def test_transfer_uploads_then_skips_on_rerun(tmp_path):
    s3 = FakeS3()
    first = bronze.transfer(ITEM, s3, "cg-lake", opener=web(), tmp_dir=str(tmp_path))
    assert first["status"] == "uploaded" and first["bytes"] == len(ZIP)
    body, meta = s3.objects[ITEM.key]
    assert body == ZIP and meta == {"sha256": SHA, "source-url": ITEM.url}
    assert list(tmp_path.iterdir()) == []  # temp file removed

    again = web()
    second = bronze.transfer(ITEM, s3, "cg-lake", opener=again, tmp_dir=str(tmp_path))
    assert second["status"] == "skipped"
    assert again.calls == [ITEM.url + ".CHECKSUM"]  # incremental: zip not downloaded again


def test_transfer_missing_conflict_and_mismatch(tmp_path):
    s3 = FakeS3()
    assert bronze.transfer(ITEM, s3, "b", opener=FakeWeb({}))["status"] == "missing"

    s3.objects[ITEM.key] = (b"old", {"sha256": "0" * 64})
    conflict = bronze.transfer(ITEM, s3, "b", opener=web())
    assert (
        conflict["status"] == "conflict" and s3.objects[ITEM.key][0] == b"old"
    )  # never overwritten

    fresh = FakeS3()
    with pytest.raises(bronze.ChecksumMismatchError):
        bronze.transfer(ITEM, fresh, "b", opener=web(sha="f" * 64), tmp_dir=str(tmp_path))
    assert fresh.objects == {} and list(tmp_path.iterdir()) == []


def test_summarize():
    s = bronze.summarize(
        [
            {"status": "uploaded", "bytes": 10, "key": "a"},
            {"status": "skipped", "bytes": 0, "key": "b"},
            {"status": "missing", "bytes": 0, "key": "c"},
        ]
    )
    assert (s["uploaded"], s["skipped"], s["missing"], s["conflict"]) == (1, 1, 1, 0)
    assert s["bytes_uploaded"] == 10 and s["missing_keys"] == ["c"] and s["items"] == 3
