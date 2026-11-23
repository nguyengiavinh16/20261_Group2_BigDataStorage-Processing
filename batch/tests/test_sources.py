import hashlib
import io
from datetime import date

import pytest

from cg_batch.sources import aws_eth, binance_files, darklist, ofac, pump_dataset
from cg_batch.sources.evm import normalize_address
from cg_batch.tools.pump_candidates import candidate_row, days_to_check
from cryptoguard_common import contracts

ADDR = "0x252a8bd2319d8a555b872990601221b3a2053bce"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0x252A8BD2319d8a555b872990601221b3a2053bCE", ADDR),
        ("0x000000000000000000000000252a8bd2319d8a555b872990601221b3a2053bce", ADDR),  # AWS padded
        ("0x0000000000000000000000ff252a8bd2319d8a555b872990601221b3a2053bce", None),
        ("TXyz1234", None),
        (None, None),
    ],
)
def test_normalize_address(value, expected):
    assert normalize_address(value) == expected


def test_binance_files():
    url = binance_files.daily_trades_url("PEPEUSDT", date(2026, 11, 20))
    assert url.endswith("/PEPEUSDT/PEPEUSDT-trades-2026-11-20.zip")
    assert binance_files.checksum_url(url) == url + ".CHECKSUM"
    digest = "82d87397a4b44b2104a4feeaf4ad1d5c00e9295bd1331f4fd853913f6f99c877"
    assert binance_files.parse_checksum(f"{digest}  PEPEUSDT-trades-2026-11-20.zip\n") == digest
    with pytest.raises(ValueError):
        binance_files.parse_checksum("not-a-checksum file.zip")
    assert binance_files.time_unit("1795132807605239") == "us"  # 2026 file (measured in BAT-01)
    assert binance_files.time_unit(1530000000123) == "ms"  # pre-2025 files


SDN_XML = b"""<?xml version="1.0" standalone="yes"?>
<sdnList xmlns="https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML">
  <publshInformation><Publish_Date>11/20/2026</Publish_Date><Record_Count>2</Record_Count></publshInformation>
  <sdnEntry>
    <uid>1</uid><firstName>Test</firstName><lastName>PERSON</lastName><sdnType>Individual</sdnType>
    <programList><program>CYBER2</program></programList>
    <idList>
      <id><uid>10</uid><idType>Email Address</idType><idNumber>x@example.com</idNumber></id>
      <id><uid>11</uid><idType>Digital Currency Address - XBT</idType><idNumber>12aNKp2iDKuh</idNumber></id>
      <id><uid>12</uid><idType>Digital Currency Address - ETH</idType><idNumber>0x252A8bd2319d8a555b872990601221b3a2053bce</idNumber></id>
      <id><uid>13</uid><idType>Digital Currency Address - USDT</idType><idNumber>TXyz1234</idNumber></id>
    </idList>
  </sdnEntry>
  <sdnEntry>
    <uid>2</uid><lastName>TEST EXCHANGE LTD</lastName><sdnType>Entity</sdnType>
    <programList><program>CYBER2</program><program>RUSSIA-EO14024</program></programList>
    <idList>
      <id><uid>20</uid><idType>Digital Currency Address - USDT</idType><idNumber>0x252a8bd2319d8a555b872990601221b3a2053bce</idNumber></id>
    </idList>
  </sdnEntry>
</sdnList>"""


def test_parse_sdn_xml():
    snap = ofac.parse_sdn_xml(io.BytesIO(SDN_XML))
    assert snap.publish_date == "2026-11-20"
    assert snap.record_count == 2 and snap.entry_count == 2
    assert [a.currency for a in snap.addresses] == ["XBT", "ETH", "USDT", "USDT"]
    first = snap.addresses[1]
    assert first.name == "Test PERSON" and first.programs == ("CYBER2",)

    evm = ofac.evm_addresses(snap)
    assert list(evm) == [ADDR]  # TRON USDT dropped, same address in two entries kept once
    assert {a.sdn_uid for a in evm[ADDR]} == {1, 2}
    assert evm[ADDR][1].programs == ("CYBER2", "RUSSIA-EO14024")


def test_parse_darklist():
    data = """[
      {"address": "0xC915eC7f4CFD1C0A8Aba090F03BfaAb588aEF9B4", "comment": "later", "date": "2020-11-14"},
      {"address": "0xc915ec7f4cfd1c0a8aba090f03bfaab588aef9b4", "comment": "first", "date": "7/22/17"},
      {"address": "0x09750ad360fdb7a2ee23669c4503c974d86d8694", "comment": "no date", "date": ""},
      {"address": "not-an-address", "comment": "bad", "date": "2020-01-01"}
    ]"""
    parsed = darklist.parse_darklist(data)
    assert parsed.items_in == 4 and parsed.duplicates == 1 and len(parsed.rejected) == 1
    by_addr = {e.address: e for e in parsed.entries}
    dup = by_addr["0xc915ec7f4cfd1c0a8aba090f03bfaab588aef9b4"]
    assert (dup.reported_date, dup.comment) == ("2017-07-22", "first")
    assert by_addr["0x09750ad360fdb7a2ee23669c4503c974d86d8694"].reported_date is None


PUMP_CSV = """symbol,group,date,hour,exchange
BRD,ATW,2018-12-22,17:00,binance
brd,XYZ,2018-12-22,17:02,binance
BRD,ATW,2018-12-29,17:00,binance
DALC,ATW,2018-01-02,16:00,yobit
EVX,ATW,2019-03-10,25:00,binance
"""


def test_parse_pump_csv_merges_reposts():
    parsed = pump_dataset.parse_pump_csv(PUMP_CSV)
    assert parsed.rows_in == 5
    assert parsed.rejected == ((6, "bad date or hour"),)
    binance = [e for e in parsed.events if e.exchange == "binance"]
    assert len(binance) == 2  # 17:00 and 17:02 same day = one pump, a week later = another
    first = binance[0]
    assert first.groups == ("ATW", "XYZ") and first.rows == 2
    assert first.announce_ts_us == 1545498000000000  # 2018-12-22T17:00:00Z
    assert first.pump_id == "BRD-BTC:binance:1545498000000000"
    assert first.native_symbol() == "BRDBTC"
    yobit = next(e for e in parsed.events if e.exchange == "yobit")
    with pytest.raises(ValueError):
        yobit.native_symbol()


def test_pump_id_is_a_valid_label_entity_id():
    event = pump_dataset.parse_pump_csv(PUMP_CSV).events[0]
    key = f"pump_dataset|pump_event|{event.pump_id}|pump|{event.announce_ts_us}"
    contracts.validate(
        {
            "schema_version": "1.0",
            "label_id": hashlib.sha1(key.encode()).hexdigest(),
            "entity_type": "pump_event",
            "entity_id": event.pump_id,
            "label": "pump",
            "source": "pump_dataset",
            "confidence": 0.9,
            "valid_from_ts_us": event.announce_ts_us,
            "valid_to_ts_us": None,
        },
        "label.v1",
    )


def test_candidate_row():
    event = pump_dataset.PumpEvent("BRD", "binance", 1545521400000000, ("ATW",), 1)  # 23:30 UTC
    days = days_to_check(event, baseline_days=3)
    assert days == [date(2018, 12, 22), date(2018, 12, 21), date(2018, 12, 20), date(2018, 12, 19)]
    sizes = {days[0]: 1234, days[1]: 99, days[2]: None, days[3]: 50}
    row = candidate_row(event, sizes, baseline_days=3, checked_on="2026-11-23")
    assert row["has_trade_data"] == "true" and row["pump_day_file_bytes"] == 1234
    assert row["prior_days_available"] == 2 and row["baseline_files_bytes"] == 149
    assert row["announce_utc"] == "2018-12-22T23:30:00.000000Z"
    assert row["notes"] == "announced near midnight: replay next day too"

    other = pump_dataset.PumpEvent("BRD", "binance", 1545523200000000, ("ATW",), 3)  # 00:00 UTC
    assert days_to_check(other, baseline_days=0) == [date(2018, 12, 23)]
    row = candidate_row(other, {}, baseline_days=3, checked_on="2026-11-23")
    assert row["has_trade_data"] == "false" and row["pump_day_file_bytes"] == ""
    assert row["notes"] == "merged 3 dataset rows"


def test_aws_partition_prefix():
    assert (
        aws_eth.partition_prefix("token_transfers", date(2026, 11, 20))
        == "v1.0/eth/token_transfers/date=2026-11-20/"
    )
    with pytest.raises(ValueError):
        aws_eth.partition_prefix("nfts", "2026-11-20")
