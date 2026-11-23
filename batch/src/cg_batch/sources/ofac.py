"""OFAC SDN list (SDN.XML) → sanctioned digital currency addresses (BAT-01 spike, labels job BAT-06).

Structure seen in the 2026-11-20 publication:

    <sdnList xmlns="https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML">
      <publshInformation><Publish_Date>11/20/2026</Publish_Date><Record_Count>19599</Record_Count>…
      <sdnEntry><uid/><firstName/>?<lastName/><sdnType/><programList><program/>…</programList>
        <idList><id><uid/><idType>Digital Currency Address - ETH</idType><idNumber>0x…</idNumber></id>…

The currency tag is not the chain: "USDT" holds both TRON (T…) and Ethereum (0x…) addresses, so EVM
addresses are selected by format (`evm.normalize_address`), not by tag.
SDN.XML has no per-entry listing date: valid_from for a label is the date of the first archived
snapshot containing the address, valid_to the first snapshot where it is gone (BAT-06).
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import BinaryIO

from cg_batch.sources.evm import normalize_address

SDN_XML_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"
ID_TYPE_PREFIX = "Digital Currency Address - "


@dataclass(frozen=True)
class OfacAddress:
    sdn_uid: int
    name: str
    sdn_type: str
    programs: tuple[str, ...]
    currency: str  # tag after ID_TYPE_PREFIX, e.g. ETH, XBT, USDT
    address: str  # as published

    @property
    def evm_address(self) -> str | None:
        return normalize_address(self.address)


@dataclass(frozen=True)
class SdnSnapshot:
    publish_date: str | None  # ISO YYYY-MM-DD
    record_count: int | None
    entry_count: int
    addresses: tuple[OfacAddress, ...]


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(elem: ET.Element, name: str) -> str:
    for child in elem:
        if _local(child.tag) == name:
            return (child.text or "").strip()
    return ""


def _children(elem: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in elem if _local(child.tag) == name]


def _entry_addresses(entry: ET.Element) -> list[OfacAddress]:
    uid = int(_child_text(entry, "uid") or 0)
    name = " ".join(
        p for p in (_child_text(entry, "firstName"), _child_text(entry, "lastName")) if p
    )
    sdn_type = _child_text(entry, "sdnType")
    programs = tuple(
        (p.text or "").strip() for pl in _children(entry, "programList") for p in pl if p.text
    )
    out = []
    for id_list in _children(entry, "idList"):
        for id_elem in _children(id_list, "id"):
            id_type = _child_text(id_elem, "idType")
            if not id_type.startswith(ID_TYPE_PREFIX):
                continue
            out.append(
                OfacAddress(
                    sdn_uid=uid,
                    name=name,
                    sdn_type=sdn_type,
                    programs=programs,
                    currency=id_type[len(ID_TYPE_PREFIX) :].strip(),
                    address=_child_text(id_elem, "idNumber"),
                )
            )
    return out


def _iso_date(us_date: str) -> str | None:
    """'11/20/2026' (MM/DD/YYYY) → '2026-11-20'."""
    try:
        return datetime.strptime(us_date.strip(), "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def parse_sdn_xml(source: str | BinaryIO) -> SdnSnapshot:
    """Stream-parse SDN.XML (path or binary file object); keeps only digital currency addresses."""
    publish_date, record_count, entries = None, None, 0
    addresses: list[OfacAddress] = []
    for _, elem in ET.iterparse(source, events=("end",)):
        tag = _local(elem.tag)
        if tag == "sdnEntry":
            entries += 1
            addresses.extend(_entry_addresses(elem))
            elem.clear()
        elif tag == "publshInformation":  # sic, OFAC's spelling
            publish_date = _iso_date(_child_text(elem, "Publish_Date"))
            count = _child_text(elem, "Record_Count")
            record_count = int(count) if count.isdigit() else None
    return SdnSnapshot(publish_date, record_count, entries, tuple(addresses))


def evm_addresses(snapshot: SdnSnapshot) -> dict[str, list[OfacAddress]]:
    """Lowercase EVM address → every SDN entry listing it (one address can appear in several)."""
    out: dict[str, list[OfacAddress]] = {}
    for rec in snapshot.addresses:
        addr = rec.evm_address
        if addr:
            out.setdefault(addr, []).append(rec)
    return out
