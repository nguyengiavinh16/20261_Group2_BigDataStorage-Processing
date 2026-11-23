"""MyEtherWallet ethereum-lists `addresses-darklist.json` → reported scam/phishing addresses (BAT-01/BAT-06).

File: JSON array of {"address", "comment", "date"}. Seen in BAT-01: mixed-case (checksummed) addresses,
the same address reported several times, `date` as YYYY-MM-DD, M/D/YY or empty.
`date` is when the address was reported to the list, not when it became malicious.
"""

import json
from dataclasses import dataclass
from datetime import datetime

from cg_batch.sources.evm import normalize_address

DARKLIST_URL = (
    "https://raw.githubusercontent.com/MyEtherWallet/ethereum-lists/master/"
    "src/addresses/addresses-darklist.json"
)


@dataclass(frozen=True)
class DarklistEntry:
    address: str  # lowercase
    comment: str
    reported_date: str | None  # ISO YYYY-MM-DD, earliest report


@dataclass(frozen=True)
class DarklistParse:
    entries: tuple[DarklistEntry, ...]
    items_in: int
    duplicates: int
    rejected: tuple[dict, ...]


def _iso_date(value: str | None) -> str | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_darklist(data: str | bytes | list) -> DarklistParse:
    """Dedup by lowercase address, keeping the earliest report date and its comment."""
    items = json.loads(data) if isinstance(data, str | bytes) else data
    best: dict[str, DarklistEntry] = {}
    rejected, valid = [], 0
    for item in items:
        addr = normalize_address(item.get("address")) if isinstance(item, dict) else None
        if addr is None:
            rejected.append(item if isinstance(item, dict) else {"raw": item})
            continue
        valid += 1
        entry = DarklistEntry(addr, str(item.get("comment") or ""), _iso_date(item.get("date")))
        prev = best.get(addr)
        if prev is None or (
            entry.reported_date is not None
            and (prev.reported_date is None or entry.reported_date < prev.reported_date)
        ):
            best[addr] = entry
    return DarklistParse(
        entries=tuple(best[a] for a in sorted(best)),
        items_in=len(items),
        duplicates=valid - len(best),
        rejected=tuple(rejected),
    )
