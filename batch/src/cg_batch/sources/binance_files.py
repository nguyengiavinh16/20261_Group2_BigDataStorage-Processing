"""data.binance.vision daily spot trades files: URLs, .CHECKSUM parsing, timestamp unit (BAT-01/BAT-03).

Row parsing to CanonicalTrade lives in cryptoguard_common.binance_vision (shared with replay, R2).
Measured in BAT-01: 2026 files carry 16-digit `time` values (µs), see docs/source-notes.md.
"""

from datetime import date

BASE_URL = "https://data.binance.vision/data/spot/daily/trades"


def daily_trades_url(native_symbol: str, day: date | str) -> str:
    d = day.isoformat() if isinstance(day, date) else day
    return f"{BASE_URL}/{native_symbol}/{native_symbol}-trades-{d}.zip"


def checksum_url(file_url: str) -> str:
    return f"{file_url}.CHECKSUM"


def parse_checksum(text: str) -> str:
    """`<sha256 hex>  <file name>` → lowercase sha256 hex."""
    digest = text.strip().split()[0].lower()
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError(f"not a sha256 checksum line: {text[:80]!r}")
    return digest


def time_unit(value: int | float | str) -> str:
    """Unit of an epoch value by magnitude, same thresholds as timeutil.epoch_to_us."""
    v = float(value)
    if v < 1e11:
        return "s"
    if v < 1e14:
        return "ms"
    if v < 1e17:
        return "us"
    return "ns"
