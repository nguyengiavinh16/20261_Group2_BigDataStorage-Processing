"""All message timestamps are integer epoch microseconds UTC (ADR-0004). Convert at the edges only."""

import time
from datetime import datetime, timezone

UTC = timezone.utc

US_PER_SECOND = 1_000_000


def now_us() -> int:
    return time.time_ns() // 1_000


def epoch_to_us(value: int | float | str) -> int:
    """Convert an epoch number of unknown unit (s, ms, µs, ns) to µs by magnitude.

    Exchanges disagree: Binance WS uses ms, data.binance.vision spot files switched to µs in 2025,
    OKX sends ms as a string. Valid for dates between ~1973 and ~2286.
    """
    v = float(value)
    if v < 1e11:  # seconds
        return int(round(v * US_PER_SECOND))
    if v < 1e14:  # milliseconds
        return int(round(v * 1_000))
    if v < 1e17:  # microseconds
        return int(v)
    return int(v // 1_000)  # nanoseconds


def iso_to_us(value: str) -> int:
    """Parse ISO-8601 (e.g. '2026-11-16T07:00:00.121234Z'), keeping microseconds exactly.

    Fractions longer than 6 digits (nanoseconds) are truncated.
    """
    s = value.strip().replace("Z", "+00:00")
    if "." in s:
        head, _, rest = s.partition(".")
        digits = "".join(ch for ch in rest if ch.isdigit())
        tz = rest[len(digits) :]
        s = f"{head}.{digits[:6].ljust(6, '0')}{tz}"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta = dt - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * US_PER_SECOND + delta.microseconds


def us_to_iso(ts_us: int) -> str:
    seconds, micros = divmod(int(ts_us), US_PER_SECOND)
    dt = datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=micros)
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def floor_to_window(ts_us: int, window_seconds: int) -> int:
    """Start of the tumbling window containing ts_us."""
    size = window_seconds * US_PER_SECOND
    return (int(ts_us) // size) * size
