"""SystemsLab-Sapienza pump-and-dump dataset (`pump_telegram.csv`) → pump events (BAT-01/BAT-06).

Columns: symbol, group, date (YYYY-MM-DD), hour (HH:MM, UTC), exchange. The dataset README states that
every pump is on the SYM/BTC pair. The same pump is often posted by several Telegram groups, sometimes
with announce times a few minutes apart, so rows of the same coin and exchange within
`merge_window_minutes` are merged into one event (earliest time, all groups kept).
"""

import csv
import io
from dataclasses import dataclass

from cryptoguard_common.timeutil import US_PER_SECOND, iso_to_us

PUMP_CSV_URL = "https://raw.githubusercontent.com/SystemsLab-Sapienza/pump-and-dump-dataset/master/pump_telegram.csv"
QUOTE = "BTC"
COLUMNS = ("symbol", "group", "date", "hour", "exchange")


@dataclass(frozen=True)
class PumpEvent:
    coin: str
    exchange: str
    announce_ts_us: int
    groups: tuple[str, ...]
    rows: int  # dataset rows merged into this event

    @property
    def symbol(self) -> str:
        return f"{self.coin}-{QUOTE}"

    @property
    def pump_id(self) -> str:
        """Same format as Label v1 `entity_id` for pump_event: '<SYMBOL>:<exchange>:<announce_ts_us>'."""
        return f"{self.symbol}:{self.exchange}:{self.announce_ts_us}"

    def native_symbol(self) -> str:
        if self.exchange != "binance":
            raise ValueError(f"native symbol only known for binance, not {self.exchange}")
        return f"{self.coin}{QUOTE}"


@dataclass(frozen=True)
class PumpParse:
    events: tuple[PumpEvent, ...]
    rows_in: int
    rejected: tuple[tuple[int, str], ...]  # (csv line number, reason)


def _announce_ts_us(day: str, hour: str) -> int:
    hh, mm = hour.strip().split(":")
    return iso_to_us(f"{day.strip()}T{int(hh):02d}:{int(mm):02d}:00Z")


def parse_pump_csv(text: str, *, merge_window_minutes: int = 60) -> PumpParse:
    reader = csv.DictReader(io.StringIO(text))
    missing = [c for c in COLUMNS if c not in (reader.fieldnames or [])]
    if missing:
        raise ValueError(f"pump csv missing columns {missing}")

    rows, rejected, rows_in = [], [], 0
    for line_no, row in enumerate(reader, start=2):
        rows_in += 1
        coin = (row["symbol"] or "").strip().upper()
        exchange = (row["exchange"] or "").strip().lower()
        if not coin.isalnum() or not exchange:
            rejected.append((line_no, "bad symbol or exchange"))
            continue
        try:
            ts = _announce_ts_us(row["date"], row["hour"])
        except ValueError:
            rejected.append((line_no, "bad date or hour"))
            continue
        rows.append((coin, exchange, ts, (row["group"] or "").strip()))

    window = merge_window_minutes * 60 * US_PER_SECOND
    events: list[PumpEvent] = []
    for coin, exchange, ts, group in sorted(rows):
        last = events[-1] if events else None
        if (
            last
            and (last.coin, last.exchange) == (coin, exchange)
            and ts - last.announce_ts_us <= window
        ):
            groups = last.groups if not group or group in last.groups else (*last.groups, group)
            events[-1] = PumpEvent(coin, exchange, last.announce_ts_us, groups, last.rows + 1)
        else:
            events.append(PumpEvent(coin, exchange, ts, (group,) if group else (), 1))
    return PumpParse(tuple(events), rows_in, tuple(rejected))
