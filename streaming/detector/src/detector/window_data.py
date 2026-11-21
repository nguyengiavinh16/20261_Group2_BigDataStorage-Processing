"""Trade window snapshots shared by the Flink window and feature stages."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TradeWindow:
    symbol: str
    replay_run_id: str | None
    window_start_ts_us: int
    window_end_ts_us: int
    trades: tuple[dict, ...]
    late_exchanges: tuple[str, ...]

    @property
    def is_complete(self) -> bool:
        return not self.late_exchanges
