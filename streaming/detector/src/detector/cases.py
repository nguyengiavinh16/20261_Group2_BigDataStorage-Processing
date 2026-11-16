"""Case grouping and Alert v1 construction (DET-06).

CaseTracker models the per-(symbol, exchange) keyed state the Flink job keeps: alerts whose windows start within
`case_merge_minutes` of the previous alert's window end belong to the same case. In Flink this is a
KeyedProcessFunction with ValueState(case_id, last_window_end_ts_us) and a TTL.
"""

import uuid
from dataclasses import dataclass

from cryptoguard_common.features import severity
from cryptoguard_common.timeutil import now_us
from detector.rules import RuleHit


@dataclass
class CaseState:
    case_id: str
    last_window_end_ts_us: int
    has_pump: bool


class CaseTracker:
    def __init__(self, case_merge_minutes: int):
        self.gap_us = case_merge_minutes * 60 * 1_000_000
        self.state: dict[tuple[str, str], CaseState] = {}

    def open_pump_case(self, symbol: str, exchange: str, window_start_ts_us: int) -> bool:
        st = self.state.get((symbol, exchange))
        return bool(
            st and st.has_pump and window_start_ts_us - st.last_window_end_ts_us <= self.gap_us
        )

    def assign(
        self,
        symbol: str,
        exchange: str,
        window_start_ts_us: int,
        window_end_ts_us: int,
        alert_type: str,
    ) -> str:
        key = (symbol, exchange)
        st = self.state.get(key)
        if st is None or window_start_ts_us - st.last_window_end_ts_us > self.gap_us:
            st = CaseState(f"{symbol}:{exchange}:{window_start_ts_us}", window_end_ts_us, False)
            self.state[key] = st
        st.last_window_end_ts_us = max(st.last_window_end_ts_us, window_end_ts_us)
        st.has_pump = st.has_pump or alert_type == "PUMP_SUSPECTED"
        return st.case_id


def build_alert(
    hit: RuleHit,
    features: dict,
    case_id: str,
    model_version: str | None = None,
    detected_at_ts_us: int | None = None,
) -> dict:
    """RuleHit + features → Alert v1 dict (validated by the caller's tests)."""
    return {
        "schema_version": "1.0",
        "alert_id": str(uuid.uuid4()),
        "case_id": case_id,
        "alert_type": hit.alert_type,
        "symbol": features["symbol"],
        "exchange": features["exchange"],
        "window_start_ts_us": features["window_start_ts_us"],
        "window_end_ts_us": features["window_end_ts_us"],
        "detected_at_ts_us": detected_at_ts_us if detected_at_ts_us is not None else now_us(),
        "risk_score": hit.risk_score,
        "severity": severity(hit.risk_score),
        "reasons": hit.reasons,
        "evidence": {
            k: features.get(k)
            for k in (
                "trade_count",
                "volume_usd",
                "baseline_volume_usd",
                "volume_z",
                "open",
                "close",
                "price_change_pct",
                "taker_buy_ratio",
                "is_complete",
            )
        },
        "rule_version": hit.rule_version,
        "model_version": model_version,
        "replay_run_id": features.get("replay_run_id"),
    }
