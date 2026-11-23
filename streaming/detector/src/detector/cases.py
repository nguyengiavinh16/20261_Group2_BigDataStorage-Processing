"""Case grouping, window transitions and Alert v1 construction (DET-06)."""

import uuid
from dataclasses import dataclass

from cryptoguard_common.features import severity
from cryptoguard_common.timeutil import US_PER_SECOND, now_us
from detector.baseline import ScoredFeature
from detector.rules import RuleHit, evaluate_scored_feature


@dataclass(frozen=True)
class CaseState:
    case_id: str
    last_window_end_ts_us: int
    has_pump: bool


def _active_case(state: CaseState | None, window_start_ts_us: int, gap_us: int) -> CaseState | None:
    if state and window_start_ts_us - state.last_window_end_ts_us <= gap_us:
        return state
    return None


def _assign_case(
    state: CaseState | None,
    symbol: str,
    exchange: str,
    window_start_ts_us: int,
    window_end_ts_us: int,
    alert_type: str,
) -> CaseState:
    has_pump = alert_type == "PUMP_SUSPECTED"
    if state is None:
        return CaseState(f"{symbol}:{exchange}:{window_start_ts_us}", window_end_ts_us, has_pump)
    return CaseState(
        state.case_id,
        max(state.last_window_end_ts_us, window_end_ts_us),
        state.has_pump or has_pump,
    )


class CaseTracker:
    def __init__(self, case_merge_minutes: int):
        self.gap_us = case_merge_minutes * 60 * US_PER_SECOND
        self.state: dict[tuple[str, str], CaseState] = {}

    def open_pump_case(self, symbol: str, exchange: str, window_start_ts_us: int) -> bool:
        state = _active_case(self.state.get((symbol, exchange)), window_start_ts_us, self.gap_us)
        return bool(state and state.has_pump)

    def assign(
        self,
        symbol: str,
        exchange: str,
        window_start_ts_us: int,
        window_end_ts_us: int,
        alert_type: str,
    ) -> str:
        key = (symbol, exchange)
        state = _active_case(self.state.get(key), window_start_ts_us, self.gap_us)
        state = _assign_case(
            state, symbol, exchange, window_start_ts_us, window_end_ts_us, alert_type
        )
        self.state[key] = state
        return state.case_id


@dataclass(frozen=True)
class CaseProgress:
    case: CaseState | None
    window_start_ts_us: int
    alerted: bool


def advance_case(
    previous: CaseProgress | None,
    scored: ScoredFeature,
    pump_rule: dict,
    dump_rule: dict,
    *,
    gap_us: int,
) -> tuple[CaseProgress, RuleHit | None]:
    """Advance one key, retaining the first alert for each observed window."""
    feature = scored.feature
    start_us = feature["window_start_ts_us"]
    if previous is not None:
        if start_us < previous.window_start_ts_us:
            return previous, None
        if start_us == previous.window_start_ts_us and previous.alerted:
            return previous, None

    case = _active_case(previous.case if previous else None, start_us, gap_us)
    hit = evaluate_scored_feature(
        scored, pump_rule, dump_rule, pump_case_open=bool(case and case.has_pump)
    )
    if hit is not None:
        case = _assign_case(
            case,
            feature["symbol"],
            feature["exchange"],
            start_us,
            feature["window_end_ts_us"],
            hit.alert_type,
        )
    return CaseProgress(case, start_us, hit is not None), hit


def build_alert(
    hit: RuleHit,
    features: dict,
    case_id: str,
    model_version: str | None = None,
    detected_at_ts_us: int | None = None,
) -> dict:
    """RuleHit + features → Alert v1 dict (validated by the caller's tests)."""
    evidence = {
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
    }
    if hit.baseline_source is not None:
        evidence["baseline_source"] = hit.baseline_source

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
        "evidence": evidence,
        "rule_version": hit.rule_version,
        "model_version": model_version,
        "replay_run_id": features.get("replay_run_id"),
    }
