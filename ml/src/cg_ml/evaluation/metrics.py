"""Detection metrics against ground-truth pump events (MLS-06). The definitions here ARE the evaluation protocol.

Matching rule: an alert matches an event if
  - same symbol, and same exchange (or alert exchange == 'ALL'), and
  - alert.window_start_ts_us ∈ [event.start_ts_us - before_us, event.start_ts_us + after_us].
Only alert types in `pump_types` count as detections (DUMP_PHASE alone does not).
Event-time is used (not wall clock) so replay speed does not change the results.
"""

from dataclasses import dataclass

PUMP_TYPES = frozenset({"PUMP_SUSPECTED"})


@dataclass(frozen=True)
class PumpEvent:
    event_id: str
    symbol: str
    exchange: str
    start_ts_us: int


@dataclass
class EvalResult:
    n_events: int
    n_alerts: int
    detected_events: int
    recall: float
    precision: float
    false_alerts_per_day: float
    median_delay_s: float | None
    delays_s: dict[str, float]


def _matches(alert: dict, ev: PumpEvent, before_us: int, after_us: int) -> bool:
    return (
        alert["symbol"] == ev.symbol
        and alert["exchange"] in (ev.exchange, "ALL")
        and ev.start_ts_us - before_us <= alert["window_start_ts_us"] <= ev.start_ts_us + after_us
    )


def evaluate(
    alerts: list[dict],
    events: list[PumpEvent],
    *,
    days_covered: float,
    before_minutes: int = 10,
    after_minutes: int = 30,
    pump_types: frozenset[str] = PUMP_TYPES,
) -> EvalResult:
    before_us, after_us = before_minutes * 60_000_000, after_minutes * 60_000_000
    pump_alerts = [a for a in alerts if a["alert_type"] in pump_types]

    delays: dict[str, float] = {}
    for ev in events:
        hits = [a for a in pump_alerts if _matches(a, ev, before_us, after_us)]
        if hits:
            first_end = min(a["window_end_ts_us"] for a in hits)
            delays[ev.event_id] = max(0.0, (first_end - ev.start_ts_us) / 1e6)

    true_alerts = sum(
        1 for a in pump_alerts if any(_matches(a, ev, before_us, after_us) for ev in events)
    )
    false_alerts = len(pump_alerts) - true_alerts
    sorted_delays = sorted(delays.values())
    median = None
    if sorted_delays:
        mid = len(sorted_delays) // 2
        median = (
            sorted_delays[mid]
            if len(sorted_delays) % 2
            else (sorted_delays[mid - 1] + sorted_delays[mid]) / 2
        )
    return EvalResult(
        n_events=len(events),
        n_alerts=len(pump_alerts),
        detected_events=len(delays),
        recall=len(delays) / len(events) if events else 0.0,
        precision=true_alerts / len(pump_alerts) if pump_alerts else 0.0,
        false_alerts_per_day=false_alerts / days_covered if days_covered > 0 else 0.0,
        median_delay_s=median,
        delays_s=delays,
    )


def precision_at_k(
    alerts: list[dict],
    events: list[PumpEvent],
    k: int,
    *,
    before_minutes: int = 10,
    after_minutes: int = 30,
) -> float:
    """Share of the k highest-risk pump alerts that match an event."""
    top = sorted(
        (a for a in alerts if a["alert_type"] in PUMP_TYPES), key=lambda a: -a["risk_score"]
    )[:k]
    if not top:
        return 0.0
    b, f = before_minutes * 60_000_000, after_minutes * 60_000_000
    return sum(1 for a in top if any(_matches(a, ev, b, f) for ev in events)) / len(top)
