"""Detection rules (DET-05). Input: one SymbolFeatures1m dict (with volume_z filled). Output: rule hits.

A RuleHit is turned into an Alert v1 by cases.build_alert. Rules must:
- be deterministic and configurable (thresholds from config `detector.*`),
- return machine-readable reasons (code, value, threshold),
- never fire on incomplete data without saying so (reason INCOMPLETE_WINDOW).
Reason codes are catalogued in docs/data-contracts.md; add new codes there in the same PR.
"""

from dataclasses import dataclass, field, replace

from detector.baseline import ScoredFeature


@dataclass
class RuleHit:
    alert_type: str
    risk_score: float
    rule_version: str
    reasons: list[dict] = field(default_factory=list)
    baseline_source: str | None = None


def _reason(code: str, value, threshold=None, description: str | None = None) -> dict:
    return {"code": code, "value": value, "threshold": threshold, "description": description}


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def evaluate_pump(features: dict, rule: dict) -> RuleHit | None:
    """pump-v1: all of volume_z, price change, taker-buy dominance and minimum activity must hold.

    risk = 0.5 + 0.5 * mean(strengths), so a firing rule is at least 'medium'. Strength of each condition:
      volume:  volume_z / (2 * volume_z_min)            (saturates at 2x threshold)
      price:   price_change_pct / (2 * price_change_pct_min)
      taker:   (taker_buy_ratio - 0.5) / 0.5
    """
    vz = features.get("volume_z")
    pct = features["price_change_pct"]
    tbr = features.get("taker_buy_ratio")
    if vz is None or tbr is None or features["trade_count"] < rule["min_trades"]:
        return None
    if (
        vz < rule["volume_z_min"]
        or pct < rule["price_change_pct_min"]
        or tbr < rule["taker_buy_ratio_min"]
    ):
        return None

    strengths = [
        _clip01(vz / (2 * rule["volume_z_min"])),
        _clip01(pct / (2 * rule["price_change_pct_min"])),
        _clip01((tbr - 0.5) / 0.5),
    ]
    reasons = [
        _reason("VOLUME_Z_HIGH", round(vz, 3), rule["volume_z_min"]),
        _reason("PRICE_SPIKE", round(pct, 3), rule["price_change_pct_min"]),
        _reason("TAKER_BUY_DOMINANT", round(tbr, 4), rule["taker_buy_ratio_min"]),
    ]
    if not features.get("is_complete", True):
        reasons.append(_reason("INCOMPLETE_WINDOW", True, None, "gap or late events in window"))
    return RuleHit(
        "PUMP_SUSPECTED",
        round(0.5 + 0.5 * sum(strengths) / len(strengths), 4),
        rule["version"],
        reasons,
    )


def evaluate_dump(features: dict, rule: dict, pump_case_open: bool) -> RuleHit | None:
    """dump-v1: a sharp drop while a pump case is open for this symbol/exchange."""
    if not pump_case_open:
        return None
    pct = features["price_change_pct"]
    if pct > rule["price_change_pct_max"]:
        return None
    strength = _clip01(pct / (2 * rule["price_change_pct_max"]))
    return RuleHit(
        "DUMP_PHASE",
        round(0.5 + 0.5 * strength, 4),
        rule["version"],
        [_reason("PRICE_DROP_AFTER_PUMP", round(pct, 3), rule["price_change_pct_max"])],
    )


def evaluate_scored_feature(
    scored: ScoredFeature, pump_rule: dict, dump_rule: dict, *, pump_case_open: bool
) -> RuleHit | None:
    """Apply detector eligibility, then evaluate the configured rule versions."""
    features = scored.feature
    if scored.baseline_source is None or features.get("volume_z") is None:
        return None
    hit = evaluate_pump(features, pump_rule)
    if hit is None:
        hit = evaluate_dump(features, dump_rule, pump_case_open)
    if hit is None:
        return None

    reasons = hit.reasons
    if not features.get("is_complete", True) and not any(
        reason["code"] == "INCOMPLETE_WINDOW" for reason in reasons
    ):
        reasons = [
            *reasons,
            _reason("INCOMPLETE_WINDOW", True, None, "gap or late events in window"),
        ]
    return replace(hit, reasons=reasons, baseline_source=scored.baseline_source)
