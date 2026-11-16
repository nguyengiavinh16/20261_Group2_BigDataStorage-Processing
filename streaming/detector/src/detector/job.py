"""PyFlink job: canonical.trades.v1 → features.symbol_1m.v1 + alerts.v1 (DET-04). NOT IMPLEMENTED YET.

Parameters: --mode live|replay  (replay reads replay.canonical.trades.v1 and writes replay.* topics,
            resolved with cryptoguard_common.contracts.replay_topic so names never drift from the registry).

Required behaviour (acceptance criteria in team/03-realtime-detection.md):
1. Event time from event_ts_us, bounded out-of-orderness = detector.watermark_seconds,
   allowed lateness = detector.allowed_lateness_seconds; late events → dlq.detector.v1 (LATE_EVENT) + metric.
2. key_by (symbol, exchange) AND a second branch key_by symbol with exchange='ALL'.
3. Tumbling 60 s windows (and 300 s sliding by 60 s in P4) → ProcessWindowFunction calling
   cryptoguard_common.features.window_features (SAME function as batch → no train/serve skew).
4. volume_z: baseline (mean, std) per symbol/exchange/hour-of-day from Redis key
   baseline:<symbol>:<exchange>:<hour> (written daily by batch, BAT-05). Missing → volume_z = null, no alert.
5. Rules: detector.rules.evaluate_pump / evaluate_dump with config detector.pump_rule / dump_rule.
6. Cases: keyed state equivalent of detector.cases.CaseTracker; build_alert → alerts.v1 (key = case_id).
7. Metrics: cg_detector_alerts_total{type}, cg_detector_latency_ms (detected_at - window_end, live mode).
8. P3: enrich with RISKY_FUNDING_INFLOW reason from canonical.onchain_transfers.v1 + Redis wallet_risk (DET-08).
"""


def main() -> None:
    raise NotImplementedError("DET-04")


if __name__ == "__main__":
    main()
