"""Kafka → MongoDB sink (MLS-04). NOT IMPLEMENTED YET.

    python -m cg_serving.sink --mode live|replay

- live:   alerts.v1 → 'alerts' (+ upsert 'cases'), features.symbol_1m.v1 → 'symbol_features_recent'
- replay: replay.alerts.v1 → 'alerts_replay', replay.features.symbol_1m.v1 → ignored or separate collection
Rules: validate every message against its contract (invalid → log + count, do not crash); idempotent upserts
(_id = alert_id; features keyed by symbol/exchange/window_start); commit offsets after the bulk write.
Case document: {_id: case_id, symbol, exchange, status: 'open'|'closed', first_alert_ts_us, last_alert_ts_us,
                max_risk, alert_types: [...], alert_count}. Closed when no alert for detector.case_merge_minutes.
"""


def main() -> None:
    raise NotImplementedError("MLS-04")


if __name__ == "__main__":
    main()
