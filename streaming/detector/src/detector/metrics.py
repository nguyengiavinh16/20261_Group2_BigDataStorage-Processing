"""Detector latency buckets and structured diagnostic fields."""

import logging

from cryptoguard_common.log import log_kv
from cryptoguard_common.timeutil import US_PER_SECOND

US_PER_MILLISECOND = US_PER_SECOND // 1000
LATENCY_BUCKETS_MS = (100, 250, 500, 1000, 2500, 5000, 7500, 10000, 15000, 30000, 60000, 120000)


def live_alert_latency_ms(alert: dict) -> int | None:
    if alert.get("replay_run_id") is not None:
        return None
    return (alert["detected_at_ts_us"] - alert["window_end_ts_us"]) // US_PER_MILLISECOND


class LatencyHistogram:
    """Cumulative Prometheus buckets backed by PyFlink counters."""

    def __init__(self, group):
        self.count = group.counter("cg_detector_latency_ms_count")
        self.sum = group.counter("cg_detector_latency_ms_sum")
        self.buckets = [
            (upper, group.add_group("le", str(upper)).counter("cg_detector_latency_ms_bucket"))
            for upper in LATENCY_BUCKETS_MS
        ]
        self.overflow = group.add_group("le", "+Inf").counter("cg_detector_latency_ms_bucket")

    def observe(self, latency_ms: int) -> None:
        if latency_ms < 0:
            return
        self.count.inc()
        self.sum.inc(latency_ms)
        self.overflow.inc()
        for upper, counter in self.buckets:
            if latency_ms <= upper:
                counter.inc()


def log_rejection(logger, record: dict, replay_run_id: str | None = None) -> None:
    log_kv(
        logger,
        logging.WARNING,
        "trade_rejected",
        error_type=record["error_type"],
        detected_by=record["detected_by"],
        source_topic=record["source_topic"],
        source_partition=record["source_partition"],
        source_offset=record["source_offset"],
        replay_run_id=replay_run_id,
    )
