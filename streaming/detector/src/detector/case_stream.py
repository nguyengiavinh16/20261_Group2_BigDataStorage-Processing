"""PyFlink managed case state and Alert v1 stream."""

import logging

from pyflink.common import Time, Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from cryptoguard_common.log import get_logger, log_kv
from cryptoguard_common.timeutil import US_PER_SECOND, floor_to_window
from detector.baseline import ScoredFeature
from detector.cases import CaseProgress, advance_case, build_alert
from detector.features import feature_key
from detector.metrics import US_PER_MILLISECOND, LatencyHistogram, live_alert_latency_ms

MIN_CASE_STATE_TTL_SECONDS = 24 * 60 * 60


class DetectCases(KeyedProcessFunction):
    def __init__(
        self,
        pump_rule: dict,
        dump_rule: dict,
        case_merge_minutes: int,
        window_seconds: int,
        allowed_lateness_seconds: int,
    ):
        self.pump_rule = pump_rule
        self.dump_rule = dump_rule
        self.gap_us = case_merge_minutes * 60 * US_PER_SECOND
        self.window_seconds = window_seconds
        self.allowed_lateness_seconds = allowed_lateness_seconds

    def open(self, runtime_context):
        # Processing-time TTL is a fallback if watermarks stop advancing.
        ttl_seconds = max(
            MIN_CASE_STATE_TTL_SECONDS,
            self.gap_us // US_PER_SECOND + self.window_seconds + self.allowed_lateness_seconds,
        )
        ttl = (
            StateTtlConfig.new_builder(Time.seconds(ttl_seconds))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor = ValueStateDescriptor("detector-case-progress", Types.PICKLED_BYTE_ARRAY())
        descriptor.enable_time_to_live(ttl)
        self.progress = runtime_context.get_state(descriptor)
        self.metrics = runtime_context.get_metrics_group()
        self.alert_counters = {}
        self.latency = None
        self.logger = get_logger(__name__)

    def _cleanup_at_ms(self, progress: CaseProgress) -> int:
        last_start_us = progress.window_start_ts_us
        if progress.case is not None:
            deadline_us = progress.case.last_window_end_ts_us + self.gap_us
            last_start_us = max(last_start_us, floor_to_window(deadline_us, self.window_seconds))
        # The last eligible window still has to close and finish emitting late revisions.
        retention_us = (self.window_seconds + self.allowed_lateness_seconds) * US_PER_SECOND
        return (last_start_us + retention_us) // US_PER_MILLISECOND

    def process_element(self, scored: ScoredFeature, ctx):
        previous, previous_timer = self.progress.value() or (None, None)
        current, hit = advance_case(
            previous, scored, self.pump_rule, self.dump_rule, gap_us=self.gap_us
        )
        if current != previous:
            timers = ctx.timer_service()
            cleanup = self._cleanup_at_ms(current)
            if cleanup != previous_timer:
                if previous_timer is not None:
                    timers.delete_event_time_timer(previous_timer)
                timers.register_event_time_timer(cleanup)
            self.progress.update((current, cleanup))
        if hit is not None:
            alert = build_alert(hit, scored.feature, current.case.case_id)
            self._observe_alert(alert)
            yield alert

    def _observe_alert(self, alert: dict) -> None:
        alert_type, severity = labels = alert["alert_type"], alert["severity"]
        if labels not in self.alert_counters:
            group = self.metrics.add_group("alert_type", alert_type).add_group("severity", severity)
            self.alert_counters[labels] = group.counter("cg_detector_alerts_total")
        self.alert_counters[labels].inc()

        latency_ms = live_alert_latency_ms(alert)
        if latency_ms is not None:
            if self.latency is None:
                self.latency = LatencyHistogram(self.metrics)
            self.latency.observe(latency_ms)
        log_kv(
            self.logger,
            logging.INFO,
            "alert_detected",
            alert_id=alert["alert_id"],
            case_id=alert["case_id"],
            alert_type=alert_type,
            severity=severity,
            symbol=alert["symbol"],
            exchange=alert["exchange"],
            replay_run_id=alert["replay_run_id"],
            window_end_ts_us=alert["window_end_ts_us"],
            detected_at_ts_us=alert["detected_at_ts_us"],
            latency_ms=latency_ms,
        )

    def on_timer(self, timestamp, ctx):
        stored = self.progress.value()
        if stored is not None and timestamp == stored[1]:
            self.progress.clear()
        return ()


def _case_key(scored: ScoredFeature) -> tuple[str, str, str]:
    return feature_key(scored.feature)


def case_alerts(
    scored,
    pump_rule: dict,
    dump_rule: dict,
    *,
    case_merge_minutes: int,
    window_seconds: int,
    allowed_lateness_seconds: int,
):
    key_type = Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()])
    return (
        scored.key_by(_case_key, key_type=key_type)
        .process(
            DetectCases(
                pump_rule, dump_rule, case_merge_minutes, window_seconds, allowed_lateness_seconds
            )
        )
        .name("Detect alert cases")
        .uid("detector-alert-cases")
    )
