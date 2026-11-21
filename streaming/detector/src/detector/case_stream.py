"""PyFlink managed case state and Alert v1 stream."""

from pyflink.common import Time, Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from cryptoguard_common.timeutil import US_PER_SECOND, floor_to_window
from detector.baseline import ScoredFeature
from detector.cases import CaseProgress, advance_case, build_alert
from detector.features import feature_key
from detector.windows import US_PER_MILLISECOND

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
            yield build_alert(hit, scored.feature, current.case.case_id)

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
