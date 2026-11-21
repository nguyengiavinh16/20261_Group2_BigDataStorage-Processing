"""PyFlink validation and deduplication operators for the detector."""

from pyflink.common import Time, Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from cryptoguard_common.log import get_logger
from detector.metrics import log_rejection
from detector.validation import (
    SourceRecord,
    TradeInputError,
    input_dlq_record,
    trade_dedup_key,
    validate_trade,
)

INPUT_DLQ = OutputTag("detector-input-dlq")


class ValidateTrades(ProcessFunction):
    def __init__(self, mode: str, replay_run_id: str | None = None):
        self.mode = mode
        self.replay_run_id = replay_run_id

    def open(self, runtime_context):
        metrics = runtime_context.get_metrics_group()
        self.received = metrics.counter("cg_detector_input_records_total")
        self.invalid = metrics.counter("cg_detector_invalid_records_total")
        self.filtered = metrics.counter("cg_detector_replay_filtered_total")
        self.logger = get_logger(__name__)

    def process_element(self, row, ctx):
        self.received.inc()
        record = SourceRecord(
            row.record_key,
            row.record_value,
            row.source_topic,
            row.source_partition,
            row.source_offset,
        )
        try:
            trade = validate_trade(record, self.mode)
        except TradeInputError as exc:
            self.invalid.inc()
            dlq = input_dlq_record(record, exc, step="validation")
            log_rejection(self.logger, dlq)
            yield INPUT_DLQ, dlq
        else:
            if (
                self.replay_run_id is not None
                and trade.trade.get("replay_run_id") != self.replay_run_id
            ):
                self.filtered.inc()
            else:
                yield trade


class DeduplicateTrades(KeyedProcessFunction):
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds

    def open(self, runtime_context):
        ttl = (
            StateTtlConfig.new_builder(Time.seconds(self.ttl_seconds))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        descriptor = ValueStateDescriptor("detector-seen-event", Types.BOOLEAN())
        descriptor.enable_time_to_live(ttl)
        self.seen = runtime_context.get_state(descriptor)
        metrics = runtime_context.get_metrics_group()
        self.accepted = metrics.counter("cg_detector_accepted_records_total")
        self.duplicates = metrics.counter("cg_detector_duplicate_records_total")
        self.logger = get_logger(__name__)

    def process_element(self, record, ctx):
        if self.seen.value():
            self.duplicates.inc()
            error = TradeInputError(
                "DUPLICATE", "Event ID already seen within the deduplication TTL"
            )
            dlq = input_dlq_record(record.source, error, step="deduplication")
            log_rejection(self.logger, dlq, record.trade.get("replay_run_id"))
            yield INPUT_DLQ, dlq
        else:
            self.seen.update(True)
            self.accepted.inc()
            yield record


def prepare_trades(records, mode: str, ttl_seconds: int, *, replay_run_id: str | None = None):
    """Return unique canonical trades and an input DLQ stream, before event time."""
    validated = records.process(ValidateTrades(mode, replay_run_id))
    malformed = validated.get_side_output(INPUT_DLQ)
    unique = validated.key_by(
        trade_dedup_key, key_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    ).process(DeduplicateTrades(ttl_seconds))
    return unique, malformed.union(unique.get_side_output(INPUT_DLQ))
