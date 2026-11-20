"""PyFlink validation and deduplication operators for the detector."""

from pyflink.common import Time, Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from detector.validation import (
    SourceRecord,
    TradeInputError,
    input_dlq_record,
    trade_dedup_key,
    validate_trade,
)

INPUT_DLQ = OutputTag("detector-input-dlq")


class ValidateTrades(ProcessFunction):
    def __init__(self, mode: str):
        self.mode = mode

    def open(self, runtime_context):
        metrics = runtime_context.get_metrics_group()
        self.received = metrics.counter("cg_detector_input_records_total")
        self.invalid = metrics.counter("cg_detector_invalid_records_total")

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
            yield INPUT_DLQ, input_dlq_record(record, exc, step="validation")
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

    def process_element(self, record, ctx):
        if self.seen.value():
            self.duplicates.inc()
            error = TradeInputError(
                "DUPLICATE", "Event ID already seen within the deduplication TTL"
            )
            yield INPUT_DLQ, input_dlq_record(record.source, error, step="deduplication")
        else:
            self.seen.update(True)
            self.accepted.inc()
            yield record


def prepare_trades(records, mode: str, ttl_seconds: int):
    """Return unique canonical trades and an input DLQ stream, before event time."""
    validated = records.process(ValidateTrades(mode))
    malformed = validated.get_side_output(INPUT_DLQ)
    unique = validated.key_by(
        trade_dedup_key, key_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    ).process(DeduplicateTrades(ttl_seconds))
    return unique, malformed.union(unique.get_side_output(INPUT_DLQ))
