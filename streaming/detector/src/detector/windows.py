"""Flink event-time windows for validated detector trades."""

from pyflink.common import Duration, Time, Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import FlatMapFunction, MapFunction, ProcessWindowFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import EventTimeTrigger, TumblingEventTimeWindows

from cryptoguard_common.log import get_logger
from cryptoguard_common.timeutil import floor_to_window, now_us
from detector.features import features_for_window
from detector.metrics import US_PER_MILLISECOND, log_rejection
from detector.validation import TradeInputError, ValidatedTrade, input_dlq_record
from detector.window_data import TradeWindow

WINDOW_SECONDS = 60
LATE_TRADES = OutputTag("detector-late-trades")
LATE_EXCHANGES = MapStateDescriptor("late-exchanges", Types.STRING(), Types.BOOLEAN())
COUNTED_EXCHANGES = MapStateDescriptor("counted-exchanges", Types.STRING(), Types.BOOLEAN())


class TradeTimestamp(TimestampAssigner):
    def extract_timestamp(self, record: ValidatedTrade, record_timestamp: int) -> int:
        return int(record.trade["event_ts_us"]) // US_PER_MILLISECOND


def _window_key(record: ValidatedTrade) -> tuple[str, str]:
    return record.trade.get("replay_run_id") or "", record.trade["symbol"]


class TrackLateExchanges(EventTimeTrigger):
    def on_element(self, element, timestamp, window, ctx):
        if ctx.get_current_watermark() >= window.max_timestamp():
            ctx.get_partitioned_state(LATE_EXCHANGES).put(element.trade["exchange"], True)
        return super().on_element(element, timestamp, window, ctx)

    def clear(self, window, ctx):
        ctx.get_partitioned_state(LATE_EXCHANGES).clear()
        super().clear(window, ctx)


class CollectTrades(ProcessWindowFunction):
    def process(self, key, context, elements):
        run_id, symbol = key
        window = context.window()
        window_state = context.window_state()
        late = window_state.get_map_state(LATE_EXCHANGES)
        counted = window_state.get_map_state(COUNTED_EXCHANGES)
        trades = tuple(element.trade for element in elements)
        exchanges = {"ALL", *(trade["exchange"] for trade in trades)}
        new_exchanges = tuple(
            sorted(exchange for exchange in exchanges if not counted.contains(exchange))
        )
        for exchange in new_exchanges:
            counted.put(exchange, True)
        # M05 can derive both venue features and ALL from this one raw trade set.
        yield TradeWindow(
            symbol=symbol,
            replay_run_id=run_id or None,
            window_start_ts_us=window.start * US_PER_MILLISECOND,
            window_end_ts_us=window.end * US_PER_MILLISECOND,
            trades=trades,
            late_exchanges=tuple(sorted(late.keys())),
            new_exchanges=new_exchanges,
        )

    def clear(self, context):
        context.window_state().get_map_state(COUNTED_EXCHANGES).clear()


class BuildWindowFeatures(FlatMapFunction):
    """Report window counts while processing records emitted by Flink timers."""

    def open(self, runtime_context):
        self.metrics = runtime_context.get_metrics_group()
        self.counters = {}

    def flat_map(self, window: TradeWindow):
        for exchange in window.new_exchanges:
            if exchange not in self.counters:
                self.counters[exchange] = self.metrics.add_group("exchange", exchange).counter(
                    "cg_detector_windows_total"
                )
            self.counters[exchange].inc()
        yield from features_for_window(window, computed_at_ts_us=now_us())


class ReportLateTrade(MapFunction):
    def open(self, runtime_context):
        self.late = runtime_context.get_metrics_group().counter("cg_detector_late_events_total")
        self.logger = get_logger(__name__)

    def map(self, record):
        self.late.inc()
        start_us = floor_to_window(record.trade["event_ts_us"], WINDOW_SECONDS)
        error = TradeInputError("LATE_EVENT", f"Trade arrived after expiry of window {start_us}")
        dlq = input_dlq_record(record.source, error, step="event_time")
        log_rejection(self.logger, dlq, record.trade.get("replay_run_id"))
        return dlq


def assign_trade_watermarks(trades, watermark_seconds: int, idle_timeout_seconds: int):
    strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(watermark_seconds))
        .with_idleness(Duration.of_seconds(idle_timeout_seconds))
        .with_timestamp_assigner(TradeTimestamp())
    )
    return trades.assign_timestamps_and_watermarks(strategy)


def window_trades(timestamped_trades, allowed_lateness_seconds: int):
    """Return window snapshots and one DLQ record per expired trade."""
    windows = (
        timestamped_trades.key_by(
            _window_key, key_type=Types.TUPLE([Types.STRING(), Types.STRING()])
        )
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .trigger(TrackLateExchanges())
        .allowed_lateness(allowed_lateness_seconds * 1000)
        .side_output_late_data(LATE_TRADES)
        .process(CollectTrades())
    )
    return windows, windows.get_side_output(LATE_TRADES).map(ReportLateTrade())
