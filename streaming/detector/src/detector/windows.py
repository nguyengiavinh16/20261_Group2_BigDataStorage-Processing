"""Flink event-time windows for validated detector trades."""

from pyflink.common import Duration, Time, Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import EventTimeTrigger, TumblingEventTimeWindows

from cryptoguard_common.timeutil import US_PER_SECOND, floor_to_window
from detector.validation import TradeInputError, ValidatedTrade, input_dlq_record
from detector.window_data import TradeWindow

WINDOW_SECONDS = 60
US_PER_MILLISECOND = US_PER_SECOND // 1000
LATE_TRADES = OutputTag("detector-late-trades")
LATE_EXCHANGES = MapStateDescriptor("late-exchanges", Types.STRING(), Types.BOOLEAN())


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
        late = context.window_state().get_map_state(LATE_EXCHANGES)
        # M05 can derive both venue features and ALL from this one raw trade set.
        yield TradeWindow(
            symbol=symbol,
            replay_run_id=run_id or None,
            window_start_ts_us=window.start * US_PER_MILLISECOND,
            window_end_ts_us=window.end * US_PER_MILLISECOND,
            trades=tuple(element.trade for element in elements),
            late_exchanges=tuple(sorted(late.keys())),
        )


class ReportLateTrade(MapFunction):
    def open(self, runtime_context):
        self.late = runtime_context.get_metrics_group().counter("cg_detector_late_events_total")

    def map(self, record):
        self.late.inc()
        start_us = floor_to_window(record.trade["event_ts_us"], WINDOW_SECONDS)
        error = TradeInputError("LATE_EVENT", f"Trade arrived after expiry of window {start_us}")
        return input_dlq_record(record.source, error, step="event_time")


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
