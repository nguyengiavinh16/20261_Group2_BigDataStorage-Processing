from collections import Counter
from unittest.mock import Mock

import pytest

from detector.job import main
from detector.metrics import LatencyHistogram, live_alert_latency_ms, log_rejection


def test_latency_histogram_has_cumulative_buckets_and_ignores_clock_skew():
    totals = Counter()

    def count_metric(name):
        return Mock(inc=lambda count=1: totals.update({name: count}))

    group = Mock(counter=count_metric)
    group.add_group.side_effect = lambda key, value: Mock(
        counter=lambda name: count_metric((name, key, value))
    )
    histogram = LatencyHistogram(group)
    for latency_ms in (-5, 100, 5000, 5001, 120001):
        histogram.observe(latency_ms)

    assert totals["cg_detector_latency_ms_count"] == 4
    assert totals["cg_detector_latency_ms_sum"] == 130102
    assert totals["cg_detector_latency_ms_bucket", "le", "100"] == 1
    assert totals["cg_detector_latency_ms_bucket", "le", "5000"] == 2
    assert totals["cg_detector_latency_ms_bucket", "le", "7500"] == 3
    assert totals["cg_detector_latency_ms_bucket", "le", "+Inf"] == 4


def test_latency_is_whole_milliseconds_from_window_close_and_live_only():
    alert = {"window_end_ts_us": 60_000_000, "detected_at_ts_us": 65_123_456}
    assert live_alert_latency_ms(alert) == 5123
    assert live_alert_latency_ms(alert | {"replay_run_id": "old-market-data"}) is None


def test_rejection_diagnostic_has_run_and_source_coordinates_without_payload():
    record = {
        "error_type": "LATE_EVENT",
        "detected_by": "detector.event_time",
        "source_topic": "replay.canonical.trades.v1",
        "source_partition": 1,
        "source_offset": 10,
        "raw_value": "original record",
    }
    logger = Mock()
    log_rejection(logger, record, replay_run_id="test-run")
    assert logger.log.call_args.kwargs["extra"]["extra_fields"] == {
        key: value for key, value in record.items() if key != "raw_value"
    } | {"replay_run_id": "test-run"}


@pytest.mark.parametrize("port", ["-1", "65535", "true"])
def test_invalid_metrics_port_rejected_before_flink_starts(port, monkeypatch, capsys):
    monkeypatch.setenv("CG_DETECTOR__METRICS_PORT", port)
    with pytest.raises(SystemExit, match="2"):
        main(["--preview"])
    assert "detector.metrics_port" in capsys.readouterr().err
