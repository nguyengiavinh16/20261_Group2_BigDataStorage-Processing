import pytest

from cg_ml.evaluation.metrics import PumpEvent, evaluate, precision_at_k

MIN = 60_000_000
T0 = 1_794_812_400_000_000


def alert(symbol, start, risk=0.9, exchange="binance", alert_type="PUMP_SUSPECTED"):
    return {
        "symbol": symbol,
        "exchange": exchange,
        "alert_type": alert_type,
        "risk_score": risk,
        "window_start_ts_us": start,
        "window_end_ts_us": start + MIN,
    }


EVENTS = [PumpEvent("e1", "ABC-USDT", "binance", T0), PumpEvent("e2", "XYZ-USDT", "binance", T0)]


def test_evaluate_recall_precision_delay():
    alerts = [
        alert("ABC-USDT", T0 + 1 * MIN),  # detects e1, delay = 2 min (window end)
        alert("ABC-USDT", T0 + 2 * MIN),  # same event, still a true alert
        alert("QQQ-USDT", T0, risk=0.6),  # false alert
        alert("ABC-USDT", T0 + 5 * MIN, alert_type="DUMP_PHASE"),  # ignored for detection
    ]
    r = evaluate(alerts, EVENTS, days_covered=2)
    assert r.recall == 0.5 and r.detected_events == 1
    assert r.precision == pytest.approx(2 / 3)
    assert r.false_alerts_per_day == 0.5
    assert r.median_delay_s == 120.0


def test_all_exchange_alert_matches_and_precision_at_k():
    alerts = [alert("XYZ-USDT", T0, exchange="ALL", risk=0.95), alert("QQQ-USDT", T0, risk=0.99)]
    assert evaluate(alerts, EVENTS, days_covered=1).detected_events == 1
    assert precision_at_k(alerts, EVENTS, k=1) == 0.0
    assert precision_at_k(alerts, EVENTS, k=2) == 0.5
