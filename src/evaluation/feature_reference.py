"""Đặc tả chạy được của docs/feature-list.md (W2, owner: Thái).

Không phải pipeline production. Dùng để:
  1. kiểm chứng giá trị kỳ vọng trong tests/fixtures/feature_cases.json;
  2. làm chuẩn đối chiếu với Spark batch (Đạt) và Structured Streaming (Công Anh) ở W4.

Tên trường đầu vào theo transaction event (project plan mục 7.1) và hai trường số dư tùy chọn
`source_balance_before` / `destination_balance_before` (đề xuất, chỉ cần cho feature set B).
"""
from __future__ import annotations

import math
import numbers
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

import numpy as np

SPEC_VERSION = "w2-v1"

# Thứ tự cố định (alphabetAsc). Spark StringIndexer mặc định sắp theo tần suất → phải đặt stringOrderType="alphabetAsc".
TRANSACTION_TYPES = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
HISTORY_WINDOW_STEPS = 24
DRAIN_TOLERANCE = 0.005  # nửa cent: amount và số dư có 2 chữ số thập phân

F_ID, F_TYPE, F_AMOUNT = "transaction_id", "transaction_type", "amount"
F_DEST, F_TIME, F_STEP = "destination_account", "event_time", "step"
F_OLD_SRC, F_OLD_DEST = "source_balance_before", "destination_balance_before"

CORE_FEATURES = tuple(f"type_{t}" for t in TRANSACTION_TYPES) + ("log_amount",)
TIME_FEATURES = ("hour_of_day",)
BALANCE_FEATURES = ("oldbalanceOrg", "oldbalanceDest", "amount_to_balance_ratio", "drains_origin", "dest_balance_zero")
HISTORY_FEATURES = ("dest_txn_count_past_24h", "dest_amount_sum_past_24h", "dest_seen_before")

FEATURE_SETS = {
    "A": ("core",),
    "A+T": ("core", "time"),
    "A+H": ("core", "history"),
    "A+H+T": ("core", "history", "time"),
    "B": ("core", "balance"),
}

# Chuẩn hóa khóa (bỏ ký tự không phải chữ/số, hạ chữ thường) rồi so khớp: bắt isFraud, is_fraud, IS_FRAUD...
_FORBIDDEN_KEYS = frozenset({
    "isfraud", "isflaggedfraud", "label", "fraudlabel",
    "newbalanceorig", "newbalancedest", "newbalancesource", "newbalancedestination",
    "sourcebalanceafter", "destinationbalanceafter",
})


class FeatureInputError(ValueError):
    """Đầu vào không hợp lệ. `code` ổn định để quarantine/thống kê theo lý do."""

    def __init__(self, code: str, fields: str | Sequence[str], message: str = ""):
        self.code = code
        self.fields = (fields,) if isinstance(fields, str) else tuple(fields)
        detail = f" — {message}" if message else ""
        super().__init__(f"{code}: {', '.join(self.fields)}{detail}")


# ---------------------------------------------------------------- kiểm tra đầu vào

def forbidden_fields(event: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(k for k in event if "".join(c for c in str(k).lower() if c.isalnum()) in _FORBIDDEN_KEYS))


def _reject_forbidden(event: Mapping[str, Any]) -> None:
    found = forbidden_fields(event)
    if found:
        raise FeatureInputError("FORBIDDEN_FIELD", found, "nhãn hoặc số dư sau giao dịch không được nằm trong event")


def _missing(event: Mapping[str, Any], names: Sequence[str]) -> list[str]:
    return [n for n in names if event.get(n) is None]


def _number(event: Mapping[str, Any], name: str) -> float:
    v = event[name]
    if isinstance(v, bool) or not isinstance(v, numbers.Real):
        raise FeatureInputError("BAD_TYPE", name, f"cần số, nhận {type(v).__name__}")
    v = float(v)
    if not math.isfinite(v):
        raise FeatureInputError("NON_FINITE", name)
    if v < 0:
        raise FeatureInputError("NEGATIVE_VALUE", name)
    return v


def _transaction_type(event: Mapping[str, Any]) -> str:
    v = event[F_TYPE]
    if not isinstance(v, str):
        raise FeatureInputError("BAD_TYPE", F_TYPE, f"cần chuỗi, nhận {type(v).__name__}")
    if v not in TRANSACTION_TYPES:  # so khớp chính xác, phân biệt hoa/thường
        raise FeatureInputError("UNKNOWN_CATEGORY", F_TYPE, repr(v))
    return v


def quality_flags(event: Mapping[str, Any]) -> tuple[str, ...]:
    """Cờ chất lượng không làm thay đổi feature. PaySim có đúng 16 dòng amount = 0 (đều là CASH_OUT gian lận)."""
    amount = event.get(F_AMOUNT)
    isnum = isinstance(amount, numbers.Real) and not isinstance(amount, bool)
    return ("AMOUNT_ZERO",) if isnum and amount == 0 else ()


# ---------------------------------------------------------------- thời gian

def parse_event_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise FeatureInputError("BAD_TYPE", F_TIME, f"cần chuỗi ISO-8601, nhận {type(value).__name__}")
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise FeatureInputError("BAD_TIMESTAMP", F_TIME, repr(value)) from exc
    if dt.tzinfo is None:
        raise FeatureInputError("BAD_TIMESTAMP", F_TIME, "thiếu múi giờ")
    return dt.astimezone(timezone.utc)


def step_from_event_time(value: Any, t0: datetime) -> int:
    """step = số giờ đã trôi qua kể từ T0 (làm tròn xuống) + 1. Chỉ dùng số học trên UTC, không dùng giờ địa phương."""
    dt = parse_event_time(value)
    if dt < t0:
        raise FeatureInputError("BAD_TIMESTAMP", F_TIME, "trước T0")
    return (dt - t0) // timedelta(hours=1) + 1


def resolve_step(event: Mapping[str, Any], t0: datetime | None = None) -> int:
    step, event_time = event.get(F_STEP), event.get(F_TIME)
    if step is None and event_time is None:
        raise FeatureInputError("MISSING_FIELD", (F_STEP, F_TIME))
    from_step = from_time = None
    if step is not None:
        if isinstance(step, bool) or not isinstance(step, numbers.Integral):
            raise FeatureInputError("BAD_TYPE", F_STEP, f"cần số nguyên, nhận {type(step).__name__}")
        if step < 1:
            raise FeatureInputError("OUT_OF_RANGE", F_STEP, "step bắt đầu từ 1")
        from_step = int(step)
    if event_time is not None:
        if t0 is None:
            raise ValueError("cần t0 để đổi event_time thành step")
        from_time = step_from_event_time(event_time, t0)
    if from_step is not None and from_time is not None and from_step != from_time:
        raise FeatureInputError("INCONSISTENT_TIME", (F_STEP, F_TIME), f"{from_step} != {from_time}")
    return from_step if from_step is not None else from_time


# ---------------------------------------------------------------- feature không trạng thái

def stateless_features(event: Mapping[str, Any], feature_set: str = "A", *, t0: datetime | None = None) -> dict[str, float]:
    """Feature tính trực tiếp từ một event. Nhóm 'history' bỏ qua ở đây (xem historical_features)."""
    groups = FEATURE_SETS[feature_set]
    _reject_forbidden(event)

    required = [F_TYPE, F_AMOUNT]
    if "balance" in groups:
        required += [F_OLD_SRC, F_OLD_DEST]
    missing = _missing(event, required)
    if "time" in groups and event.get(F_STEP) is None and event.get(F_TIME) is None:
        missing += [F_STEP, F_TIME]
    if missing:
        raise FeatureInputError("MISSING_FIELD", missing)

    ttype = _transaction_type(event)
    amount = _number(event, F_AMOUNT)
    out = {f"type_{t}": 1.0 if t == ttype else 0.0 for t in TRANSACTION_TYPES}
    out["log_amount"] = math.log1p(amount)

    if "time" in groups:
        out["hour_of_day"] = float((resolve_step(event, t0) - 1) % 24)
    if "balance" in groups:
        old_src, old_dest = _number(event, F_OLD_SRC), _number(event, F_OLD_DEST)
        out["oldbalanceOrg"] = old_src
        out["oldbalanceDest"] = old_dest
        out["amount_to_balance_ratio"] = amount / (old_src + 1.0)
        out["drains_origin"] = 1.0 if amount > 0 and abs(amount - old_src) < DRAIN_TOLERANCE else 0.0
        out["dest_balance_zero"] = 1.0 if old_dest == 0 else 0.0
    return out


# ---------------------------------------------------------------- feature lịch sử

def historical_features(events: Sequence[Mapping[str, Any]], *, t0: datetime | None = None) -> dict[str, dict[str, float]]:
    """Feature lịch sử theo `destination_account`, tính với toàn bộ `events` (không phụ thuộc thứ tự đến).

    Cửa sổ: các giao dịch tới cùng tài khoản nhận có step trong [s-24, s-1]. Cùng step không tính cho nhau, vì
    dữ liệu chỉ có độ phân giải theo giờ. `transaction_id` trùng chỉ tính một lần (bản đầu tiên).
    """
    seen_ids: set[Any] = set()
    by_dest: dict[Any, list[tuple[Any, int, float]]] = defaultdict(list)
    for event in events:
        _reject_forbidden(event)
        missing = _missing(event, [F_ID, F_DEST, F_AMOUNT])
        if missing:
            raise FeatureInputError("MISSING_FIELD", missing)
        if event[F_ID] in seen_ids:
            continue
        seen_ids.add(event[F_ID])
        by_dest[event[F_DEST]].append((event[F_ID], resolve_step(event, t0), _number(event, F_AMOUNT)))

    out: dict[str, dict[str, float]] = {}
    for items in by_dest.values():
        for tid, step, _ in items:
            window = [a for _, s, a in items if step - HISTORY_WINDOW_STEPS <= s <= step - 1]
            out[tid] = {
                "dest_txn_count_past_24h": float(len(window)),
                "dest_amount_sum_past_24h": math.fsum(window),
                "dest_seen_before": 1.0 if any(s < step for _, s, _ in items) else 0.0,
            }
    return out


# ---------------------------------------------------------------- threshold

def _unit_interval(value: Any, name: str) -> float:
    if value is None:
        raise FeatureInputError("MISSING_FIELD", name)
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise FeatureInputError("BAD_TYPE", name, f"cần số, nhận {type(value).__name__}")
    value = float(value)
    if not math.isfinite(value):
        raise FeatureInputError("NON_FINITE", name)
    if not 0.0 <= value <= 1.0:
        raise FeatureInputError("OUT_OF_RANGE", name, "phải nằm trong [0, 1]")
    return value


def alert_triggered(fraud_score: Any, threshold: Any) -> bool:
    """Quy ước: alert khi fraud_score >= threshold (bằng ngưỡng vẫn là alert)."""
    return _unit_interval(fraud_score, "fraud_score") >= _unit_interval(threshold, "threshold")


def select_threshold(y_true: Sequence[int], scores: Sequence[float], *, min_recall: float = 0.80,
                     min_precision: float | None = None) -> dict[str, Any]:
    """Chọn threshold trên tập validation (alert khi score >= threshold).

    Ứng viên là các giá trị score khác nhau. Trong các ứng viên có recall >= min_recall (và precision >= min_precision
    nếu có) chọn precision cao nhất; hòa thì recall cao hơn, rồi threshold cao hơn. Không có ứng viên nào thỏa thì chọn
    F2 cao nhất. Lưu ý: nếu không đặt min_precision, luôn có ứng viên thỏa (hạ threshold xuống score nhỏ nhất → alert tất cả),
    nên nhánh F2 không bao giờ chạy.
    """
    if not 0.0 < min_recall <= 1.0:
        raise ValueError("min_recall phải nằm trong (0, 1]")
    y = np.asarray(y_true)
    s = np.asarray(scores, dtype=float)
    if y.ndim != 1 or y.shape != s.shape or y.size == 0:
        raise ValueError("y_true và scores phải là hai mảng 1 chiều cùng độ dài, không rỗng")
    if not np.isfinite(s).all():
        raise FeatureInputError("NON_FINITE", "scores")
    if not np.isin(y, (0, 1)).all():
        raise ValueError("y_true chỉ được chứa 0 hoặc 1")
    y = y.astype(np.int64)
    n_pos = int(y.sum())
    if n_pos == 0:
        raise ValueError("y_true không có mẫu dương: recall không xác định")

    order = np.argsort(-s, kind="stable")
    s_sorted, y_sorted = s[order], y[order]
    last_of_value = np.r_[s_sorted[1:] != s_sorted[:-1], True]
    thr = s_sorted[last_of_value]
    tp = np.cumsum(y_sorted)[last_of_value]
    fp = np.cumsum(1 - y_sorted)[last_of_value]
    precision = tp / (tp + fp)
    recall = tp / n_pos
    denom = 4 * precision + recall
    f2 = np.divide(5 * precision * recall, denom, out=np.zeros_like(denom), where=denom > 0)

    ok = recall >= min_recall
    if min_precision is not None:
        ok &= precision >= min_precision
    if ok.any():
        idx = np.flatnonzero(ok)
        pick = idx[np.lexsort((thr[idx], recall[idx], precision[idx]))[-1]]
        rule = "max_precision_at_min_recall"
    else:
        pick = np.lexsort((thr, recall, f2))[-1]
        rule = "max_f2_fallback"
    return {
        "threshold": float(thr[pick]), "precision": float(precision[pick]), "recall": float(recall[pick]),
        "f2": float(f2[pick]), "alerts": int(tp[pick] + fp[pick]), "tp": int(tp[pick]), "fp": int(fp[pick]),
        "fn": int(n_pos - tp[pick]), "rule": rule,
    }
