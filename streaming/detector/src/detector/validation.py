"""Decode and validate detector input before assigning event time."""

import json
import math
from dataclasses import dataclass

from cryptoguard_common.contracts import ContractError, validate
from cryptoguard_common.timeutil import now_us


@dataclass(frozen=True)
class SourceRecord:
    record_key: bytes | None
    record_value: bytes | None
    source_topic: str
    source_partition: int
    source_offset: int


@dataclass(frozen=True)
class ValidatedTrade:
    trade: dict
    source: SourceRecord


class TradeInputError(ValueError):
    def __init__(self, error_type: str, message: str):
        super().__init__(message)
        self.error_type = error_type


def _reject_constant(value: str) -> float:
    raise ValueError(f"{value} is not valid JSON")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("JSON number is outside the finite float range")
    return parsed


def validate_trade(record: SourceRecord, mode: str) -> ValidatedTrade:
    """Return a canonical trade with its raw record, or a classified input error."""
    if record.record_value is None:
        raise TradeInputError("PARSE_ERROR", "Kafka value is null")
    try:
        trade = json.loads(
            record.record_value.decode("utf-8"),
            parse_constant=_reject_constant,
            parse_float=_finite_float,
        )
    except (ValueError, RecursionError) as exc:
        raise TradeInputError("PARSE_ERROR", f"Cannot decode canonical trade: {exc}") from exc

    if not isinstance(trade, dict):
        raise TradeInputError("SCHEMA_VIOLATION", "Canonical trade must be a JSON object")
    try:
        validate(trade, "canonical_trade.v1")
    except ContractError as exc:
        raise TradeInputError("SCHEMA_VIOLATION", str(exc)) from exc

    try:
        key = record.record_key.decode("utf-8") if record.record_key is not None else None
    except UnicodeDecodeError as exc:
        raise TradeInputError("PARSE_ERROR", "Kafka key is not UTF-8") from exc
    if key != trade["symbol"]:
        raise TradeInputError("INVALID_VALUE", "Kafka key must match the canonical trade symbol")

    run_id = trade.get("replay_run_id")
    if mode == "replay" and (run_id is None or not run_id.strip()):
        raise TradeInputError("INVALID_VALUE", "Replay trades require a non-empty replay_run_id")
    if mode == "live" and run_id is not None:
        raise TradeInputError(
            "INVALID_VALUE", "Live trades must have a null or absent replay_run_id"
        )
    return ValidatedTrade(trade, record)


def trade_dedup_key(record: ValidatedTrade) -> tuple[str, str]:
    """Scope each event ID to its replay run, or to the live stream."""
    return record.trade.get("replay_run_id") or "", record.trade["event_id"]


def input_dlq_record(record: SourceRecord, error: TradeInputError, *, step: str) -> dict:
    """Build a DlqRecord v1 with the original position and a UTF-8 payload preview."""
    # Backslash-escape invalid bytes and preserve the 64 KiB cap after encoding.
    raw = (
        "null"
        if record.record_value is None
        else record.record_value[:65536].decode("utf-8", errors="backslashreplace")
    )
    return {
        "schema_version": "1.0",
        "error_type": error.error_type,
        "error_message": str(error)[:2000],
        "source_topic": record.source_topic,
        "source_partition": record.source_partition,
        "source_offset": record.source_offset,
        "source_key": (
            None
            if record.record_key is None
            else record.record_key.decode("utf-8", errors="backslashreplace")
        ),
        "raw_value": raw.encode("utf-8")[:65536].decode("utf-8", errors="ignore"),
        "detected_at_ts_us": now_us(),
        "detected_by": f"detector.{step}",
    }
