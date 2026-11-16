"""Envelope-level normalization: dispatch to adapters, turn failures into DLQ records (ING-05).

`normalize_envelope` is what the Flink job calls for every raw message. Dedup and watermarks are NOT here:
they need state/time and live in job.py (ING-06).
"""

import json

from cryptoguard_common.contracts import is_valid
from cryptoguard_common.timeutil import now_us
from normalizer.adapters import ADAPTERS, AdapterError


def dlq_record(
    error_type: str,
    message: str,
    raw_value: str,
    *,
    source_topic: str,
    detected_by: str,
    partition: int | None = None,
    offset: int | None = None,
    key: str | None = None,
) -> dict:
    return {
        "schema_version": "1.0",
        "error_type": error_type,
        "error_message": message[:2000],
        "source_topic": source_topic,
        "source_partition": partition,
        "source_offset": offset,
        "source_key": key,
        "raw_value": raw_value[:65536],
        "detected_at_ts_us": now_us(),
        "detected_by": detected_by,
    }


def normalize_envelope(
    raw_value: str,
    *,
    source_topic: str,
    partition: int | None = None,
    offset: int | None = None,
    key: str | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (canonical_trades, dlq_records) for one raw Kafka message value."""

    def fail(error_type: str, msg: str, step: str) -> tuple[list[dict], list[dict]]:
        return [], [
            dlq_record(
                error_type,
                msg,
                raw_value,
                source_topic=source_topic,
                detected_by=f"normalizer.{step}",
                partition=partition,
                offset=offset,
                key=key,
            )
        ]

    try:
        envelope = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        return fail("PARSE_ERROR", f"envelope is not JSON: {exc}", "envelope")
    if not is_valid(envelope, "raw_envelope.v1"):
        return fail("SCHEMA_VIOLATION", "envelope does not match raw_envelope.v1", "envelope")

    source, payload = envelope["source"], envelope["payload"]
    if isinstance(payload, str):
        return fail("PARSE_ERROR", "payload is not valid JSON", f"{source}_adapter")
    adapter = ADAPTERS.get(source)
    if adapter is None:
        return fail("OTHER", f"no trade adapter for source {source}", "dispatch")

    try:
        trades = adapter(payload, envelope["ingest_ts_us"], envelope.get("replay_run_id"))
    except AdapterError as exc:
        return fail(exc.error_type, str(exc), f"{source}_adapter")
    except (KeyError, TypeError, ValueError) as exc:
        return fail("SCHEMA_VIOLATION", f"{type(exc).__name__}: {exc}", f"{source}_adapter")

    good, bad = [], []
    for trade in trades:
        if is_valid(trade, "canonical_trade.v1"):
            good.append(trade)
        else:
            bad.extend(
                fail(
                    "SCHEMA_VIOLATION",
                    f"canonical output invalid: {trade.get('event_id')}",
                    f"{source}_adapter",
                )[1]
            )
    return good, bad
