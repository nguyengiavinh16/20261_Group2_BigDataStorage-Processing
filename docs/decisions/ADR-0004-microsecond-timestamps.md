# ADR-0004: Integer epoch-microsecond timestamps in all messages

- Status: Accepted (supersedes the ISO-8601 strings used in W1 schemas)
- Date: 2026-11-16

## Context
Sources disagree: Binance WS = ms numbers, binance.vision spot files = µs since 2025, OKX = ms as strings,
Coinbase/Kraken = ISO-8601 with micro/nanoseconds, Ethereum = block seconds. Mixed units are a classic silent bug.

## Decision
Every timestamp in Kafka messages and exported records is an **integer number of microseconds since the Unix epoch,
UTC**, with field suffix `_ts_us`. Conversion happens only at the edges with `cryptoguard_common.timeutil`
(`epoch_to_us` detects s/ms/µs/ns by magnitude; `iso_to_us` keeps exact microseconds).
Iceberg tables keep the `_ts_us` column and add a derived `TIMESTAMP` for partitioning and SQL.

## Consequences
- + Arithmetic (latency, windows) is trivial and exact; no timezone ambiguity.
- − Less human-readable in raw JSON; dashboards/API clients format it.
