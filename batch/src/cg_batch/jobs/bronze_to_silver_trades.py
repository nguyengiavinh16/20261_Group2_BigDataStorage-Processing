"""Bronze → silver.trades (BAT-04). NOT IMPLEMENTED YET.

    spark-submit ... -m cg_batch.jobs.bronze_to_silver_trades --date 2026-11-01 [--symbol BTCUSDT]

Steps:
1. Read bronze/binance_vision/spot/trades/<SYMBOL>/<date>.zip (CSV without header) and, if present,
   bronze/kafka/canonical.trades.v1/dt=<date>/.
2. Map rows with cryptoguard_common.binance_vision.trade_row_to_canonical (via mapInPandas) → CanonicalTrade v1.
3. Add event_ts = timestamp_micros(event_ts_us), source.
4. Dedup on event_id; MERGE INTO cg.silver.trades (idempotent re-runs of the same date).
5. Log counts in/out/duplicates/rejected → rows for gold.data_quality_daily.
"""


def main() -> None:
    raise NotImplementedError("BAT-04")


if __name__ == "__main__":
    main()
