-- Iceberg tables (Spark SQL). Catalog `cg` = JDBC catalog on Postgres (ADR-0005). Owner: R4.
-- These definitions ARE the batch contract: consumers (R3 baselines, R5 training/eval/dashboards) rely on them.
-- Change process = same as contracts/ (PR approved by consumers; breaking → new table name *_v2 + ADR).
-- Timestamps: *_ts_us BIGINT (same as Kafka contracts) + a derived TIMESTAMP column for partitioning/queries.

CREATE NAMESPACE IF NOT EXISTS cg.silver;
CREATE NAMESPACE IF NOT EXISTS cg.gold;

-- ======================= SILVER =======================

-- CanonicalTrade v1 rows from (a) binance.vision history and (b) archived canonical.trades.v1.
CREATE TABLE IF NOT EXISTS cg.silver.trades (
  event_id        STRING    NOT NULL,   -- dedup key
  exchange        STRING    NOT NULL,
  symbol          STRING    NOT NULL,
  base            STRING,
  quote           STRING,
  native_symbol   STRING,
  trade_id        STRING,
  price           DOUBLE,
  qty             DOUBLE,
  quote_qty       DOUBLE,
  quote_qty_usd   DOUBLE,
  taker_side      STRING,
  event_ts_us     BIGINT    NOT NULL,
  ingest_ts_us    BIGINT,
  event_ts        TIMESTAMP NOT NULL,
  source          STRING    NOT NULL    -- 'binance_vision' | 'live'
) USING iceberg
PARTITIONED BY (days(event_ts), exchange);

-- OnchainTransfer v1 rows from AWS public blockchain token_transfers (+ archived stream).
CREATE TABLE IF NOT EXISTS cg.silver.onchain_transfers (
  event_id        STRING    NOT NULL,
  chain           STRING    NOT NULL,
  block_number    BIGINT,
  block_ts_us     BIGINT    NOT NULL,
  block_ts        TIMESTAMP NOT NULL,
  tx_hash         STRING,
  log_index       INT,
  token_address   STRING,
  token_symbol    STRING,
  from_address    STRING,
  to_address      STRING,
  amount_raw      STRING,
  amount          DOUBLE,
  amount_usd      DOUBLE,
  status          STRING
) USING iceberg
PARTITIONED BY (days(block_ts), token_symbol);

-- 1-minute USD price per asset (from binance.vision klines or trades). Used for as-of USD joins.
CREATE TABLE IF NOT EXISTS cg.silver.minute_price (
  asset           STRING    NOT NULL,   -- e.g. ETH, BTC (USD-pegged assets are 1.0 and not stored)
  minute_ts_us    BIGINT    NOT NULL,
  minute_ts       TIMESTAMP NOT NULL,
  price_usd       DOUBLE    NOT NULL,
  source          STRING
) USING iceberg
PARTITIONED BY (days(minute_ts));

-- Label v1 rows (OFAC, darklist, pump dataset, elliptic, manual).
CREATE TABLE IF NOT EXISTS cg.silver.labels (
  label_id          STRING NOT NULL,
  entity_type       STRING NOT NULL,
  entity_id         STRING NOT NULL,
  label             STRING NOT NULL,
  source            STRING NOT NULL,
  confidence        DOUBLE,
  valid_from_ts_us  BIGINT,
  valid_to_ts_us    BIGINT,
  metadata_json     STRING
) USING iceberg
PARTITIONED BY (source);

-- ======================= GOLD =======================

-- SymbolFeatures1m v1, computed with cryptoguard_common.features (same code as the streaming detector).
CREATE TABLE IF NOT EXISTS cg.gold.symbol_features_1m (
  symbol STRING, exchange STRING, window_seconds INT,
  window_start_ts_us BIGINT, window_end_ts_us BIGINT, window_start TIMESTAMP,
  trade_count BIGINT, volume_base DOUBLE, volume_usd DOUBLE,
  open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, vwap DOUBLE,
  price_change_pct DOUBLE, taker_buy_ratio DOUBLE, volume_z DOUBLE, baseline_volume_usd DOUBLE,
  is_complete BOOLEAN, computed_at_ts_us BIGINT
) USING iceberg
PARTITIONED BY (days(window_start), exchange);

-- Baseline stats for volume_z (exported to Redis key baseline:<symbol>:<exchange>:<hour_of_day>).
CREATE TABLE IF NOT EXISTS cg.gold.symbol_baseline (
  symbol STRING, exchange STRING, hour_of_day INT,
  mean_volume_usd DOUBLE, std_volume_usd DOUBLE, n_windows BIGINT,
  baseline_days INT, as_of_date DATE
) USING iceberg
PARTITIONED BY (as_of_date);

-- Aggregated wallet → wallet flows (input of the graph job).
CREATE TABLE IF NOT EXISTS cg.gold.wallet_edges (
  src STRING, dst STRING, token_symbol STRING,
  total_usd DOUBLE, tx_count BIGINT,
  first_ts_us BIGINT, last_ts_us BIGINT, as_of_date DATE
) USING iceberg
PARTITIONED BY (as_of_date);

-- WalletRisk v1 (exported to Redis and MongoDB).
CREATE TABLE IF NOT EXISTS cg.gold.wallet_risk (
  address STRING, chain STRING, risk_score DOUBLE, taint_score DOUBLE,
  hops_to_nearest_bad INT, nearest_bad_address STRING, nearest_bad_source STRING,
  label_sources ARRAY<STRING>, is_exchange_wallet BOOLEAN,
  pagerank DOUBLE, fan_in BIGINT, fan_out BIGINT, total_in_usd DOUBLE, total_out_usd DOUBLE,
  first_seen_ts_us BIGINT, last_seen_ts_us BIGINT, as_of_date DATE, computed_at_ts_us BIGINT
) USING iceberg
PARTITIONED BY (as_of_date);

-- Ground-truth pump events usable for evaluation (subset of labels with data available on Binance).
CREATE TABLE IF NOT EXISTS cg.gold.pump_cases (
  pump_id STRING, symbol STRING, exchange STRING,
  announce_ts_us BIGINT, source STRING, has_trade_data BOOLEAN, notes STRING
) USING iceberg;

-- Archive of alerts.v1 / replay.alerts.v1 (from Bronze) for evaluation and dashboards.
CREATE TABLE IF NOT EXISTS cg.gold.alerts_history (
  alert_id STRING, case_id STRING, alert_type STRING, symbol STRING, exchange STRING,
  window_start_ts_us BIGINT, window_end_ts_us BIGINT, detected_at_ts_us BIGINT,
  risk_score DOUBLE, severity STRING, reasons_json STRING, rule_version STRING, model_version STRING,
  replay_run_id STRING, detected_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(detected_at));

-- Output O7 (data quality report), one row per source per day.
CREATE TABLE IF NOT EXISTS cg.gold.data_quality_daily (
  dt DATE, source STRING, records_in BIGINT, records_valid BIGINT, duplicates BIGINT,
  schema_errors BIGINT, unknown_symbols BIGINT, gaps BIGINT, late_events BIGINT, dlq_count BIGINT,
  p95_ingest_lag_ms DOUBLE, computed_at_ts_us BIGINT
) USING iceberg
PARTITIONED BY (dt);
