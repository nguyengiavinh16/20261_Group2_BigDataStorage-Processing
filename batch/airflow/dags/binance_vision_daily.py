"""Daily: download data.binance.vision trades for the watchlist → Bronze → silver.trades (BAT-03/BAT-04).

Skeleton only. Rules for every DAG in this folder:
- idempotent: re-running a date overwrites/merges the same partition, never duplicates,
- incremental: skip files already in Bronze with the same checksum,
- bounded: respect config data_budget (max_days_history, max_symbols) in the local profile,
- heavy work runs in Spark (SparkKubernetesOperator / SparkApplication), not in the Airflow worker.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

BASE_URL = "https://data.binance.vision/data/spot/daily/trades"


@dag(
    dag_id="binance_vision_daily",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["cryptoguard", "bronze", "R4"],
)
def binance_vision_daily():
    @task
    def plan(ds: str | None = None) -> list[dict]:
        """Return [{symbol, date, url, checksum_url}] for the watchlist (native Binance symbols)."""
        raise NotImplementedError("BAT-03")

    @task
    def download_to_bronze(item: dict) -> str:
        """Stream zip → s3://cg-lake/bronze/binance_vision/spot/trades/<SYMBOL>/<date>.zip, verify .CHECKSUM."""
        raise NotImplementedError("BAT-03")

    @task
    def bronze_to_silver(paths: list[str]) -> None:
        """Submit SparkApplication cg_batch.jobs.bronze_to_silver_trades for the date."""
        raise NotImplementedError("BAT-04")

    bronze_to_silver(download_to_bronze.expand(item=plan()))


binance_vision_daily()
