"""Daily: download data.binance.vision trades for the watchlist → Bronze (BAT-03; Silver step = BAT-04).

Logic lives in cg_batch.bronze (unit-tested); this file only wires Airflow tasks.
- idempotent: re-running a date never duplicates (same sha256 in Bronze → skipped),
- incremental: only missing days are downloaded, verified against binance.vision .CHECKSUM,
- bounded: config data_budget (max_symbols, max_days_history) unless allow_over_budget is set,
- heavy work runs in Spark (BAT-04 SparkApplication), not in the Airflow worker.

Manual runs (Trigger DAG w/ config), e.g. the evaluation shortlist or a backfill:
    {"pump_ids": ["GTO-BTC:binance:1604419200000000"], "allow_over_budget": true}
    {"symbols": ["ETH-USDT"], "dates": ["2026-11-20", "2026-11-21"]}
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param


@dag(
    dag_id="binance_vision_daily",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["cryptoguard", "bronze", "R4"],
    params={
        "symbols": Param(
            None, type=["null", "array"], description="canonical symbols, default: watchlist"
        ),
        "dates": Param(
            None, type=["null", "array"], description="YYYY-MM-DD, default: the run date"
        ),
        "pump_ids": Param(
            None, type=["null", "array"], description="pump_events_candidates.csv ids"
        ),
        "allow_over_budget": Param(False, type="boolean"),
    },
)
def binance_vision_daily():
    @task
    def plan(ds: str | None = None, params: dict | None = None) -> list[dict]:
        """Return BronzeItem dicts {native_symbol, day, url, key} for this run."""
        from cg_batch.bronze import plan as plan_items
        from cryptoguard_common.config import load_config

        p = params or {}
        items = plan_items(
            load_config(),
            ds=ds,
            symbols=p.get("symbols"),
            dates=p.get("dates"),
            pump_ids=p.get("pump_ids"),
            allow_over_budget=bool(p.get("allow_over_budget")),
        )
        print(f"planned {len(items)} files")
        return [i.as_dict() for i in items]

    @task(max_active_tis_per_dagrun=4)
    def download_to_bronze(item: dict) -> dict:
        """Stream zip → s3://<bucket>/bronze/binance_vision/spot/trades/<SYMBOL>/<date>.zip, verify .CHECKSUM."""
        from cg_batch.bronze import BronzeItem, transfer
        from cg_batch.storage import s3_client
        from cryptoguard_common.config import load_config

        cfg = load_config()
        return transfer(BronzeItem(**item), s3_client(cfg), cfg["storage"]["bucket"])

    @task
    def summarize(results: list[dict]) -> dict:
        """Counts per status for the run log / data quality (BAT-14). Conflicts fail the run."""
        from cg_batch.bronze import summarize as summarize_results

        summary = summarize_results(list(results))
        print(summary)
        if summary["conflict"]:
            raise AirflowFailException(
                f"Bronze objects with a different sha256: {summary['conflict_keys']}"
            )
        return summary

    # BAT-04 appends: submit the SparkApplication cg_batch.jobs.bronze_to_silver_trades for the planned days.
    summarize(download_to_bronze.expand(item=plan()))


binance_vision_daily()
