"""Create every namespace and table of batch/ddl/iceberg_tables.sql in the Iceberg catalog (BAT-02).

Idempotent (`IF NOT EXISTS` only): safe to re-run after every DDL change, existing tables are not altered.

    # host, Spark local mode (MinIO localhost:30900, Postgres localhost:30432, values from .env)
    PYTHONPATH=batch/src python -m cg_batch.jobs.apply_ddl
    # cluster: kubectl apply -f batch/k8s/apply-ddl.sparkapplication.yaml
    # --smoke also writes, reads and drops a scratch table (proves the S3FileIO + catalog write path)
"""

import argparse
import logging
import time
from pathlib import Path

from cg_batch.ddl import created_object, default_ddl_path, split_statements, with_catalog
from cg_batch.spark_session import get_spark
from cryptoguard_common.config import load_config
from cryptoguard_common.log import get_logger, log_kv

log = get_logger("cg_batch.jobs.apply_ddl")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Apply batch/ddl/iceberg_tables.sql (BAT-02)")
    parser.add_argument(
        "--ddl", default=None, help="DDL file (default: batch/ddl/iceberg_tables.sql)"
    )
    parser.add_argument("--smoke", action="store_true", help="write/read/drop a scratch table too")
    args = parser.parse_args(argv)

    started = time.monotonic()
    cfg = load_config()
    catalog = cfg["iceberg"]["catalog_name"]
    path = Path(args.ddl) if args.ddl else default_ddl_path()
    statements = [with_catalog(s, catalog) for s in split_statements(path.read_text("utf-8"))]

    spark = get_spark("cg-apply-ddl", cfg)
    tables = []
    for stmt in statements:
        spark.sql(stmt)
        obj = created_object(stmt)
        if obj and obj[0] == "table":
            tables.append(obj[1])

    missing = [t for t in tables if not spark.catalog.tableExists(t)]
    smoke_rows = _smoke(spark, catalog) if args.smoke else None
    log_kv(
        log,
        logging.INFO if not missing else logging.ERROR,
        "ddl applied",
        job="apply_ddl",
        ddl=str(path),
        rows_in=len(statements),
        rows_out=len(tables) - len(missing),
        rows_rejected=len(missing),
        tables=tables,
        missing=missing,
        smoke_rows=smoke_rows,
        duration_s=round(time.monotonic() - started, 1),
    )
    spark.stop()
    if missing:
        raise SystemExit(f"tables missing after DDL: {missing}")
    if args.smoke and smoke_rows != 2:
        raise SystemExit(f"smoke table read back {smoke_rows} rows, expected 2")


def _smoke(spark, catalog: str) -> int:
    table = f"{catalog}.silver.cg_smoke_test"
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")  # leftover of an interrupted run
    spark.sql(f"CREATE TABLE {table} (id BIGINT, note STRING) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    rows = spark.table(table).count()
    spark.sql(f"DROP TABLE {table} PURGE")
    return rows


if __name__ == "__main__":
    main()
