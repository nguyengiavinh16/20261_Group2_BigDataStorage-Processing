"""PaySim batch ETL: raw CSV -> validated Parquet and replay handoff.

Run from the project root: python -m src.processing.etl --limit 10000
Or run directly: python ./src/processing/etl.py --limit 10000
Input is one local UTF-8 CSV with one record per physical line. Local Docker
storage is supported here; HDFS and multiline CSV require separate ingestion.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

if __package__:
    from .schema import PAYSIM_SCHEMA
else:
    from schema import PAYSIM_SCHEMA


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "etl.json"
RAW_COLUMNS = PAYSIM_SCHEMA.fieldNames()
COLUMN_MAPPING = {
    "step": "step",
    "type": "transaction_type",
    "amount": "amount",
    "nameOrig": "source_account",
    "oldbalanceOrg": "old_balance_origin",
    "newbalanceOrig": "new_balance_origin",
    "nameDest": "destination_account",
    "oldbalanceDest": "old_balance_destination",
    "newbalanceDest": "new_balance_destination",
    "isFraud": "label",
    "isFlaggedFraud": "existing_fraud_flag",
}
REPLAY_COLUMNS = (
    "transaction_id", "step", "transaction_type", "amount",
    "source_account", "destination_account",
)
LOG = logging.getLogger("paysim.etl")


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def project_path(value):
    path = Path(value).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()


def create_spark(settings):
    """A local batch session inside Docker, following the lab's Kryo setup."""
    # Python workers must use the active Conda/container interpreter, too.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder
        .appName(settings["app_name"])
        .master(settings["master"])
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.sql.shuffle.partitions", str(settings["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings["log_level"])
    LOG.info("Spark %s | master=%s", spark.version, spark.sparkContext.master)
    return spark


def extract(spark, input_path, limit):
    """Preserve every physical data line, including malformed/blank records."""
    field_count = len(RAW_COLUMNS)

    # A nested parser is serialized with its closure; workers need no imports
    # from this project. csv.reader handles quoted commas and broken quotes.
    def parse_partition(records):
        import csv

        for raw_record, index in records:
            error = None
            try:
                values = next(csv.reader([raw_record], strict=True))
                if len(values) != field_count:
                    error = "csv_field_count"
                    values = [None] * field_count
            except (csv.Error, StopIteration):
                error = "csv_parse_error"
                values = [None] * field_count
            yield (index + 1, raw_record, error, *values)

    # Hadoop Path escapes spaces itself. as_uri() percent-encodes them first,
    # making Hadoop look for a literal "%20" in local filenames on Windows.
    indexed = spark.sparkContext.textFile(input_path.as_posix()).zipWithIndex()
    records = indexed.filter(lambda item: item[1] > 0)
    if limit is not None:
        records = records.filter(lambda item: item[1] <= limit)
    schema = StructType([
        StructField("source_line", LongType(), False),
        StructField("raw_record", StringType(), False),
        StructField("parse_error", StringType(), True),
        *[StructField("raw_" + name, StringType(), True) for name in RAW_COLUMNS],
    ])
    return spark.createDataFrame(records.mapPartitions(parse_partition), schema)


def transform(raw, checksum, input_path, domains):
    """Cast safely, normalize and attach every applicable validation reason."""
    reasons = [F.when(F.col("parse_error").isNotNull(), F.col("parse_error"))]
    result = raw
    parsed = F.col("parse_error").isNull()
    integer_names = {"step", "isFraud", "isFlaggedFraud"}

    for field in PAYSIM_SCHEMA.fields:
        name = field.name
        text = F.trim(F.col("raw_" + name))
        present = text.isNotNull() & (F.length(text) > 0)
        reasons.append(F.when(parsed & ~present, F.lit(name + "_required")))
        if field.dataType.simpleString() == "string":
            value = F.upper(text) if name == "type" else text
        else:
            target = field.dataType.simpleString()
            value = F.expr(f"try_cast(trim(`raw_{name}`) AS {target})")
            if name in integer_names:
                # Prevent permissive string-to-int casts from truncating 1.5.
                value = F.when(text.rlike(r"^[+-]?[0-9]+$"), value)
            reasons.append(F.when(parsed & present & value.isNull(), F.lit(name + "_invalid_type")))
            if name not in integer_names:
                nonfinite = F.isnan(value) | (F.abs(value) == F.lit(float("inf")))
                reasons.append(F.when(parsed & present & nonfinite, F.lit(name + "_nonfinite")))
        result = result.withColumn(name, value)

    reasons.extend([
        F.when(F.col("step") < domains["step_minimum"], F.lit("step_nonpositive")),
        F.when(F.col("amount") < domains["amount_minimum"], F.lit("amount_negative")),
        F.when(~F.col("type").isin(domains["transaction_types"]), F.lit("unknown_type")),
    ])
    for name, values in domains["label_values"].items():
        reasons.append(F.when(~F.col(name).isin(values), F.lit(name + "_outside_binary")))

    return (
        result
        .withColumn("error_reasons", F.filter(F.array(*reasons), lambda reason: reason.isNotNull()))
        .withColumn("source_file", F.lit(str(input_path)))
        .withColumn("input_sha256", F.lit(checksum))
        .withColumn("transaction_id", F.sha2(F.concat(F.lit(checksum + ":"), F.col("source_line").cast("string")), 256))
    )


def distributions(dataframe):
    return {
        name: {str(row[name]): row["count"] for row in dataframe.groupBy(name).count().collect()}
        for name in ("label", "transaction_type")
    }


def verify_output(spark, expected, path, expected_count):
    actual = spark.read.parquet(str(path))
    expected_types = [(field.name, field.dataType.simpleString()) for field in expected.schema]
    actual_types = [(field.name, field.dataType.simpleString()) for field in actual.schema]
    if expected_types != actual_types:
        raise RuntimeError(f"Parquet schema mismatch: {path}")
    actual_count = actual.count()
    if actual_count != expected_count:
        raise RuntimeError(f"Parquet count mismatch: {path}: {actual_count} != {expected_count}")
    return actual, {"rows": actual_count, "schema_matches": True, "schema": actual_types}


def git_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, stderr=subprocess.DEVNULL, text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_etl(config_path=DEFAULT_CONFIG_PATH, *, input_path=None, output_root=None, limit=None, master=None):
    """Run all three ETL stages, verify outputs and return the run manifest."""
    started = time.perf_counter()
    config_path = project_path(config_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    schema_path = project_path(config["schema_path"])
    reference = json.loads(schema_path.read_text(encoding="utf-8"))
    if [column["name"] for column in reference["columns"]] != RAW_COLUMNS:
        raise ValueError("Reference schema column order differs from PAYSIM_SCHEMA")
    source = project_path(input_path if input_path is not None else config["input_path"])
    destination = project_path(output_root if output_root is not None else config["output_root"])
    if not source.is_file():
        raise ValueError(f"Input must be one local CSV file: {source}")
    with source.open("r", encoding="utf-8-sig", newline="") as stream:
        try:
            header = next(csv.reader(stream, strict=True))
        except (StopIteration, csv.Error) as error:
            raise ValueError("CSV header is missing or malformed") from error
    if header != RAW_COLUMNS:
        raise ValueError(f"CSV header must be exactly {RAW_COLUMNS}; got {header}")
    if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool) or limit < 1):
        raise ValueError("limit must be a positive integer, or omitted for the full dataset")
    version = config["dataset_version"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", version):
        raise ValueError("dataset_version must be a simple directory name")
    if master is not None:
        config["spark"]["master"] = master
    if not config["spark"]["master"].startswith("local["):
        raise ValueError("This single-file Docker ETL currently supports local Spark masters only")
    if config["spark"]["shuffle_partitions"] < 1:
        raise ValueError("shuffle_partitions must be positive")
    checksum = sha256_file(source)
    input_stat = source.stat()
    effective_config = dict(config, input_path=str(source), output_root=str(destination), limit=limit)
    schema_hash = sha256_file(schema_path)
    code_hash = sha256_file(Path(__file__))
    config_hash = hashlib.sha256(json.dumps(effective_config, sort_keys=True).encode()).hexdigest()
    run_hash = hashlib.sha256(f"{checksum}:{config_hash}:{schema_hash}:{code_hash}".encode()).hexdigest()[:12]
    run_dir = destination / version / run_hash
    if source.is_relative_to(run_dir.resolve()):
        raise ValueError("Output directory must not contain the input CSV")
    run_dir.mkdir(parents=True, exist_ok=True)
    lock_path = run_dir / ".etl.lock"
    try:
        lock = lock_path.open("x", encoding="utf-8")
    except FileExistsError as error:
        raise RuntimeError(f"Run directory is locked: {run_dir}; check for a running job") from error
    lock.close()
    outputs = {name: str(run_dir / name) for name in ("processed", "quarantine", "replay", "labels")}
    manifest_path = run_dir / "manifest.json"
    manifest = {
        "status": "running", "run_id": run_hash, "started_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_version": version, "input_path": str(source), "input_sha256": checksum,
        "input_size_bytes": input_stat.st_size, "config": effective_config,
        "config_sha256": config_hash, "schema_version": reference["schema_version"],
        "schema_sha256": schema_hash, "etl_code_sha256": code_hash, "git_commit": git_commit(),
        "outputs": outputs, "column_mapping": COLUMN_MAPPING,
        "id_policy": "sha256(input_sha256 + ':' + 1-based physical source_line)",
        "duplicate_policy": "report_and_retain", "validation_policy_status": reference["status"],
        "event_time_policy": "not_generated; step retained pending timestamp contract",
        "storage": "local Docker filesystem; HDFS not verified",
    }
    handler = logging.FileHandler(run_dir / "etl.log", mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOG.addHandler(handler)
    LOG.setLevel(logging.INFO)
    spark = None
    validated = None
    try:
        write_json(manifest_path, manifest)
        LOG.info("EXTRACT %s | sha256=%s | limit=%s", source, checksum, limit)
        spark = create_spark(config["spark"])
        manifest["spark_version"] = spark.version
        validated = transform(extract(spark, source, limit), checksum, source, reference["domain_checks"])
        validated.persist(StorageLevel.MEMORY_AND_DISK)
        summary = validated.agg(
            F.count("*").alias("raw"),
            F.sum(F.when(F.size("error_reasons") == 0, 1).otherwise(0)).alias("valid"),
        ).first()
        counts = {"raw": summary["raw"], "valid": summary["valid"] or 0}
        counts["quarantine"] = counts["raw"] - counts["valid"]
        valid = validated.filter(F.size("error_reasons") == 0)
        quarantine = validated.filter(F.size("error_reasons") > 0).select(
            "transaction_id", "source_file", "source_line", "input_sha256", "raw_record",
            *["raw_" + name for name in RAW_COLUMNS], "error_reasons",
        )
        processed = valid.select(
            "transaction_id", "source_file", "source_line", "input_sha256",
            *[F.col(name).alias(COLUMN_MAPPING[name]) for name in RAW_COLUMNS],
        )
        replay = processed.select(*REPLAY_COLUMNS)
        labels = processed.select("transaction_id", "label")
        duplicate_summary = validated.groupBy("raw_record").count().agg(
            F.sum(F.col("count") - 1).alias("duplicates")
        ).first()
        counts["duplicate_rows"] = duplicate_summary["duplicates"] or 0
        reason_counts = {
            row["reason"]: row["count"]
            for row in quarantine.select(F.explode("error_reasons").alias("reason")).groupBy("reason").count().collect()
        }
        LOG.info("TRANSFORM raw=%s valid=%s quarantine=%s duplicates retained=%s", counts["raw"], counts["valid"], counts["quarantine"], counts["duplicate_rows"])
        before_distribution = distributions(processed)
        datasets = {"processed": processed, "quarantine": quarantine, "replay": replay, "labels": labels}
        readback = {}
        for name, dataframe in datasets.items():
            LOG.info("LOAD %s -> %s", name, outputs[name])
            dataframe.write.mode("overwrite").option("compression", config["parquet_compression"]).parquet(outputs[name])
            expected_count = counts["quarantine"] if name == "quarantine" else counts["valid"]
            actual, readback[name] = verify_output(spark, dataframe, outputs[name], expected_count)
            counts[name] = expected_count
            if name == "processed":
                if actual.select("transaction_id").distinct().count() != expected_count:
                    raise RuntimeError("Processed transaction IDs are not unique")
                if distributions(actual) != before_distribution:
                    raise RuntimeError("Label/type distributions changed after Parquet write")
        if counts["raw"] != counts["valid"] + counts["quarantine"]:
            raise RuntimeError("Row reconciliation failed")
        if source.stat().st_size != input_stat.st_size or source.stat().st_mtime_ns != input_stat.st_mtime_ns:
            raise RuntimeError("Input file changed during ETL; outputs cannot be trusted")
        manifest.update(
            status="verified", counts=counts, error_reason_counts=reason_counts,
            readback=readback, distributions=before_distribution,
            completed_utc=datetime.now(timezone.utc).isoformat(), runtime_seconds=round(time.perf_counter() - started, 3),
        )
        write_json(manifest_path, manifest)
        report = (
            "# PaySim ETL run report\n\n"
            f"Status: verified\n\nInput: `{source}`\n\nInput SHA-256: `{checksum}`\n\n"
            "| Count | Rows |\n|---|---:|\n"
            + "".join(f"| {name} | {count:,} |\n" for name, count in counts.items())
            + "\nChecks passed: row reconciliation, all four Parquet schemas/counts, processed ID uniqueness, label/type distribution readback.\n\n"
            + "Duplicates, zero amounts and outliers are retained. Error reasons may overlap.\n\n"
            + "Replay contains only ID, step, type, amount and account identifiers; labels are stored separately.\n\n"
            + "Validation defaults remain subject to team review. Event time and HDFS have not been verified.\n"
        )
        (run_dir / "etl-test-report.md").write_text(report, encoding="utf-8")
        LOG.info("VERIFIED %s | runtime=%.3fs", manifest_path, manifest["runtime_seconds"])
        return manifest
    except Exception as error:
        manifest.update(status="failed", error=str(error), runtime_seconds=round(time.perf_counter() - started, 3))
        write_json(manifest_path, manifest)
        LOG.exception("ETL failed")
        raise
    finally:
        if validated is not None:
            validated.unpersist()
        if spark is not None:
            spark.stop()
        LOG.removeHandler(handler)
        handler.close()
        lock_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--input", dest="input_path", help="One local CSV file, using the container path")
    parser.add_argument("--output-root", help="Parent directory for versioned ETL outputs")
    parser.add_argument("--limit", type=int, help="Process the first N physical data lines, including invalid rows")
    parser.add_argument("--master", help="Override local[2] from config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    manifest = run_etl(args.config, input_path=args.input_path, output_root=args.output_root, limit=args.limit, master=args.master)
    print(json.dumps({"status": manifest["status"], "counts": manifest["counts"], "outputs": manifest["outputs"]}, indent=2))


if __name__ == "__main__":
    main()
