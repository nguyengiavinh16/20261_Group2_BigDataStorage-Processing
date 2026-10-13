"""Explicit Spark schema for the 11 columns in the raw PaySim CSV.

Column names and order match ``configs/paysim_schema.json`` and the CSV
header, including the original spelling of ``oldbalanceOrg``.

Raw fields are nullable so missing values can be inspected and quarantined
by ETL. The schema describes types only: required fields, finite numbers,
positive steps, allowed categories and binary labels need separate validation.
``isFraud`` is ground truth and ``isFlaggedFraud`` is an existing system flag;
neither should be used as a scoring feature.

Example with strict CSV parsing and header checking::

    from src.processing.schema import PAYSIM_SCHEMA

    raw_df = (
        spark.read
        .schema(PAYSIM_SCHEMA)
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .csv("data/paysim dataset.csv")
    )

FAILFAST stops on parse errors. An ETL that quarantines malformed rows must
use a separate parsing strategy that preserves the original record.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


PAYSIM_SCHEMA = StructType(
    [
        StructField("step", IntegerType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("nameOrig", StringType(), nullable=True),
        StructField("oldbalanceOrg", DoubleType(), nullable=True),
        StructField("newbalanceOrig", DoubleType(), nullable=True),
        StructField("nameDest", StringType(), nullable=True),
        StructField("oldbalanceDest", DoubleType(), nullable=True),
        StructField("newbalanceDest", DoubleType(), nullable=True),
        StructField("isFraud", IntegerType(), nullable=True),
        StructField("isFlaggedFraud", IntegerType(), nullable=True),
    ]
)
