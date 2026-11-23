"""AWS Public Blockchain Ethereum dataset location (BAT-01 spike, BAT-07 ingestion).

Public, no AWS account needed (anonymous / --no-sign-request). One Parquet file per table per day was
seen for date=2026-11-20; schema and pitfalls in docs/source-notes.md.
"""

from datetime import date

BUCKET = "aws-public-blockchain"
PREFIX = "v1.0/eth"
REGION = "us-east-2"
TABLES = ("blocks", "transactions", "logs", "token_transfers", "traces", "contracts")


def partition_prefix(table: str, day: date | str) -> str:
    """Key prefix of one daily partition, e.g. v1.0/eth/token_transfers/date=2026-11-20/."""
    if table not in TABLES:
        raise ValueError(f"unknown table {table!r}")
    d = day.isoformat() if isinstance(day, date) else day
    return f"{PREFIX}/{table}/date={d}/"
