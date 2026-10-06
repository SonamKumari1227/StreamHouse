"""Is a Delta table still receiving data?

    make freshness TABLE=bronze/raw_orders_cdc MAX_AGE_HOURS=24

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

Exits 1 when the newest row is older than the threshold, which is what makes it usable as an
Airflow task: late means failed, and failed means visible.

WHY THE NEWEST ROW AND NOT THE NEWEST FILE

A Delta table's file timestamps move for reasons that have nothing to do with ingestion -
OPTIMIZE rewrites every file in a table that has received nothing for a week. Only a row
timestamp says data arrived. This reads `ingest_ts`, which Bronze stamps at write time, and
falls back to the Delta log's own commit time for tables that have no such column.

This is the check Phase 3 learned the need for the hard way: CDC not landed inside Kafka's
7-day retention is gone, so a stream that quietly stops is a deadline, not an inconvenience.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_MAX_AGE_HOURS = 24.0


def newest_row_age_hours(spark: SparkSession, path: str, column: str) -> float:
    """Hours since the most recent row landed. Raises if the table has no rows."""
    table = spark.read.format("delta").load(path)
    if column not in table.columns:
        raise ValueError(f"{path} has no column '{column}'; columns are {table.columns}")

    row = table.select(
        F.max(column).alias("newest"),
        F.current_timestamp().alias("now"),
    ).head()

    if row is None or row["newest"] is None:
        raise ValueError(f"{path} is empty; nothing has ever been ingested")

    return float((row["now"] - row["newest"]).total_seconds() / 3600.0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fail if a Delta table has gone stale.")
    parser.add_argument("--path", required=True, help="e.g. s3a://bronze/raw_orders_cdc")
    parser.add_argument(
        "--column", default="ingest_ts", help="timestamp column (default: ingest_ts)"
    )
    parser.add_argument("--max-age-hours", type=float, default=DEFAULT_MAX_AGE_HOURS)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("freshness-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        age = newest_row_age_hours(spark, args.path, args.column)
    finally:
        pass

    # One parseable line, so a DAG task does not have to scrape prose.
    print(f"FRESHNESS path={args.path} age_hours={age:.2f} limit={args.max_age_hours:.2f}")
    spark.stop()

    if age > args.max_age_hours:
        print(f"STALE: {args.path} has received nothing for {age:.1f}h")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
