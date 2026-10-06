"""Prove a backfill is idempotent: checksum a day, destroy it, rebuild it, compare.

    make backfill-checksum TABLE=agg_sla_daily DATE=2026-09-28
    make backfill-delete   TABLE=agg_sla_daily DATE=2026-09-28

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

WHAT "BIT-IDENTICAL" HAS TO MEAN

Not "the same number of rows", which a wrong rebuild passes easily. The checksum here is over
the **content** of every row: each row is rendered to a string, hashed, and the hashes are
summed. Summing rather than concatenating makes the result independent of row order - which
matters, because Spark gives no ordering guarantee and a rebuild that produced identical data
in a different order is still correct.

Two things would break determinism and are therefore absent from the aggregate being checked:
anything reading the clock, and anything depending on arrival order. `agg_sla_daily` is a pure
aggregate of the fact rows for a day.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER = "s3a://silver"
GOLD_WAREHOUSE = "s3a://gold/warehouse"

# The column each table is partitioned/filtered by for a day-scoped operation.
DATE_COLUMN = {
    "agg_sla_daily": "order_date",
}


def table_path(table: str) -> str:
    """dbt writes marts into the warehouse directory under <schema>.db/<table>."""
    return f"{GOLD_WAREHOUSE}/gold.db/{table}"


def day_rows(spark: SparkSession, table: str, day: str) -> DataFrame:
    column = DATE_COLUMN[table]
    return spark.read.format("delta").load(table_path(table)).filter(F.col(column) == day)


def checksum(spark: SparkSession, table: str, day: str) -> tuple[int, int]:
    """(row_count, order-independent content hash) for one day."""
    rows = day_rows(spark, table, day)
    count = rows.count()

    # to_json renders the whole row deterministically; crc32 of each, summed, is independent
    # of the order the rows come back in.
    total = (
        rows.select(
            F.crc32(F.to_json(F.struct([rows[c] for c in sorted(rows.columns)]))).alias("h")
        )
        .agg(F.sum("h").alias("t"))
        .head()["t"]
    )
    return count, int(total or 0)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Checksum or delete one day of a Gold table.")
    parser.add_argument("--table", required=True, choices=sorted(DATE_COLUMN))
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--action",
        default="checksum",
        choices=("checksum", "delete"),
        help="checksum (default) prints count and hash; delete removes that day",
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName(f"backfill-{args.action}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if args.action == "delete":
        from delta.tables import DeltaTable

        column = DATE_COLUMN[args.table]
        DeltaTable.forPath(spark, table_path(args.table)).delete(f"{column} = '{args.date}'")
        count, _ = checksum(spark, args.table, args.date)
        print(f"DELETED {args.table} {args.date}; rows now: {count}")
        spark.stop()
        return 0

    count, digest = checksum(spark, args.table, args.date)
    # A single parseable line, so a DAG task can compare two runs without scraping prose.
    print(f"CHECKSUM table={args.table} date={args.date} rows={count} hash={digest}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
