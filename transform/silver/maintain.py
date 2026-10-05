"""Delta maintenance for the Silver tables: compaction and retention.

    make silver-maintain
    make silver-maintain DRY_RUN=1     # report file counts, change nothing

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

WHY THIS EXISTS

Streaming micro-batches write small files - one or more per batch, per partition. A table
written by a continuous stream accumulates thousands of them, and a query then spends its time
opening files rather than reading rows. `OPTIMIZE` rewrites them into a few large ones.

`ZORDER BY` co-locates rows that are read together, so a predicate on the Z-ordered column
can skip whole files rather than opening them to find out. The column is the one queries
filter on: the business key for the dimensions, `order_id` for the fact.

VACUUM, AND THE RETENTION THAT IS NOT A FREE CHOICE

`VACUUM` deletes files no longer referenced by the current version - which is what makes time
travel work, so deleting them ends it. The retention is therefore a stated policy, not a
default to accept quietly:

**168 hours (7 days).** It matches the Kafka topic retention deliberately. Within that window
both the raw events and the table's history are available, so a bad batch can be diagnosed
from either end and reprocessed. Beyond it, neither is - and a policy that promised longer
Delta history than the upstream topic can supply would be false comfort.

Delta refuses a retention below 168 hours unless a safety check is disabled, because a shorter
window can delete files a concurrent reader is still using. **This job does not disable it.**
`--retention-hours` can only widen.
"""

from __future__ import annotations

import argparse
import sys
from typing import NamedTuple

from pyspark.sql import SparkSession

SILVER = "s3a://silver"
DEFAULT_RETENTION_HOURS = 168


class Table(NamedTuple):
    name: str
    zorder_by: str

    @property
    def path(self) -> str:
        return f"{SILVER}/{self.name}"


# Z-order on the column queries filter by. For the dimensions that is the business key, which
# is also what Gold joins on.
TABLES = (
    Table("fact_order_state", "order_id"),
    Table("dim_restaurant_scd2", "restaurant_id"),
    Table("dim_rider_scd2", "rider_id"),
    Table("dim_menu_item_scd2", "menu_item_id"),
    Table("gps_trips_sessionized", "trip_id"),
)


def file_count(spark: SparkSession, path: str) -> int:
    """How many files the current version references."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return -1
    files: list[str] = spark.read.format("delta").load(path).inputFiles()
    return len(files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compact and vacuum the Silver tables.")
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=DEFAULT_RETENTION_HOURS,
        help=f"VACUUM retention, minimum {DEFAULT_RETENTION_HOURS} (default: same)",
    )
    parser.add_argument("--dry-run", action="store_true", help="report only, change nothing")
    args = parser.parse_args(argv)

    if args.retention_hours < DEFAULT_RETENTION_HOURS:
        print(
            f"refusing retention below {DEFAULT_RETENTION_HOURS}h: it can delete files a "
            "concurrent reader still needs, and it would outlive neither the Kafka topic "
            "nor the stated policy. See the module docstring.",
            file=sys.stderr,
        )
        return 2

    from delta.tables import DeltaTable

    spark = SparkSession.builder.appName("silver-maintain").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        if not DeltaTable.isDeltaTable(spark, table.path):
            print(f"{table.name:<24} not built yet, skipping")
            continue

        before = file_count(spark, table.path)
        if args.dry_run:
            print(f"{table.name:<24} {before} file(s)  (dry run, nothing changed)")
            continue

        delta = DeltaTable.forPath(spark, table.path)
        delta.optimize().executeZOrderBy(table.zorder_by)
        after = file_count(spark, table.path)

        delta.vacuum(args.retention_hours)
        print(
            f"{table.name:<24} {before} -> {after} file(s), "
            f"zordered by {table.zorder_by}, vacuumed at {args.retention_hours}h"
        )

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
