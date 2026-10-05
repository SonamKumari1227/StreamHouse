"""Apply an expectation suite to a Silver table and quarantine what fails.

    make quality-gate TABLE=fact_order_state
    make quality-gate TABLE=gps_trips_sessionized
    make quality-gate TABLE=dim_menu_item_scd2

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

Exit status is the contract: 0 when every row passed, 1 when any row was quarantined. Phase 5
orchestration reads that, and Phase 6 alerts on the quarantine count. The job does not delete
or alter the Silver table - a gate reports, it does not repair.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from quality.expectations import (
    SUITES,
    Expectation,
    quarantine_path,
    scd2_suite,
    validate,
    write_failures,
)

SILVER = "s3a://silver"

# The SCD2 suite is generated per dimension, since it needs the business key.
SCD2_KEYS = {
    "dim_restaurant_scd2": "restaurant_id",
    "dim_rider_scd2": "rider_id",
    "dim_menu_item_scd2": "menu_item_id",
}

TABLES = sorted({*SUITES, *SCD2_KEYS})


def suite_for(table: str) -> tuple[Expectation, ...]:
    if table in SCD2_KEYS:
        return scd2_suite(SCD2_KEYS[table])
    return SUITES[table]()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate a Silver table against its expectations.")
    parser.add_argument("--table", required=True, choices=TABLES)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName(f"quality-gate-{args.table}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(f"{SILVER}/{args.table}")
    suite = suite_for(args.table)
    result = validate(df, suite)

    total = df.count()
    passed = result.passed.count()
    failed = write_failures(result.failed, args.table)

    print(f"=== {args.table} ===")
    print(f"rows       : {total}")
    print(f"passed     : {passed}")
    print(f"quarantined: {failed}")

    if failed:
        print(f"\nquarantine : {quarantine_path(args.table)}")
        print("\nviolations by expectation:")
        counts = (
            result.failed.selectExpr("explode(violations) AS expectation")
            .groupBy("expectation")
            .count()
            .orderBy("count", ascending=False)
        )
        for row in counts.collect():
            print(f"  {row['expectation']:<32} {row['count']}")

    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
