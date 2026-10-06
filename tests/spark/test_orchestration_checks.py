"""The scripts the Phase 5 DAGs call.

Run with `make test-spark`.

These exist as scripts rather than as Python inlined in a DAG precisely so they can be tested
here and run by hand. The first version of `api_extracts` piped a script to
`spark-submit /dev/stdin`, which failed in two seconds with an empty stdout and no usable
error - untestable and undiagnosable at once.

What matters about all three: **the exit code is the contract.** Airflow reads it, so a check
that returns 0 on a problem is worse than no check at all.

PYTHON 3.8 - this runs inside the Spark image. See ADR-0009.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from quality.backfill_check import checksum
from quality.freshness_check import newest_row_age_hours
from quality.weather_coverage import days_for_city

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("orchestration-check-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# --------------------------------------------------------------------------- freshness

BRONZE_ISH = StructType(
    [
        StructField("pk", LongType()),
        StructField("ingest_ts", TimestampType()),
    ]
)


def write_delta(spark: SparkSession, rows: list[Any], schema: StructType, path: str) -> None:
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").save(path)


def test_a_recent_table_reads_as_fresh(spark: SparkSession, tmp_path: Any) -> None:
    path = str(tmp_path / "fresh")
    now = datetime.utcnow()
    write_delta(spark, [(1, now - timedelta(minutes=5))], BRONZE_ISH, path)

    assert newest_row_age_hours(spark, path, "ingest_ts") < 1.0


def test_an_old_table_reads_as_stale(spark: SparkSession, tmp_path: Any) -> None:
    """The case the DAG exists to catch: ingestion stopped and nothing failed."""
    path = str(tmp_path / "stale")
    write_delta(spark, [(1, datetime.utcnow() - timedelta(hours=48))], BRONZE_ISH, path)

    assert newest_row_age_hours(spark, path, "ingest_ts") > 24.0


def test_freshness_uses_the_newest_row_not_the_oldest(spark: SparkSession, tmp_path: Any) -> None:
    """A table with old rows is fine as long as new ones keep arriving."""
    path = str(tmp_path / "mixed")
    now = datetime.utcnow()
    write_delta(
        spark,
        [(1, now - timedelta(days=30)), (2, now - timedelta(minutes=1))],
        BRONZE_ISH,
        path,
    )

    assert newest_row_age_hours(spark, path, "ingest_ts") < 1.0


def test_an_empty_table_is_an_error_not_a_pass(spark: SparkSession, tmp_path: Any) -> None:
    """A table that has never received anything must not read as fresh."""
    path = str(tmp_path / "empty")
    write_delta(spark, [], BRONZE_ISH, path)

    with pytest.raises(ValueError, match="empty"):
        newest_row_age_hours(spark, path, "ingest_ts")


def test_a_missing_column_says_so(spark: SparkSession, tmp_path: Any) -> None:
    path = str(tmp_path / "wrongcols")
    write_delta(spark, [(1, datetime.utcnow())], BRONZE_ISH, path)

    with pytest.raises(ValueError, match="no column"):
        newest_row_age_hours(spark, path, "not_a_column")


# --------------------------------------------------------------------------- weather coverage

WEATHER = StructType(
    [
        StructField("city", StringType()),
        StructField("weather_date", DateType()),
        StructField("precipitation_mm", DoubleType()),
    ]
)


def test_a_covered_city_counts_its_days(spark: SparkSession, tmp_path: Any) -> None:
    path = str(tmp_path / "weather")
    write_delta(
        spark,
        [
            ("Bengaluru", date(2026, 10, 1), 0.0),
            ("Bengaluru", date(2026, 10, 2), 4.0),
            ("Pune", date(2026, 10, 1), 1.0),
        ],
        WEATHER,
        path,
    )

    assert days_for_city(spark, "Bengaluru", path) == 2
    assert days_for_city(spark, "Pune", path) == 1


def test_a_missing_city_counts_zero(spark: SparkSession, tmp_path: Any) -> None:
    """dim_weather left-joins, so a missing city is silent downstream. This is what notices."""
    path = str(tmp_path / "weather")
    write_delta(spark, [("Bengaluru", date(2026, 10, 1), 0.0)], WEATHER, path)

    assert days_for_city(spark, "Chennai", path) == 0


# --------------------------------------------------------------------------- backfill checksum

AGG = StructType(
    [
        StructField("order_date", DateType()),
        StructField("city", StringType()),
        StructField("delivered_orders", LongType()),
    ]
)

DAY = date(2026, 9, 28)


def write_agg(spark: SparkSession, rows: list[Any], path: str) -> None:
    spark.createDataFrame(rows, AGG).write.format("delta").mode("overwrite").save(path)


def test_the_same_data_checksums_the_same(
    spark: SparkSession, tmp_path: Any, monkeypatch: Any
) -> None:
    """Determinism. Without it, "bit-identical" is unprovable rather than merely false."""
    import quality.backfill_check as bc

    path = str(tmp_path / "agg")
    monkeypatch.setattr(bc, "table_path", lambda table: path)
    write_agg(spark, [(DAY, "Pune", 10), (DAY, "Delhi", 4)], path)

    assert checksum(spark, "agg_sla_daily", str(DAY)) == checksum(spark, "agg_sla_daily", str(DAY))


def test_row_order_does_not_change_the_checksum(
    spark: SparkSession, tmp_path: Any, monkeypatch: Any
) -> None:
    """Spark guarantees no ordering, so a rebuild in a different order is still correct.

    Summing per-row hashes rather than hashing a concatenation is what makes that true.
    """
    import quality.backfill_check as bc

    path = str(tmp_path / "agg")
    monkeypatch.setattr(bc, "table_path", lambda table: path)

    write_agg(spark, [(DAY, "Pune", 10), (DAY, "Delhi", 4)], path)
    first = checksum(spark, "agg_sla_daily", str(DAY))

    write_agg(spark, [(DAY, "Delhi", 4), (DAY, "Pune", 10)], path)
    assert checksum(spark, "agg_sla_daily", str(DAY)) == first


def test_changed_content_changes_the_checksum(
    spark: SparkSession, tmp_path: Any, monkeypatch: Any
) -> None:
    """The failure the whole mechanism exists to detect.

    One order different in one city - a row count would not notice, and this must.
    """
    import quality.backfill_check as bc

    path = str(tmp_path / "agg")
    monkeypatch.setattr(bc, "table_path", lambda table: path)

    write_agg(spark, [(DAY, "Pune", 10), (DAY, "Delhi", 4)], path)
    before = checksum(spark, "agg_sla_daily", str(DAY))

    write_agg(spark, [(DAY, "Pune", 11), (DAY, "Delhi", 4)], path)
    after = checksum(spark, "agg_sla_daily", str(DAY))

    assert before[0] == after[0], "row counts are equal, which is the point"
    assert before[1] != after[1], "content differs and the hash must say so"


def test_other_days_do_not_affect_a_days_checksum(
    spark: SparkSession, tmp_path: Any, monkeypatch: Any
) -> None:
    """A backfill is scoped to one day; so is its proof."""
    import quality.backfill_check as bc

    path = str(tmp_path / "agg")
    monkeypatch.setattr(bc, "table_path", lambda table: path)

    write_agg(spark, [(DAY, "Pune", 10)], path)
    before = checksum(spark, "agg_sla_daily", str(DAY))

    write_agg(spark, [(DAY, "Pune", 10), (date(2026, 9, 29), "Pune", 99)], path)
    assert checksum(spark, "agg_sla_daily", str(DAY)) == before
