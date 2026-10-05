"""Silver SCD2 dimensions, asserted against a real Spark and a real Delta MERGE.

Run with `make test-spark`.

The correctness bar the Silver README sets is one sentence - *validity windows never overlap
for any key* - and it is the last assertion in this file. Everything above it exists because
each is a distinct way to breach that bar:

* a version chained to the wrong neighbour leaves a gap or an overlap
* two rows left `is_current` for one key means two windows claim "now"
* a replayed batch appends a duplicate version on top of the open one
* opening a version for a change that altered nothing we track inflates history with
  windows that say the same thing twice

PYTHON 3.8 - this runs inside the Spark image. See ADR-0009.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from transform.silver.dim_scd2 import (
    MENU_ITEMS,
    merge_batch,
    new_versions,
    parse_changes,
    stage_for_merge,
)

pytestmark = pytest.mark.spark

SPEC = MENU_ITEMS


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("silver-scd2-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


BRONZE_ROW = StructType(
    [
        StructField("pk", LongType()),
        StructField("lsn", LongType()),
        StructField("op", StringType()),
        StructField("before_json", StringType()),
        StructField("after_json", StringType()),
    ]
)


def payload(item: int = 7, price: str = "100.00", available: bool = True, at: str = "01") -> str:
    return json.dumps(
        {
            "menu_item_id": item,
            "restaurant_id": 3,
            "name": "Paneer Tikka",
            "price_inr": float(price),
            "is_available": available,
            "updated_at": f"2026-10-{at}T12:00:00.000000Z",
        }
    )


def change(
    lsn: int,
    price: str = "100.00",
    item: int = 7,
    available: bool = True,
    at: str = "01",
    op: str = "u",
) -> tuple[Any, ...]:
    return (item, lsn, op, None, payload(item, price, available, at))


def versions_for(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    df = spark.createDataFrame(rows, BRONZE_ROW)
    return new_versions(parse_changes(df, SPEC), SPEC, None)


# --------------------------------------------------------------------------- parsing


def test_price_keeps_its_scale(spark: SparkSession) -> None:
    """A dimension that rounds money cannot answer what an order actually cost."""
    df = spark.createDataFrame([change(1, price="249.99")], BRONZE_ROW)
    row = parse_changes(df, SPEC).collect()[0]
    assert row["price_inr"] == Decimal("249.99")


def test_valid_from_is_the_source_commit_time(spark: SparkSession) -> None:
    """Not processing time. A window saying when Spark noticed a change joins to nothing."""
    df = spark.createDataFrame([change(1, at="03")], BRONZE_ROW)
    row = parse_changes(df, SPEC).collect()[0]
    assert str(row["valid_from"]) == "2026-10-03 12:00:00"


# --------------------------------------------------------------------------- chaining


def test_a_single_change_is_open_ended(spark: SparkSession) -> None:
    rows = versions_for(spark, [change(1, price="100.00")]).collect()
    assert len(rows) == 1
    assert rows[0]["is_current"] is True
    assert rows[0]["valid_to"] is None


def test_versions_chain_within_one_batch(spark: SparkSession) -> None:
    """Three price points arriving together are three versions, not one.

    This is where a dimension differs from the fact table: collapsing to the newest would
    discard exactly the price history the dimension exists to keep.
    """
    rows = versions_for(
        spark,
        [
            change(1, price="100.00", at="01"),
            change(2, price="110.00", at="02"),
            change(3, price="120.00", at="03"),
        ],
    )
    by_lsn = {r["lsn"]: r for r in rows.collect()}
    assert len(by_lsn) == 3

    # Each window closes exactly where the next opens: no gap, no overlap.
    assert str(by_lsn[1]["valid_to"]) == "2026-10-02 12:00:00"
    assert str(by_lsn[2]["valid_from"]) == "2026-10-02 12:00:00"
    assert str(by_lsn[2]["valid_to"]) == "2026-10-03 12:00:00"
    assert by_lsn[3]["valid_to"] is None

    assert [by_lsn[i]["is_current"] for i in (1, 2, 3)] == [False, False, True]


def test_only_the_last_version_is_current(spark: SparkSession) -> None:
    rows = versions_for(spark, [change(1, price="100.00"), change(2, price="110.00")])
    assert rows.filter("is_current").count() == 1


def test_untracked_change_opens_no_version(spark: SparkSession) -> None:
    """updated_at moves on every write. Only tracked columns justify a new version.

    Without this the dimension grows a version per touch, each saying what the last one said.
    """
    rows = versions_for(
        spark,
        [
            change(1, price="100.00", at="01"),
            change(2, price="100.00", at="02"),  # same tracked values, later timestamp
            change(3, price="100.00", at="03"),
        ],
    )
    assert rows.count() == 1


def test_a_value_returning_to_a_previous_one_is_still_a_new_version(spark: SparkSession) -> None:
    """100 -> 110 -> 100 is three versions. Only consecutive repeats collapse."""
    rows = versions_for(
        spark,
        [
            change(1, price="100.00", at="01"),
            change(2, price="110.00", at="02"),
            change(3, price="100.00", at="03"),
        ],
    )
    assert rows.count() == 3


def test_keys_are_chained_independently(spark: SparkSession) -> None:
    rows = versions_for(
        spark,
        [
            change(1, item=7, price="100.00", at="01"),
            change(2, item=9, price="500.00", at="01"),
            change(3, item=7, price="110.00", at="02"),
        ],
    )
    assert rows.filter("is_current").count() == 2
    assert rows.filter("menu_item_id = 7").count() == 2
    assert rows.filter("menu_item_id = 9").count() == 1


# --------------------------------------------------------------------------- staging


def test_staging_adds_one_closer_per_key(spark: SparkSession) -> None:
    """Each key contributes one 'close the open row' instruction, whatever its version count."""
    versions = versions_for(
        spark, [change(1, price="100.00", at="01"), change(2, price="110.00", at="02")]
    )
    staged = stage_for_merge(versions, SPEC)
    assert staged.filter(F.col("merge_key").isNotNull()).count() == 1
    assert staged.filter(F.col("merge_key").isNull()).count() == 2


def test_the_closer_is_the_earliest_version(spark: SparkSession) -> None:
    """It must carry the earliest valid_from, since that is when the open row stops being true."""
    versions = versions_for(
        spark, [change(1, price="100.00", at="01"), change(2, price="110.00", at="02")]
    )
    closer = stage_for_merge(versions, SPEC).filter(F.col("merge_key").isNotNull()).collect()[0]
    assert str(closer["valid_from"]) == "2026-10-01 12:00:00"


# --------------------------------------------------------------- the real MERGE, on disk


def apply(spark: SparkSession, target: str, rows: list[tuple[Any, ...]], batch: int = 0) -> None:
    merge_batch(spark.createDataFrame(rows, BRONZE_ROW), batch, spark, SPEC, target)


def read(spark: SparkSession, target: str) -> DataFrame:
    return spark.read.format("delta").load(target)


def test_first_load_inserts_without_duplicating(spark: SparkSession, tmp_path: Any) -> None:
    """The guard on the insert. Without `s.merge_key IS NULL` every key would double here."""
    target = str(tmp_path / "dim")
    apply(spark, target, [change(1, price="100.00", at="01")])
    assert read(spark, target).count() == 1


def test_a_later_batch_closes_the_open_row(spark: SparkSession, tmp_path: Any) -> None:
    target = str(tmp_path / "dim")
    apply(spark, target, [change(1, price="100.00", at="01")], batch=0)
    apply(spark, target, [change(2, price="110.00", at="02")], batch=1)

    rows = {r["lsn"]: r for r in read(spark, target).collect()}
    assert len(rows) == 2
    assert rows[1]["is_current"] is False
    assert str(rows[1]["valid_to"]) == "2026-10-02 12:00:00"
    assert rows[2]["is_current"] is True
    assert rows[2]["valid_to"] is None


def test_replaying_a_batch_adds_nothing(spark: SparkSession, tmp_path: Any) -> None:
    """Idempotency. foreachBatch is at-least-once, so this batch will arrive twice."""
    target = str(tmp_path / "dim")
    batch = [change(1, price="100.00", at="01"), change(2, price="110.00", at="02")]
    apply(spark, target, batch, batch=0)
    before = read(spark, target).collect()
    apply(spark, target, batch, batch=0)

    assert read(spark, target).count() == len(before)
    assert read(spark, target).filter("is_current").count() == 1


def test_a_restated_value_across_batches_opens_no_version(
    spark: SparkSession, tmp_path: Any
) -> None:
    """A later batch repeating the open row's tracked values must not open a second window."""
    target = str(tmp_path / "dim")
    apply(spark, target, [change(1, price="100.00", at="01")], batch=0)
    apply(spark, target, [change(2, price="100.00", at="02")], batch=1)

    assert read(spark, target).count() == 1


def test_validity_windows_never_overlap(spark: SparkSession, tmp_path: Any) -> None:
    """The correctness bar, stated in transform/silver/README.md.

    Built across several batches and several keys, because the single-batch path and the
    close-the-open-row path are different code and only their combination is the real thing.
    """
    target = str(tmp_path / "dim")
    apply(spark, target, [change(1, item=7, price="100.00", at="01")], batch=0)
    apply(
        spark,
        target,
        [
            change(2, item=7, price="110.00", at="02"),
            change(3, item=7, price="120.00", at="03"),
            change(4, item=9, price="500.00", at="02"),
        ],
        batch=1,
    )
    apply(spark, target, [change(5, item=7, price="130.00", at="05")], batch=2)

    dim = read(spark, target)

    # Exactly one open window per key.
    open_per_key = dim.filter("is_current").groupBy(SPEC.key).count().collect()
    assert all(r["count"] == 1 for r in open_per_key)

    # No key has two windows covering the same instant. A closed window ends exactly where
    # the next begins, so an overlap means valid_to ran past the following valid_from.
    ordered = dim.alias("a").join(dim.alias("b"), F.expr(f"a.{SPEC.key} = b.{SPEC.key}"))
    overlapping = ordered.filter(
        F.expr("a.valid_from < b.valid_from AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)")
    )
    assert overlapping.count() == 0, "two validity windows overlap for one key"

    # And the history is continuous: every closed window hands straight over to another.
    closed = dim.filter("valid_to IS NOT NULL").select(SPEC.key, "valid_to")
    starts = dim.select(F.col(SPEC.key).alias("k"), F.col("valid_from").alias("vf"))
    orphaned = closed.join(
        starts, (F.col(SPEC.key) == F.col("k")) & (F.col("valid_to") == F.col("vf")), "left_anti"
    )
    assert orphaned.count() == 0, "a closed window hands over to nothing"
