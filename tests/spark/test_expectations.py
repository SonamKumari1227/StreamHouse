"""The Bronze -> Silver quality gate.

Run with `make test-spark`.

Two properties matter more than any individual rule:

1. **A failing row is quarantined, never dropped and never fatal.** Validation returns both
   halves, so the caller cannot accidentally discard the bad one.
2. **A row carries every rule it broke**, not just the first. One row violating four
   expectations tells you something a single violation does not.

And one subtlety worth a test of its own: a null predicate is a violation. SQL's three-valued
logic would otherwise pass a row that never satisfied the rule - which is precisely the row
the gate exists to catch.

PYTHON 3.8 - this runs inside the Spark image. See ADR-0009.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from quality.expectations import (
    Expectation,
    fact_order_state_suite,
    gps_trips_suite,
    scd2_suite,
    validate,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("quality-gate-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


MONEY = DecimalType(10, 2)
T0 = datetime(2026, 10, 6, 12, 0, 0)

ORDER = StructType(
    [
        StructField("order_id", LongType()),
        StructField("rider_id", LongType()),
        StructField("status", StringType()),
        StructField("placed_ts", TimestampType()),
        StructField("promised_ts", TimestampType()),
        StructField("delivered_ts", TimestampType()),
        StructField("subtotal_inr", MONEY),
        StructField("delivery_fee_inr", MONEY),
        StructField("discount_inr", MONEY),
        StructField("total_inr", MONEY),
    ]
)


def order(
    order_id: int = 1,
    rider: int | None = 7,
    status: str = "DELIVERED",
    placed: datetime | None = T0,
    promised: datetime | None = None,
    delivered: datetime | None = None,
    subtotal: str = "100.00",
    fee: str = "20.00",
    discount: str = "5.00",
    total: str | None = None,
) -> tuple[Any, ...]:
    computed = Decimal(subtotal) + Decimal(fee) - Decimal(discount)
    return (
        order_id,
        rider,
        status,
        placed,
        promised if promised is not None else (placed + timedelta(minutes=30) if placed else None),
        delivered
        if delivered is not None
        else (placed + timedelta(minutes=25) if placed else None),
        Decimal(subtotal),
        Decimal(fee),
        Decimal(discount),
        Decimal(total) if total is not None else computed,
    )


def failures(spark: SparkSession, rows: list[tuple[Any, ...]]) -> list[list[str]]:
    df = spark.createDataFrame(rows, ORDER)
    result = validate(df, fact_order_state_suite())
    return [list(r["violations"]) for r in result.failed.collect()]


# --------------------------------------------------------------------------- the two halves


def test_a_clean_table_quarantines_nothing(spark: SparkSession) -> None:
    df = spark.createDataFrame([order(1), order(2)], ORDER)
    result = validate(df, fact_order_state_suite())
    assert result.passed.count() == 2
    assert result.failed.count() == 0


def test_a_bad_row_is_separated_not_dropped(spark: SparkSession) -> None:
    """The good rows still go through. One bad row must not stop the batch."""
    df = spark.createDataFrame([order(1), order(2, total="999.99"), order(3)], ORDER)
    result = validate(df, fact_order_state_suite())
    assert result.passed.count() == 2
    assert result.failed.count() == 1
    assert result.passed.count() + result.failed.count() == df.count()


def test_the_violation_is_named(spark: SparkSession) -> None:
    assert failures(spark, [order(total="999.99")]) == [["total_reconciles"]]


def test_a_row_carries_every_rule_it_broke(spark: SparkSession) -> None:
    """Three problems in one row should read as three, not as one.

    The status stays DELIVERED on purpose. `rider_assigned_once_picked_up` is conditional on
    exactly that status, so an invalid status would excuse the missing rider instead of
    compounding with it - and this test is about rules accumulating.
    """
    broken = order(
        order_id=1,
        rider=None,
        status="DELIVERED",
        delivered=T0 - timedelta(hours=1),
        total="999.99",
    )
    violations = failures(spark, [broken])[0]
    assert set(violations) == {
        "total_reconciles",
        "delivered_after_placed",
        "rider_assigned_once_picked_up",
    }


def test_an_unknown_status_is_caught(spark: SparkSession) -> None:
    assert failures(spark, [order(status="TELEPORTED")]) == [["status_is_known"]]


# --------------------------------------------------------------------------- null handling


def test_a_null_makes_the_predicate_fail_not_pass(spark: SparkSession) -> None:
    """The three-valued-logic trap.

    `NULL >= placed_ts` is NULL, which is not TRUE - and a filter keeping `~predicate` would
    drop this row from both halves, so it would vanish entirely rather than be quarantined.
    """
    df = spark.createDataFrame([order(placed=None)], ORDER)
    result = validate(df, fact_order_state_suite())
    assert result.passed.count() == 0
    assert result.failed.count() == 1
    assert "placed_ts_present" in result.failed.collect()[0]["violations"]


def test_an_unassigned_rider_is_fine_before_pickup(spark: SparkSession) -> None:
    """A PLACED order legitimately has no rider. The rule is conditional, not blanket."""
    df = spark.createDataFrame([order(rider=None, status="PLACED")], ORDER)
    assert validate(df, fact_order_state_suite()).failed.count() == 0


def test_an_undelivered_order_is_fine(spark: SparkSession) -> None:
    df = spark.createDataFrame([order(status="ACCEPTED", rider=7, delivered=None)], ORDER)
    assert validate(df, fact_order_state_suite()).failed.count() == 0


# --------------------------------------------------------------------------- SCD2 suite

DIM = StructType(
    [
        StructField("menu_item_id", LongType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
    ]
)


def test_scd2_accepts_a_well_formed_history(spark: SparkSession) -> None:
    rows = [
        (7, T0, T0 + timedelta(days=1), False),
        (7, T0 + timedelta(days=1), None, True),
    ]
    df = spark.createDataFrame(rows, DIM)
    assert validate(df, scd2_suite("menu_item_id")).failed.count() == 0


def test_scd2_rejects_a_backwards_window(spark: SparkSession) -> None:
    rows = [(7, T0, T0 - timedelta(days=1), False)]
    df = spark.createDataFrame(rows, DIM)
    result = validate(df, scd2_suite("menu_item_id"))
    assert "window_is_forward" in result.failed.collect()[0]["violations"]


def test_scd2_rejects_a_closed_row_still_marked_current(spark: SparkSession) -> None:
    """Two rows claiming "now" for one key is the failure the correctness bar forbids."""
    rows = [(7, T0, T0 + timedelta(days=1), True)]
    df = spark.createDataFrame(rows, DIM)
    violations = validate(df, scd2_suite("menu_item_id")).failed.collect()[0]["violations"]
    assert "current_is_open_ended" in violations
    assert "closed_is_not_current" in violations


# --------------------------------------------------------------------------- trips suite

TRIP = StructType(
    [
        StructField("rider_id", LongType()),
        StructField("trip_id", StringType()),
        StructField("started_ts", TimestampType()),
        StructField("ended_ts", TimestampType()),
        StructField("duration_s", DoubleType()),
        StructField("ping_count", LongType()),
        StructField("avg_speed_kmph", DoubleType()),
        StructField("max_speed_kmph", DoubleType()),
        StructField("straight_line_km", DoubleType()),
    ]
)


def test_a_sane_trip_passes(spark: SparkSession) -> None:
    rows = [(7, "t1", T0, T0 + timedelta(minutes=10), 600.0, 120, 22.0, 45.0, 3.4)]
    df = spark.createDataFrame(rows, TRIP)
    assert validate(df, gps_trips_suite()).failed.count() == 0


def test_an_implausible_distance_is_caught(spark: SparkSession) -> None:
    """A delivery rider has not crossed a country; that is a bad coordinate."""
    rows = [(7, "t1", T0, T0 + timedelta(minutes=10), 600.0, 120, 22.0, 45.0, 4000.0)]
    df = spark.createDataFrame(rows, TRIP)
    violations = validate(df, gps_trips_suite()).failed.collect()[0]["violations"]
    assert "distance_is_plausible" in violations


def test_an_average_above_the_maximum_is_caught(spark: SparkSession) -> None:
    """Arithmetically impossible, so it means the aggregation is wrong."""
    rows = [(7, "t1", T0, T0 + timedelta(minutes=10), 600.0, 120, 90.0, 45.0, 3.4)]
    df = spark.createDataFrame(rows, TRIP)
    violations = validate(df, gps_trips_suite()).failed.collect()[0]["violations"]
    assert "avg_speed_within_max" in violations


# --------------------------------------------------------------------------- the primitive


def test_expectation_violated_treats_null_as_broken(spark: SparkSession) -> None:
    """The building block the whole gate rests on, asserted directly."""
    df = spark.createDataFrame([(1, None), (2, 5)], "id int, value int")
    rule = Expectation("value_is_positive", F.col("value") > 0)
    marked = df.withColumn("broken", rule.violated())
    broken = {r["id"]: r["broken"] for r in marked.collect()}
    assert broken[1] is True
    assert broken[2] is False
