"""Silver `gps_trips_sessionized`, asserted against a real Spark.

Run with `make test-spark`.

`withWatermark` is a no-op on a static DataFrame, so `sessionize` can be driven in batch here
while being exactly the code the stream runs. What that does *not* cover is dropping late
pings, which only a streaming watermark does - so these tests assert the shape and the
arithmetic of a trip, and the watermark's effect is documented rather than faked.

PYTHON 3.8 - this runs inside the Spark image. See ADR-0009.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transform.silver.gps_trips_sessionized import merge_batch, sessionize

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("silver-gps-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


PING = StructType(
    [
        StructField("rider_id", LongType()),
        StructField("trip_id", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("speed_kmph", DoubleType()),
        StructField("event_ts", TimestampType()),
    ]
)

T0 = datetime(2026, 10, 6, 9, 0, 0)

# Bengaluru. Two points about 1.57 km apart, which is what the distance assertion leans on.
LAT_A, LON_A = 12.9716, 77.5946
LAT_B, LON_B = 12.9856, 77.5946


def ping(
    seconds: int,
    trip: str = "trip-1",
    rider: int = 7,
    lat: float = LAT_A,
    lon: float = LON_A,
    speed: float = 20.0,
) -> tuple[Any, ...]:
    return (rider, trip, lat, lon, speed, T0 + timedelta(seconds=seconds))


def trips_of(spark: SparkSession, rows: list[tuple[Any, ...]]) -> DataFrame:
    return sessionize(spark.createDataFrame(rows, PING))


# --------------------------------------------------------------------------- shape


def test_one_row_per_trip(spark: SparkSession) -> None:
    rows = [ping(0), ping(30), ping(60), ping(0, trip="trip-2"), ping(30, trip="trip-2")]
    assert trips_of(spark, rows).count() == 2


def test_the_same_trip_id_for_two_riders_stays_two_trips(spark: SparkSession) -> None:
    """The key is (rider, trip). Grouping on trip_id alone would fuse two riders' journeys."""
    rows = [ping(0, rider=7), ping(30, rider=7), ping(0, rider=9), ping(30, rider=9)]
    assert trips_of(spark, rows).count() == 2


def test_ping_count_and_bounds(spark: SparkSession) -> None:
    rows = [ping(0), ping(45), ping(90)]
    trip = trips_of(spark, rows).collect()[0]
    assert trip["ping_count"] == 3
    assert trip["started_ts"] == T0
    assert trip["ended_ts"] == T0 + timedelta(seconds=90)
    assert trip["duration_s"] == 90.0


def test_trip_date_comes_from_event_time(spark: SparkSession) -> None:
    """Partitioning on arrival would scatter one trip across two days."""
    trip = trips_of(spark, [ping(0), ping(30)]).collect()[0]
    assert str(trip["trip_date"]) == "2026-10-06"


# --------------------------------------------------------------------------- endpoints


def test_endpoints_follow_event_time_not_row_order(spark: SparkSession) -> None:
    """min_by/max_by, not first/last.

    The rows are handed over newest-first on purpose: a group has no inherent order, and
    first() would return whatever the shuffle put in front - which is a bug that passes
    whenever the input happens to arrive sorted.
    """
    rows = [
        ping(60, lat=LAT_B, lon=LON_B),
        ping(0, lat=LAT_A, lon=LON_A),
        ping(30, lat=12.98, lon=77.5946),
    ]
    trip = trips_of(spark, rows).collect()[0]
    assert trip["start_lat"] == pytest.approx(LAT_A)
    assert trip["end_lat"] == pytest.approx(LAT_B)


def test_straight_line_distance(spark: SparkSession) -> None:
    """0.014 degrees of latitude is about 1.56 km, anywhere on Earth."""
    rows = [ping(0, lat=LAT_A, lon=LON_A), ping(60, lat=LAT_B, lon=LON_B)]
    trip = trips_of(spark, rows).collect()[0]
    assert trip["straight_line_km"] == pytest.approx(1.557, abs=0.02)


def test_a_stationary_trip_covers_no_distance(spark: SparkSession) -> None:
    """A rider waiting at the restaurant. The haversine must not produce a NaN here."""
    rows = [ping(0), ping(30), ping(60)]
    trip = trips_of(spark, rows).collect()[0]
    assert trip["straight_line_km"] == pytest.approx(0.0, abs=1e-9)


def test_speed_is_averaged_and_peaked(spark: SparkSession) -> None:
    rows = [ping(0, speed=10.0), ping(30, speed=20.0), ping(60, speed=60.0)]
    trip = trips_of(spark, rows).collect()[0]
    assert trip["avg_speed_kmph"] == pytest.approx(30.0)
    assert trip["max_speed_kmph"] == pytest.approx(60.0)


# --------------------------------------------------------------- the merge, on real Delta


def test_a_refined_trip_overwrites_rather_than_duplicating(
    spark: SparkSession, tmp_path: Any
) -> None:
    """`update` mode re-states a trip as more pings land. That must converge, not accumulate.

    This is the idempotency property: the same trip seen twice is one row, carrying the later
    and more complete picture.
    """
    target = str(tmp_path / "trips")
    merge_batch(trips_of(spark, [ping(0), ping(30)]), 0, spark, target)
    merge_batch(trips_of(spark, [ping(0), ping(30), ping(90)]), 1, spark, target)

    rows = spark.read.format("delta").load(target).collect()
    assert len(rows) == 1
    assert rows[0]["ping_count"] == 3
    assert rows[0]["duration_s"] == 90.0


def test_replaying_a_batch_changes_nothing(spark: SparkSession, tmp_path: Any) -> None:
    target = str(tmp_path / "trips")
    trips = trips_of(spark, [ping(0), ping(30)])
    merge_batch(trips, 0, spark, target)
    before = spark.read.format("delta").load(target).collect()
    merge_batch(trips, 0, spark, target)

    assert spark.read.format("delta").load(target).collect() == before


def test_separate_trips_land_as_separate_rows(spark: SparkSession, tmp_path: Any) -> None:
    target = str(tmp_path / "trips")
    merge_batch(trips_of(spark, [ping(0), ping(30)]), 0, spark, target)
    merge_batch(
        trips_of(spark, [ping(0, trip="trip-2"), ping(30, trip="trip-2")]), 1, spark, target
    )

    assert spark.read.format("delta").load(target).count() == 2
