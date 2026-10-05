"""The Bronze transforms, asserted against a real Spark.

Run with `make test-spark`; these do not run on the host, which has no JVM.

What is worth testing here is narrow and deliberate. The streaming plumbing - Kafka source,
checkpoints, foreachBatch - is Spark's code, and asserting on it would be testing the
framework. What is ours, and what Phase 3 inherits, is three decisions:

1. **The dedup key.** `(source_table, pk, lsn)` must collapse a redelivered change and must
   NOT collapse two genuine changes to the same row. Phase 1 established that a duplicate is
   the same WAL record twice, not the same row written twice (ADR-0008); if dedup ever
   widened to `(table, pk)` it would silently eat history, and SCD2 would be built on a lie.

2. **Where the primary key comes from.** An insert or update carries it in `after`, a delete
   carries it only in `before`. Reading `after` alone would drop the key on every delete and
   land a null pk in Bronze.

3. **Event time is the device clock.** GPS `event_ts` is when the ping was emitted, never when
   it arrived. Phase 3 watermarks on it and chaos scenario 1 delivers pings long after it, so
   a transform that quietly substituted arrival time would make lateness undetectable.

PYTHON 3.8 - this runs inside the Spark image. See ADR-0009.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingestion.bronze_cdc_stream import to_bronze_rows
from ingestion.bronze_gps_stream import to_bronze_pings

pytestmark = pytest.mark.spark


# --------------------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    """A local session. spark-defaults.conf from the image applies, Delta extensions and all.

    local[2] rather than local[1]: a single partition would hide any transform that only
    happens to work because everything landed on one worker.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("bronze-transform-tests")
        # The default 200 turns every tiny test shuffle into 200 empty tasks.
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


ORDER_FIELDS = StructType(
    [
        StructField("order_id", LongType()),
        StructField("status", StringType()),
    ]
)

ENVELOPE = StructType(
    [
        StructField("before", ORDER_FIELDS),
        StructField("after", ORDER_FIELDS),
        StructField("source", StructType([StructField("lsn", LongType())])),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ]
)

DECODED = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("value", BinaryType()),
        StructField("envelope", ENVELOPE),
    ]
)

KAFKA_TS = datetime(2026, 9, 28, 12, 0, 0)


def change(
    op: str,
    lsn: int,
    offset: int = 0,
    before: tuple[int, str] | None = None,
    after: tuple[int, str] | None = (4939, "PLACED"),
) -> tuple[Any, ...]:
    """One decoded CDC row. Defaults describe an insert of order 4939."""
    return (
        "cdc.public.orders",
        0,
        offset,
        KAFKA_TS,
        b"\x00\x00\x00\x00\x01payload",
        (before, after, (lsn,), op, 1759060000000),
    )


# --------------------------------------------------------------------------- CDC: the pk


def test_insert_takes_the_key_from_after(spark: SparkSession) -> None:
    df = spark.createDataFrame([change("c", lsn=100)], DECODED)
    row = to_bronze_rows(df, "orders").collect()[0]
    assert row["pk"] == 4939
    assert row["op"] == "c"
    assert row["lsn"] == 100


def test_delete_takes_the_key_from_before(spark: SparkSession) -> None:
    """A delete carries no `after`. Reading it alone would land a null pk in Bronze."""
    df = spark.createDataFrame(
        [change("d", lsn=200, before=(4939, "DELIVERED"), after=None)], DECODED
    )
    row = to_bronze_rows(df, "orders").collect()[0]
    assert row["pk"] == 4939
    assert row["op"] == "d"


def test_source_table_is_stamped_on_every_row(spark: SparkSession) -> None:
    """Bronze is one table per source table, but the column is what makes the key unique."""
    df = spark.createDataFrame([change("c", lsn=1)], DECODED)
    assert to_bronze_rows(df, "orders").collect()[0]["source_table"] == "orders"


def test_before_and_after_are_kept_as_json(spark: SparkSession) -> None:
    """The full payload survives into Bronze; Silver reshapes it, Bronze never discards it."""
    df = spark.createDataFrame(
        [change("u", lsn=300, before=(4939, "PLACED"), after=(4939, "ACCEPTED"))], DECODED
    )
    row = to_bronze_rows(df, "orders").collect()[0]
    assert '"status":"PLACED"' in row["before_json"]
    assert '"status":"ACCEPTED"' in row["after_json"]


def test_kafka_coordinates_survive(spark: SparkSession) -> None:
    """Without these, a row in Bronze cannot be traced back to the message that produced it."""
    df = spark.createDataFrame([change("c", lsn=1, offset=77)], DECODED)
    row = to_bronze_rows(df, "orders").collect()[0]
    assert row["topic"] == "cdc.public.orders"
    assert row["partition"] == 0
    assert row["offset"] == 77
    assert row["kafka_ts"] == KAFKA_TS


# --------------------------------------------------------------------------- CDC: dedup


def test_redelivered_change_collapses_to_one_row(spark: SparkSession) -> None:
    """The same WAL record twice. This is the replay foreachBatch can hand us."""
    rows = [change("u", lsn=500, offset=1), change("u", lsn=500, offset=1)]
    df = spark.createDataFrame(rows, DECODED)
    deduped = to_bronze_rows(df, "orders").dropDuplicates(["source_table", "pk", "lsn"])
    assert deduped.count() == 1


def test_two_real_changes_to_one_row_are_both_kept(spark: SparkSession) -> None:
    """The failure that matters.

    Two updates to order 4939 at different LSNs are genuine, ordered history - PLACED then
    ACCEPTED. A dedup keyed on (table, pk) would keep one and silently destroy the other,
    and Phase 3's SCD2 would build validity windows over a history with holes in it.
    """
    rows = [
        change("u", lsn=500, offset=1, before=(4939, "PLACED"), after=(4939, "ACCEPTED")),
        change("u", lsn=600, offset=2, before=(4939, "ACCEPTED"), after=(4939, "PICKED_UP")),
    ]
    df = spark.createDataFrame(rows, DECODED)
    deduped = to_bronze_rows(df, "orders").dropDuplicates(["source_table", "pk", "lsn"])
    assert deduped.count() == 2
    assert sorted(r["lsn"] for r in deduped.collect()) == [500, 600]


def test_same_lsn_on_different_rows_is_not_a_duplicate(spark: SparkSession) -> None:
    """One transaction touching two rows can report the same LSN. The pk separates them."""
    rows = [
        change("c", lsn=700, offset=1, after=(1, "PLACED")),
        change("c", lsn=700, offset=2, after=(2, "PLACED")),
    ]
    df = spark.createDataFrame(rows, DECODED)
    deduped = to_bronze_rows(df, "orders").dropDuplicates(["source_table", "pk", "lsn"])
    assert deduped.count() == 2


# --------------------------------------------------------------------------- GPS

PING = StructType(
    [
        StructField("rider_id", LongType()),
        StructField("trip_id", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("speed_kmph", DoubleType()),
        StructField("heading_deg", DoubleType()),
        StructField("accuracy_m", DoubleType()),
        StructField("event_ts", LongType()),
    ]
)

DECODED_PING = StructType(
    [
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("value", BinaryType()),
        StructField("ping", PING),
    ]
)

# Device clock: 2026-09-28 06:30:00 UTC. Deliberately on a different date from arrival.
EVENT_TS_MS = 1790577000000
ARRIVED_LATE = datetime(2026, 9, 29, 9, 0, 0)


def ping(
    offset: int = 0,
    event_ts_ms: int = EVENT_TS_MS,
    arrived: datetime = ARRIVED_LATE,
    body: bool = True,
) -> tuple[Any, ...]:
    payload = (7, "trip-1", 12.97, 77.59, 24.5, 90.0, 5.0, event_ts_ms) if body else None
    return ("gps.pings", 0, offset, arrived, b"\x00\x00\x00\x00\x02payload", payload)


def test_event_ts_is_the_device_clock_not_arrival(spark: SparkSession) -> None:
    """A ping delivered a day late still carries the moment it was emitted.

    This is the whole basis of Phase 3's watermark and of chaos scenario 1. If this ever
    reads as the arrival time, lateness becomes undetectable and the scenario passes
    vacuously.
    """
    df = spark.createDataFrame([ping()], DECODED_PING)
    row = to_bronze_pings(df).collect()[0]
    assert row["event_ts"] == datetime.utcfromtimestamp(EVENT_TS_MS / 1000)
    assert row["kafka_ts"] == ARRIVED_LATE
    assert row["event_ts"] != row["kafka_ts"]


def test_event_date_partitions_on_event_time(spark: SparkSession) -> None:
    """Partitioning on arrival would scatter one device-day across two partitions."""
    df = spark.createDataFrame([ping()], DECODED_PING)
    row = to_bronze_pings(df).collect()[0]
    assert str(row["event_date"]) == "2026-09-28"
    assert ARRIVED_LATE.date().isoformat() == "2026-09-29"


def test_undecodable_ping_is_not_carried_into_bronze(spark: SparkSession) -> None:
    """A null payload is a decode failure; it belongs in quarantine, not in the good rows."""
    df = spark.createDataFrame([ping(offset=1), ping(offset=2, body=False)], DECODED_PING)
    assert to_bronze_pings(df).count() == 1


def test_replayed_pings_collapse_on_kafka_coordinates(spark: SparkSession) -> None:
    """GPS has no primary key, so (topic, partition, offset) is the identity of a ping."""
    df = spark.createDataFrame([ping(offset=5), ping(offset=5)], DECODED_PING)
    deduped = to_bronze_pings(df).dropDuplicates(["topic", "partition", "offset"])
    assert deduped.count() == 1
