"""Silver `fact_order_state`, asserted against a real Spark.

Run with `make test-spark`.

The three things worth pinning here are the three that are easy to get wrong and silent when
wrong - a corrupted fact looks exactly like a correct one until someone reconciles it:

1. **Ranking inside the batch.** Several changes to one order arrive together. Collapsing to
   anything but the highest LSN writes a state the order has already left, and not collapsing
   at all makes Delta refuse the MERGE outright.
2. **Typing.** Debezium sends ISO-8601 strings and decimal money. A timestamp read as a
   number, or money read as a double, is wrong in a way that still produces plausible output.
3. **Deletes.** The row lives in `before`, and `after` is null.

PYTHON 3.8 - this runs inside the Spark image. See ADR-0009.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transform.silver.fact_order_state import latest_per_order, merge_batch, parse_changes

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("silver-fact-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


BRONZE_ROW = StructType(
    [
        StructField("source_table", StringType()),
        StructField("pk", LongType()),
        StructField("lsn", LongType()),
        StructField("op", StringType()),
        StructField("before_json", StringType()),
        StructField("after_json", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("kafka_ts", TimestampType()),
    ]
)

KAFKA_TS = datetime(2026, 9, 28, 17, 0, 0)


def payload(order_id: int = 4939, status: str = "PLACED", **overrides: Any) -> str:
    """A Debezium `after` payload, shaped exactly as Bronze stored it.

    Note what is NOT here: to_json omits null fields, so a PLACED order genuinely has no
    rider_id or accepted_ts key at all. Parsing must cope with absence, not just null.
    """
    body = {
        "order_id": order_id,
        "customer_id": 189,
        "restaurant_id": 70,
        "status": status,
        "placed_ts": "2026-09-28T17:11:53.125601Z",
        "promised_ts": "2026-09-28T17:13:46.893692Z",
        "subtotal_inr": 1965.72,
        "delivery_fee_inr": 35.34,
        "discount_inr": 0.00,
        "total_inr": 2001.06,
        "payment_id": 16427,
        "updated_at": "2026-09-28T17:11:53.125601Z",
    }
    body.update(overrides)
    return json.dumps(body)


def bronze(
    lsn: int,
    op: str = "u",
    order_id: int = 4939,
    status: str = "PLACED",
    after: str | None = None,
    before: str | None = None,
    **overrides: Any,
) -> tuple[Any, ...]:
    if after is None and op != "d":
        after = payload(order_id, status, **overrides)
    return ("orders", order_id, lsn, op, before, after, 0, lsn, KAFKA_TS)


# --------------------------------------------------------------------------- typing


def test_timestamps_are_parsed_not_divided(spark: SparkSession) -> None:
    """ZonedTimestamp is an ISO-8601 string with microseconds, not epoch millis."""
    df = spark.createDataFrame([bronze(lsn=1, op="c")], BRONZE_ROW)
    row = parse_changes(df).collect()[0]
    assert row["placed_ts"] == datetime(2026, 9, 28, 17, 11, 53, 125601)


def test_money_stays_decimal(spark: SparkSession) -> None:
    """A double would lose paise silently, and money errors are not self-announcing."""
    df = spark.createDataFrame([bronze(lsn=1, op="c")], BRONZE_ROW)
    row = parse_changes(df).collect()[0]
    assert row["total_inr"] == Decimal("2001.06")
    assert isinstance(row["total_inr"], Decimal)


def test_absent_keys_become_null(spark: SparkSession) -> None:
    """A PLACED order has no rider_id key at all; that must read as null, not fail."""
    df = spark.createDataFrame([bronze(lsn=1, op="c")], BRONZE_ROW)
    row = parse_changes(df).collect()[0]
    assert row["rider_id"] is None
    assert row["delivered_ts"] is None


def test_unparseable_payload_is_dropped(spark: SparkSession) -> None:
    """No key means nothing to merge on. Such a row must never reach the MERGE."""
    rows = [bronze(lsn=1, op="c"), ("orders", None, 2, "u", None, "{not json", 0, 2, KAFKA_TS)]
    df = spark.createDataFrame(rows, BRONZE_ROW)
    assert parse_changes(df).count() == 1


# --------------------------------------------------------------------------- deletes


def test_delete_reads_its_row_from_before(spark: SparkSession) -> None:
    """`after` is null on a delete; the before image is the only copy of the row."""
    df = spark.createDataFrame(
        [bronze(lsn=9, op="d", after=None, before=payload(4939, "DELIVERED"))], BRONZE_ROW
    )
    row = parse_changes(df).collect()[0]
    assert row["order_id"] == 4939
    assert row["status"] == "DELIVERED"
    assert row["is_deleted"] is True


def test_a_normal_change_is_not_flagged_deleted(spark: SparkSession) -> None:
    df = spark.createDataFrame([bronze(lsn=1, op="u")], BRONZE_ROW)
    assert parse_changes(df).collect()[0]["is_deleted"] is False


# --------------------------------------------------------------------------- ranking


def test_batch_collapses_to_the_highest_lsn(spark: SparkSession) -> None:
    """The trap the Silver README names.

    PLACED, ACCEPTED and PICKED_UP for one order arrive in a single batch. All three are
    genuine changes at different LSNs, so Bronze's (pk, lsn) dedup keeps all three - and
    Delta would refuse a MERGE that matched one target row three times. The batch must
    collapse to the newest state, not merely to *a* state.
    """
    rows = [
        bronze(lsn=100, status="PLACED"),
        bronze(lsn=300, status="PICKED_UP"),
        bronze(lsn=200, status="ACCEPTED"),
    ]
    df = spark.createDataFrame(rows, BRONZE_ROW)
    latest = latest_per_order(parse_changes(df))
    assert latest.count() == 1
    row = latest.collect()[0]
    assert row["status"] == "PICKED_UP"
    assert row["lsn"] == 300


def test_ranking_is_per_order_not_global(spark: SparkSession) -> None:
    """A global 'take the max LSN' would keep one order and discard every other."""
    rows = [
        bronze(lsn=100, order_id=1, status="PLACED"),
        bronze(lsn=900, order_id=2, status="DELIVERED"),
        bronze(lsn=150, order_id=1, status="ACCEPTED"),
    ]
    df = spark.createDataFrame(rows, BRONZE_ROW)
    latest = latest_per_order(parse_changes(df))
    assert latest.count() == 2
    by_order = {r["order_id"]: r for r in latest.collect()}
    assert by_order[1]["status"] == "ACCEPTED"
    assert by_order[2]["status"] == "DELIVERED"


def test_a_delete_can_be_the_winning_change(spark: SparkSession) -> None:
    """A delete is the newest state when it has the highest LSN, flag and all."""
    rows = [
        bronze(lsn=100, status="PLACED"),
        bronze(lsn=400, op="d", after=None, before=payload(4939, "CANCELLED")),
    ]
    df = spark.createDataFrame(rows, BRONZE_ROW)
    row = latest_per_order(parse_changes(df)).collect()[0]
    assert row["is_deleted"] is True
    assert row["lsn"] == 400


# --------------------------------------------------------------- the replay guard (MERGE)
#
# These drive the real Delta MERGE against a table on local disk. They are the only tests
# that prove the idempotency claim, because the guard lives in the MERGE condition itself
# and not in any pure function.


def one_row(spark: SparkSession, path: str) -> dict[str, Any]:
    rows = spark.read.format("delta").load(path).collect()
    assert len(rows) == 1, f"expected exactly one order, got {len(rows)}"
    state: dict[str, Any] = rows[0].asDict()
    return state


def batch_of(spark: SparkSession, lsn: int, status: str) -> Any:
    return spark.createDataFrame([bronze(lsn=lsn, status=status)], BRONZE_ROW)


def test_first_batch_inserts(spark: SparkSession, tmp_path: Any) -> None:
    target = str(tmp_path / "fact")
    merge_batch(batch_of(spark, 100, "PLACED"), 0, spark, target)
    assert one_row(spark, target)["status"] == "PLACED"


def test_newer_batch_advances_the_order(spark: SparkSession, tmp_path: Any) -> None:
    target = str(tmp_path / "fact")
    merge_batch(batch_of(spark, 100, "PLACED"), 0, spark, target)
    merge_batch(batch_of(spark, 200, "ACCEPTED"), 1, spark, target)

    state = one_row(spark, target)
    assert state["status"] == "ACCEPTED"
    assert state["lsn"] == 200


def test_replaying_an_older_batch_does_not_move_the_order_backwards(
    spark: SparkSession, tmp_path: Any
) -> None:
    """The failure the guard exists to prevent.

    foreachBatch is at-least-once, and batches are not guaranteed to arrive in LSN order
    after a restart. Without `s.lsn > t.lsn` this replay would succeed and a DELIVERED order
    would silently revert to PLACED - a corruption that reconciles against nothing and
    announces itself nowhere.
    """
    target = str(tmp_path / "fact")
    merge_batch(batch_of(spark, 300, "DELIVERED"), 0, spark, target)
    merge_batch(batch_of(spark, 100, "PLACED"), 1, spark, target)

    state = one_row(spark, target)
    assert state["status"] == "DELIVERED", "an older replay overwrote newer state"
    assert state["lsn"] == 300


def test_merging_the_same_batch_twice_changes_nothing(spark: SparkSession, tmp_path: Any) -> None:
    """Exactly-once in effect: the same batch handed over twice leaves one unchanged row."""
    target = str(tmp_path / "fact")
    batch = batch_of(spark, 200, "ACCEPTED")
    merge_batch(batch, 0, spark, target)
    before = one_row(spark, target)
    merge_batch(batch, 0, spark, target)

    assert one_row(spark, target) == before
