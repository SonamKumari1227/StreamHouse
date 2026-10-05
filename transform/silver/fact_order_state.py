"""Bronze CDC -> Silver `fact_order_state`.

    make silver-orders              # continuous
    make silver-orders ONCE=1       # drain what is available and stop

Grain: **one row per order, holding its latest known state.** Not one row per change - that
is Bronze's grain and Bronze keeps it. This table answers "what is order 4939 now?".

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

Four things here are load-bearing:

1. **Rank by LSN inside the micro-batch, before merging.** Delta refuses a MERGE whose source
   matches one target row more than once, and a batch routinely carries several changes to the
   same order - PLACED, ACCEPTED, PICKED_UP can all arrive together. Bronze's
   `dropDuplicates(pk, lsn)` does not help: those are three *different* LSNs and all three are
   genuine. Collapsing them to the highest LSN per order is what makes the MERGE well-defined,
   and taking anything other than the highest would write a state the order has already left.

2. **The MERGE is guarded by `s.lsn > t.lsn`.** Micro-batches are not guaranteed to arrive in
   LSN order after a restart, and `foreachBatch` is at-least-once. Without the guard a replayed
   older batch would overwrite newer state - the update would succeed and silently move the
   order backwards. With it, replay is a no-op and the job is idempotent.

3. **A delete carries its row in `before`, not `after`.** `after` is null for op='d', so the
   payload is coalesced across the two. The row is kept and flagged `is_deleted` rather than
   removed: Silver is the history-preserving layer, and a fact that vanishes cannot be
   reconciled against Bronze.

4. **Debezium timestamps are ISO-8601 strings, not epoch millis.** `time.precision.mode=connect`
   plus a `timestamptz` column gives `io.debezium.time.ZonedTimestamp`, which is a string like
   `2026-09-28T17:11:53.125601Z` - microsecond precision, explicit zone. They are cast, not
   divided. Money is `decimal(10,2)` on the wire and stays decimal here; a float would quietly
   lose paise.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

BRONZE = "s3a://bronze"
SILVER = "s3a://silver"
CHECKPOINTS = "s3a://checkpoints"

SOURCE = f"{BRONZE}/raw_orders_cdc"
TARGET = f"{SILVER}/fact_order_state"

# The shape of the Debezium `after`/`before` payload, which Bronze stored as a JSON string.
# Timestamps are strings here on purpose - they are ISO-8601 on the wire and are cast below.
# Money is decimal(10,2), matching the source DDL and the registered Avro scale.
MONEY = DecimalType(10, 2)

ORDER_PAYLOAD = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("restaurant_id", LongType()),
        StructField("rider_id", LongType()),
        StructField("status", StringType()),
        StructField("placed_ts", StringType()),
        StructField("accepted_ts", StringType()),
        StructField("picked_up_ts", StringType()),
        StructField("delivered_ts", StringType()),
        StructField("promised_ts", StringType()),
        StructField("cancel_reason", StringType()),
        StructField("subtotal_inr", MONEY),
        StructField("delivery_fee_inr", MONEY),
        StructField("discount_inr", MONEY),
        StructField("total_inr", MONEY),
        StructField("payment_id", LongType()),
        StructField("updated_at", StringType()),
    ]
)

TIMESTAMP_COLUMNS = (
    "placed_ts",
    "accepted_ts",
    "picked_up_ts",
    "delivered_ts",
    "promised_ts",
    "updated_at",
)

# Every column the MERGE writes. Listed once so insert and update cannot drift apart.
PAYLOAD_COLUMNS = (
    "customer_id",
    "restaurant_id",
    "rider_id",
    "status",
    "placed_ts",
    "accepted_ts",
    "picked_up_ts",
    "delivered_ts",
    "promised_ts",
    "cancel_reason",
    "subtotal_inr",
    "delivery_fee_inr",
    "discount_inr",
    "total_inr",
    "payment_id",
    "updated_at",
    "is_deleted",
    "lsn",
    "op",
)


def parse_changes(bronze: DataFrame) -> DataFrame:
    """Bronze change rows -> typed order state, one row per change.

    `after` holds the row for every op except a delete, which carries it in `before`. The
    coalesce is per-column rather than per-struct so a delete keeps its full before image.
    """
    parsed = bronze.withColumn(
        "payload",
        F.coalesce(
            F.from_json(F.col("after_json"), ORDER_PAYLOAD),
            F.from_json(F.col("before_json"), ORDER_PAYLOAD),
        ),
    )

    columns = [
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.restaurant_id").alias("restaurant_id"),
        F.col("payload.rider_id").alias("rider_id"),
        F.col("payload.status").alias("status"),
    ]
    # Cast, never arithmetic: these are ISO-8601 strings with an explicit zone.
    columns += [F.col(f"payload.{c}").cast("timestamp").alias(c) for c in TIMESTAMP_COLUMNS]
    columns += [
        F.col("payload.cancel_reason").alias("cancel_reason"),
        F.col("payload.subtotal_inr").alias("subtotal_inr"),
        F.col("payload.delivery_fee_inr").alias("delivery_fee_inr"),
        F.col("payload.discount_inr").alias("discount_inr"),
        F.col("payload.total_inr").alias("total_inr"),
        F.col("payload.payment_id").alias("payment_id"),
        (F.col("op") == "d").alias("is_deleted"),
        F.col("lsn"),
        F.col("op"),
    ]
    # A payload that parsed to nothing has no key to merge on and must not reach the MERGE.
    return parsed.select(*columns).filter(F.col("order_id").isNotNull())


def latest_per_order(changes: DataFrame) -> DataFrame:
    """Collapse a batch to one row per order: the change with the highest LSN.

    Without this the MERGE raises on a multiple-match, because a single batch commonly holds
    several changes to the same order. See point 1 in the module docstring.
    """
    newest = Window.partitionBy("order_id").orderBy(F.col("lsn").desc())
    return (
        changes.withColumn("_rank", F.row_number().over(newest))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def merge_batch(
    batch_df: DataFrame, batch_id: int, spark: SparkSession, target: str = TARGET
) -> None:
    """Idempotent upsert of one micro-batch into the Silver fact.

    `target` is a parameter so the merge - including the replay guard, which is the whole
    basis of the idempotency claim - can be exercised against a real Delta table in a test
    rather than only against the live bucket.
    """
    from delta.tables import DeltaTable

    latest = latest_per_order(parse_changes(batch_df))

    if DeltaTable.isDeltaTable(spark, target):
        assignments = {c: f"s.{c}" for c in PAYLOAD_COLUMNS}
        (
            DeltaTable.forPath(spark, target)
            .alias("t")
            .merge(latest.alias("s"), "t.order_id = s.order_id")
            # The guard. An older change for an order already at a higher LSN is a replay,
            # and replaying must not move the order backwards.
            .whenMatchedUpdate(condition="s.lsn > t.lsn", set=assignments)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        latest.write.format("delta").mode("append").save(target)

    print(f"batch {batch_id}: {latest.count()} order(s) merged into {target}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Silver fact_order_state from Bronze.")
    parser.add_argument("--once", action="store_true", help="process what is available, then stop")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("silver-fact-order-state").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Streaming off the Bronze Delta table, so Silver picks up where it left off rather than
    # rebuilding from the beginning on every run.
    stream = spark.readStream.format("delta").load(SOURCE)

    writer = (
        stream.writeStream.foreachBatch(lambda df, bid: merge_batch(df, bid, spark))
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_fact_order_state")
        .queryName("silver-fact-order-state")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    writer.start().awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
