"""Bronze CDC -> Silver SCD2 dimensions.

    make silver-dim DIM=restaurants ONCE=1
    make silver-dim DIM=riders
    make silver-dim DIM=menu_items

Grain: **one row per (business key, validity window).** `valid_from` / `valid_to` /
`is_current`, so a query can ask what a restaurant's commission was on a given day, or what a
menu item cost when an order was placed - which is exactly what Gold's fact tables need and
what a latest-state-only dimension cannot answer.

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

One module, three dimensions. They differ only in key, columns and target path, and three
near-identical files would be three places for the window logic to drift apart.

The hard parts, in the order they bite:

1. **A MERGE cannot close one row and open another in the same pass.** One source row may
   take one action against one target row. The way out is the standard staged union: each new
   version appears twice in the source - once with `merge_key = <key>` to close the row that
   is currently open, once with `merge_key = NULL` so it can never match and is inserted. The
   insert is guarded by `s.merge_key IS NULL`, or the closing copy would also be inserted
   whenever no open row existed and every key would be duplicated on first load.

2. **A batch can carry several changes to one key, and all of them are history.** The fact
   table collapses to the newest; a dimension must not - that is the price history. Versions
   are therefore chained inside the batch with `lead()`: each one's `valid_to` is the next
   one's `valid_from`, and only the last stays `is_current`.

3. **Most CDC updates change nothing we track.** `updated_at` moves on every write, so
   comparing whole rows would open a new version for every touch and the dimension would grow
   without saying anything. Versions are opened on a hash of the tracked columns only, and the
   hash is stored so the comparison against the currently open row is exact rather than a
   column-by-column null-sensitive mess.

4. **Replay must be a no-op.** Changes at or below the open row's LSN are dropped before
   anything is staged, so redelivering a batch adds no versions.

`valid_from` is the source commit time (`updated_at`), never processing time. A window that
says when Spark noticed a change is not a window anyone can join a fact to.
"""

from __future__ import annotations

import argparse
import sys
from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
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


class DimensionSpec(NamedTuple):
    """Everything that differs between the three dimensions."""

    source_table: str
    target_name: str
    key: str
    payload: StructType
    tracked: tuple[str, ...]

    @property
    def source(self) -> str:
        return f"{BRONZE}/raw_{self.source_table}_cdc"

    @property
    def target(self) -> str:
        return f"{SILVER}/{self.target_name}"

    @property
    def checkpoint(self) -> str:
        return f"{CHECKPOINTS}/silver_{self.target_name}"


# Decimal scales mirror the source DDL and the registered Avro scale exactly. Reading money
# or a rating as a double would be wrong in a way that still looks plausible.
RESTAURANTS = DimensionSpec(
    source_table="restaurants",
    target_name="dim_restaurant_scd2",
    key="restaurant_id",
    payload=StructType(
        [
            StructField("restaurant_id", LongType()),
            StructField("name", StringType()),
            StructField("city", StringType()),
            StructField("lat", DecimalType(9, 6)),
            StructField("lon", DecimalType(9, 6)),
            StructField("cuisine", StringType()),
            StructField("rating", DecimalType(2, 1)),
            StructField("commission_pct", DecimalType(5, 2)),
            StructField("is_open", BooleanType()),
            StructField("updated_at", StringType()),
        ]
    ),
    tracked=("name", "city", "lat", "lon", "cuisine", "rating", "commission_pct", "is_open"),
)

RIDERS = DimensionSpec(
    source_table="riders",
    target_name="dim_rider_scd2",
    key="rider_id",
    payload=StructType(
        [
            StructField("rider_id", LongType()),
            StructField("name", StringType()),
            StructField("city", StringType()),
            StructField("vehicle_type", StringType()),
            StructField("tier", StringType()),
            StructField("shift_start", StringType()),
            StructField("is_online", BooleanType()),
            StructField("updated_at", StringType()),
        ]
    ),
    tracked=("name", "city", "vehicle_type", "tier", "shift_start", "is_online"),
)

MENU_ITEMS = DimensionSpec(
    source_table="menu_items",
    target_name="dim_menu_item_scd2",
    key="menu_item_id",
    payload=StructType(
        [
            StructField("menu_item_id", LongType()),
            StructField("restaurant_id", LongType()),
            StructField("name", StringType()),
            StructField("price_inr", DecimalType(10, 2)),
            StructField("is_available", BooleanType()),
            StructField("updated_at", StringType()),
        ]
    ),
    tracked=("restaurant_id", "name", "price_inr", "is_available"),
)

DIMENSIONS = {
    "restaurants": RESTAURANTS,
    "riders": RIDERS,
    "menu_items": MENU_ITEMS,
}

# Timestamp-valued columns are ISO-8601 strings on the wire (io.debezium.time.ZonedTimestamp)
# and are cast, never divided. See CLAUDE.md, "What the CDC payload actually looks like".
TIMESTAMP_COLUMNS = ("updated_at", "shift_start")


def parse_changes(bronze: DataFrame, spec: DimensionSpec) -> DataFrame:
    """Bronze change rows -> typed dimension changes, one row per change.

    A delete carries its row in `before`; everything else carries it in `after`.
    """
    parsed = bronze.withColumn(
        "payload",
        F.coalesce(
            F.from_json(F.col("after_json"), spec.payload),
            F.from_json(F.col("before_json"), spec.payload),
        ),
    )

    columns = [F.col(f"payload.{spec.key}").alias(spec.key)]
    for name in spec.tracked:
        source = F.col(f"payload.{name}")
        columns.append(
            source.cast("timestamp").alias(name)
            if name in TIMESTAMP_COLUMNS
            else source.alias(name)
        )
    columns += [
        # The source commit time, which is what the validity window must be expressed in.
        F.col("payload.updated_at").cast("timestamp").alias("valid_from"),
        (F.col("op") == "d").alias("is_deleted"),
        F.col("lsn"),
    ]

    changes = parsed.select(*columns).filter(F.col(spec.key).isNotNull())
    return changes.withColumn("attr_hash", attribute_hash(spec))


def attribute_hash(spec: DimensionSpec) -> F.Column:
    """A stable fingerprint of the tracked columns, plus the deleted flag.

    concat_ws skips nulls, which would make ('a', null, 'b') and ('a', 'b', null) collide, so
    nulls get an explicit sentinel first. The sentinel is a control character no business
    string will contain.
    """
    parts = []
    for name in (*spec.tracked, "is_deleted"):
        parts.append(F.coalesce(F.col(name).cast("string"), F.lit("\u0000")))
    return F.sha2(F.concat_ws("\u0001", *parts), 256)


def new_versions(changes: DataFrame, spec: DimensionSpec, current: DataFrame | None) -> DataFrame:
    """Changes -> the versions this batch should add, already chained.

    `current` is the currently-open row per key (key, lsn, attr_hash), or None on first load.
    It does two jobs: it drops replayed changes, and it stops a new version being opened when
    the batch merely restates what the open row already says.
    """
    if current is not None:
        open_row = current.select(
            F.col(spec.key).alias("_k"),
            F.col("lsn").alias("_open_lsn"),
            F.col("attr_hash").alias("_open_hash"),
        )
        changes = changes.join(open_row, changes[spec.key] == F.col("_k"), "left")
        # Replay: at or below the open row's LSN this change is already represented.
        changes = changes.filter(F.col("_open_lsn").isNull() | (F.col("lsn") > F.col("_open_lsn")))
    else:
        changes = changes.withColumn("_open_hash", F.lit(None).cast("string"))

    ordered = Window.partitionBy(spec.key).orderBy("lsn")

    # The hash of whatever was in effect immediately before this change: the previous change
    # in the batch, or - for the first change - the row currently open in the target.
    previous = F.coalesce(F.lag("attr_hash").over(ordered), F.col("_open_hash"))
    changed = changes.withColumn("_previous_hash", previous).filter(
        F.col("_previous_hash").isNull() | (F.col("attr_hash") != F.col("_previous_hash"))
    )

    # Chain what survives: each version runs until the next one starts, and only the last of
    # them is still open.
    chained = Window.partitionBy(spec.key).orderBy("lsn")
    versioned = changed.withColumn("valid_to", F.lead("valid_from").over(chained)).withColumn(
        "is_current", F.lead("valid_from").over(chained).isNull()
    )

    return versioned.select(
        spec.key,
        *spec.tracked,
        "valid_from",
        "valid_to",
        "is_current",
        "is_deleted",
        "lsn",
        "attr_hash",
    )


def stage_for_merge(versions: DataFrame, spec: DimensionSpec) -> DataFrame:
    """Duplicate each key's earliest new version as a 'close the open row' instruction.

    See point 1 in the module docstring: one MERGE pass, two effects, so the source has to
    carry both. `merge_key` NULL can never match and is therefore always an insert.
    """
    earliest = Window.partitionBy(spec.key).orderBy("lsn")
    closers = (
        versions.withColumn("_rank", F.row_number().over(earliest))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn("merge_key", F.col(spec.key))
    )
    inserts = versions.withColumn("merge_key", F.lit(None).cast(versions.schema[spec.key].dataType))
    return closers.unionByName(inserts)


def merge_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    spec: DimensionSpec,
    target: str | None = None,
) -> None:
    """Apply one micro-batch to the SCD2 dimension."""
    from delta.tables import DeltaTable

    path = target if target is not None else spec.target
    changes = parse_changes(batch_df, spec)

    if not DeltaTable.isDeltaTable(spark, path):
        versions = new_versions(changes, spec, None)
        versions.write.format("delta").mode("append").save(path)
        print(f"batch {batch_id}: {versions.count()} version(s) created in {path}")
        return

    table = DeltaTable.forPath(spark, path)
    current = table.toDF().filter(F.col("is_current")).select(spec.key, "lsn", "attr_hash")
    versions = new_versions(changes, spec, current)
    staged = stage_for_merge(versions, spec)

    columns = [
        spec.key,
        *spec.tracked,
        "valid_from",
        "valid_to",
        "is_current",
        "is_deleted",
        "lsn",
        "attr_hash",
    ]
    (
        table.alias("t")
        .merge(staged.alias("s"), f"t.{spec.key} = s.merge_key AND t.is_current")
        # Close the open row at the moment the new version starts.
        .whenMatchedUpdate(set={"valid_to": "s.valid_from", "is_current": "false"})
        # Only the merge_key IS NULL copies are inserts. Without this condition the closing
        # copy would be inserted too whenever no open row existed.
        .whenNotMatchedInsert(
            condition="s.merge_key IS NULL", values={c: f"s.{c}" for c in columns}
        )
        .execute()
    )
    print(f"batch {batch_id}: {versions.count()} new version(s) merged into {path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an SCD2 dimension from Bronze CDC.")
    parser.add_argument("--dim", required=True, choices=sorted(DIMENSIONS))
    parser.add_argument("--once", action="store_true", help="process what is available, then stop")
    args = parser.parse_args(argv)
    spec = DIMENSIONS[args.dim]

    spark = SparkSession.builder.appName(f"silver-{spec.target_name}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    stream = spark.readStream.format("delta").load(spec.source)
    writer = (
        stream.writeStream.foreachBatch(lambda df, bid: merge_batch(df, bid, spark, spec))
        .option("checkpointLocation", spec.checkpoint)
        .queryName(f"silver-{spec.target_name}")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    writer.start().awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
