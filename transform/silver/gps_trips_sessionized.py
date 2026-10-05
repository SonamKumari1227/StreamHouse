"""Bronze GPS pings -> Silver `gps_trips_sessionized`.

    make silver-trips
    make silver-trips ONCE=1

Grain: **one row per (rider, trip).** A stream of ~200 pings/s is not something anyone
queries directly; a trip is.

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

WATERMARKING, AND WHAT IT ACTUALLY BUYS

The aggregation is keyed on `trip_id`, which the device assigns, so this is not gap-based
sessionization - there is no "30 minutes of silence ends a trip" heuristic to get wrong. What
the watermark does instead is bound **state**: without one, Spark would keep every trip key it
has ever seen in memory forever, because any ping could in principle still arrive for any of
them. `withWatermark("event_ts", "15 minutes")` lets it drop a trip's state once event time
has moved 15 minutes past that trip's last ping.

The watermark is on `event_ts`, the **device clock** - never `ingest_ts`. Bronze keeps both
precisely so this choice exists, and watermarking on arrival time would make lateness
undetectable by definition: every ping is punctual the moment it lands. Chaos scenario 1
delivers pings long after they were emitted, and only an event-time watermark notices.

The trade: a ping arriving more than 15 minutes (in event time) after its trip's latest ping
is dropped by Spark, silently. That is the designed cost of bounded state, not an oversight.
`--late-tolerance` exists so a backfill can widen it.

WHY `update` AND A MERGE, NOT `append`

`append` output mode would emit each trip exactly once, when the watermark finally passes it -
correct, but nothing appears until then, and a trip in progress is invisible. `update` plus a
MERGE keyed on (rider_id, trip_id) writes the trip as soon as it has pings and refines it as
more arrive. Re-stating a trip is idempotent because the merge overwrites its metrics rather
than accumulating them, so a replayed batch converges on the same row.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE = "s3a://bronze"
SILVER = "s3a://silver"
CHECKPOINTS = "s3a://checkpoints"

SOURCE = f"{BRONZE}/raw_gps"
TARGET = f"{SILVER}/gps_trips_sessionized"

EARTH_RADIUS_KM = 6371.0

# Metrics the MERGE overwrites. Listed once so insert and update cannot drift apart.
METRIC_COLUMNS = (
    "started_ts",
    "ended_ts",
    "duration_s",
    "ping_count",
    "avg_speed_kmph",
    "max_speed_kmph",
    "start_lat",
    "start_lon",
    "end_lat",
    "end_lon",
    "straight_line_km",
    "trip_date",
)


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in kilometres.

    Straight line start to end, deliberately: summing the distance between consecutive pings
    would need an ordered walk within each group, which a streaming aggregation cannot do.
    This is a displacement, not a route length, and the column name says so.
    """
    phi1, phi2 = F.radians(lat1), F.radians(lat2)
    d_phi = F.radians(lat2 - lat1)
    d_lambda = F.radians(lon2 - lon1)
    a = F.pow(F.sin(d_phi / 2), 2) + F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(d_lambda / 2), 2)
    return F.lit(EARTH_RADIUS_KM) * 2 * F.asin(F.sqrt(a))


def sessionize(pings: DataFrame, late_tolerance: str = "15 minutes") -> DataFrame:
    """Pings -> one row per (rider, trip).

    `withWatermark` is a no-op on a static DataFrame, which is what makes this function
    testable in batch while being the same code the stream runs.
    """
    grouped = (
        pings.withWatermark("event_ts", late_tolerance)
        .groupBy("rider_id", "trip_id")
        .agg(
            F.min("event_ts").alias("started_ts"),
            F.max("event_ts").alias("ended_ts"),
            F.count(F.lit(1)).alias("ping_count"),
            F.avg("speed_kmph").alias("avg_speed_kmph"),
            F.max("speed_kmph").alias("max_speed_kmph"),
            # min_by/max_by rather than first/last: a group has no inherent order, and
            # first() would return whichever row the shuffle happened to put in front.
            F.expr("min_by(lat, event_ts)").alias("start_lat"),
            F.expr("min_by(lon, event_ts)").alias("start_lon"),
            F.expr("max_by(lat, event_ts)").alias("end_lat"),
            F.expr("max_by(lon, event_ts)").alias("end_lon"),
        )
    )

    return (
        grouped.withColumn(
            "duration_s",
            F.col("ended_ts").cast("double") - F.col("started_ts").cast("double"),
        )
        .withColumn(
            "straight_line_km",
            haversine_km(
                F.col("start_lat"), F.col("start_lon"), F.col("end_lat"), F.col("end_lon")
            ),
        )
        # Partition on the day the trip began, in event time.
        .withColumn("trip_date", F.to_date("started_ts"))
    )


def merge_batch(
    batch_df: DataFrame, batch_id: int, spark: SparkSession, target: str = TARGET
) -> None:
    """Upsert one batch of trips. Re-stating a trip overwrites it rather than accumulating."""
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, target):
        assignments = {c: f"s.{c}" for c in METRIC_COLUMNS}
        (
            DeltaTable.forPath(spark, target)
            .alias("t")
            .merge(batch_df.alias("s"), "t.rider_id = s.rider_id AND t.trip_id = s.trip_id")
            .whenMatchedUpdate(set=assignments)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        batch_df.write.format("delta").partitionBy("trip_date").mode("append").save(target)

    print(f"batch {batch_id}: {batch_df.count()} trip(s) merged into {target}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sessionize Bronze GPS pings into trips.")
    parser.add_argument(
        "--late-tolerance",
        default="15 minutes",
        help="event-time watermark delay; widen for a backfill (default: 15 minutes)",
    )
    parser.add_argument("--once", action="store_true", help="process what is available, then stop")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("silver-gps-trips").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    pings = spark.readStream.format("delta").load(SOURCE)
    trips = sessionize(pings, args.late_tolerance)

    writer = (
        trips.writeStream.foreachBatch(lambda df, bid: merge_batch(df, bid, spark))
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_gps_trips")
        .queryName("silver-gps-trips")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    writer.start().awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
