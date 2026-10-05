"""Kafka GPS pings -> Bronze Delta.

    make stream-gps
    make stream-gps ONCE=1

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

Sibling of bronze_cdc_stream.py, but the differences matter:

* **Append-only, no MERGE.** GPS pings are immutable facts, not row changes, and there is no
  primary key to merge on. Idempotency comes from the Kafka coordinates instead: a ping is
  identified by (topic, partition, offset), which cannot repeat. A replayed batch therefore
  re-inserts identical rows, so the dedup is a MERGE on those three columns rather than on
  (pk, lsn).

* **No before/after envelope.** The payload is the ping itself, so the Bronze row is flat.

* **event_ts is the device clock, not arrival.** Phase 3 watermarks on it, and chaos scenario
  1 will deliberately deliver pings long after it. Bronze therefore keeps both event_ts and
  ingest_ts and never conflates them - a pipeline that overwrites event time with arrival time
  cannot detect lateness at all, which is the whole point of the scenario.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

REGISTRY_URL = "http://redpanda:8081"
KAFKA_BOOTSTRAP = "redpanda:9092"
SUBJECT = "gps.pings-value"

BRONZE = "s3a://bronze"
QUARANTINE = "s3a://quarantine"
CHECKPOINTS = "s3a://checkpoints"

WIRE_HEADER_BYTES = 5


def fetch_schema(subject: str) -> str:
    url = f"{REGISTRY_URL}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url, timeout=10) as response:
        schema: str = json.load(response)["schema"]
    return schema


def to_bronze_pings(decoded: DataFrame) -> DataFrame:
    """Flatten decoded pings to the Bronze shape.

    Split out of write_batch so it can be asserted on directly: it is the only part of this
    job that is a pure function of its input, and the only part worth a unit test.
    """
    return decoded.filter(F.col("ping").isNotNull()).select(
        F.col("ping.rider_id").cast("long").alias("rider_id"),
        F.col("ping.trip_id").alias("trip_id"),
        F.col("ping.lat").alias("lat"),
        F.col("ping.lon").alias("lon"),
        F.col("ping.speed_kmph").alias("speed_kmph"),
        F.col("ping.heading_deg").alias("heading_deg"),
        F.col("ping.accuracy_m").alias("accuracy_m"),
        # Device time. Phase 3 watermarks on this; it is never the arrival time.
        #
        # NOT divided by 1000. The contract declares event_ts as
        # {"type": "long", "logicalType": "timestamp-millis"}, and from_avro honours the
        # logical type - so this arrives already decoded as a TIMESTAMP. Dividing it was this
        # job's first contact with real data and it failed outright:
        #   [DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] ... ("TIMESTAMP" and "INT")
        F.col("ping.event_ts").alias("event_ts"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
        F.to_date(F.col("ping.event_ts")).alias("event_date"),
    )


def write_batch(
    batch_df: DataFrame,
    batch_id: int,
    target: str,
    quarantine_path: str,
    spark: SparkSession,
) -> None:
    from delta.tables import DeltaTable

    batch_df.persist()
    try:
        bad = batch_df.filter(F.col("value").isNotNull() & F.col("ping").isNull())
        bad_count = bad.count()
        if bad_count:
            (
                bad.select("topic", "partition", "offset", "timestamp", "value")
                .withColumn("quarantined_at", F.current_timestamp())
                .withColumn("reason", F.lit("avro_decode_failed"))
                .write.format("delta")
                .mode("append")
                .save(quarantine_path)
            )
            print(f"batch {batch_id}: quarantined {bad_count} undecodable ping(s)")

        deduped = to_bronze_pings(batch_df).dropDuplicates(["topic", "partition", "offset"])

        if DeltaTable.isDeltaTable(spark, target):
            (
                DeltaTable.forPath(spark, target)
                .alias("t")
                .merge(
                    deduped.alias("s"),
                    "t.topic = s.topic AND t.partition = s.partition AND t.offset = s.offset",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            deduped.write.format("delta").partitionBy("event_date").mode("append").save(target)
        print(f"batch {batch_id}: {deduped.count()} candidate ping(s) merged into {target}")
    finally:
        batch_df.unpersist()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stream GPS pings into Bronze Delta.")
    parser.add_argument("--topic", default="gps.pings")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("bronze-gps").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema_json = fetch_schema(SUBJECT)
    print(f"decoding {args.topic} against {SUBJECT}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
    )
    stripped = raw.withColumn(
        "avro_payload",
        F.expr(f"substring(value, {WIRE_HEADER_BYTES + 1}, length(value))"),
    )
    decoded = stripped.withColumn(
        "ping", from_avro(F.col("avro_payload"), schema_json, {"mode": "PERMISSIVE"})
    )

    target = f"{BRONZE}/raw_gps"
    quarantine_path = f"{QUARANTINE}/bronze_gps"

    writer = (
        decoded.writeStream.foreachBatch(
            lambda df, bid: write_batch(df, bid, target, quarantine_path, spark)
        )
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_gps")
        .queryName("bronze-gps")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    writer.start().awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
