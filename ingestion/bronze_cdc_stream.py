"""Kafka CDC -> Bronze Delta.

Reads Debezium change events for one table off Redpanda, decodes the Avro envelope against
the schema registry, and lands them in an append-only Bronze Delta table on MinIO.

    make stream-bronze                      # orders, the default
    make stream-bronze TABLE=payments

PYTHON 3.8. This runs via spark-submit inside the Spark image, which has Python 3.8.10 - not
the 3.11 the rest of the project targets. No StrEnum, no slots=True dataclasses, no match.
See ADR-0009.

Three things here are load-bearing:

1. **Confluent wire format, not bare Avro.** Debezium prefixes every payload with a magic
   byte and a 4-byte schema id. `from_avro` expects neither, so the first five bytes are
   stripped before decoding. Feeding the whole payload to `from_avro` fails in a way that
   looks like schema corruption.

2. **Idempotency is a MERGE, not an append.** `foreachBatch` gives at-least-once: a batch can
   be replayed after failure. The write is therefore a MERGE keyed on
   `(source_table, pk, lsn)` with only WHEN NOT MATCHED THEN INSERT - append-only semantics,
   but replaying a batch inserts nothing the second time. That key is ADR-0008's, and Phase 1
   proved it rejects real redelivered changes.

3. **Undecodable rows go to quarantine, never to /dev/null.** A row whose payload does not
   match the registered schema is a contract violation and evidence; dropping it silently is
   how a pipeline lies about its own completeness.

4. **Tombstones are not violations.** Debezium emits a null-valued record after every delete
   so the topic can be log-compacted. It carries no payload, so it cannot be decoded - but it
   is routine traffic, and the preceding op='d' event already carries the full before image.
   The first version of this job quarantined them, which would have filled the DLQ during
   normal operation and made Phase 6's quality metric read as a permanent failure.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

# The registry is reachable inside the compose network under this name.
REGISTRY_URL = "http://redpanda:8081"
KAFKA_BOOTSTRAP = "redpanda:9092"

BRONZE = "s3a://bronze"
QUARANTINE = "s3a://quarantine"
CHECKPOINTS = "s3a://checkpoints"

# Confluent wire format: 1 magic byte + 4 bytes of big-endian schema id.
WIRE_HEADER_BYTES = 5

# Primary key column per captured table, used to build the dedup key.
PRIMARY_KEYS = {
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "restaurants": "restaurant_id",
    "menu_items": "menu_item_id",
    "riders": "rider_id",
}


def fetch_schema(subject: str) -> str:
    """Latest registered Avro schema for a subject, as a JSON string.

    urllib rather than requests: the Spark image ships neither requests nor fastavro, and
    stdlib is one less thing to pin.
    """
    url = f"{REGISTRY_URL}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url, timeout=10) as response:
        schema: str = json.load(response)["schema"]
    return schema


def build_stream(
    spark: SparkSession, table: str, schema_json: str, starting_offsets: str
) -> DataFrame:
    """Kafka -> decoded CDC rows, with the undecodable ones kept rather than dropped."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", f"cdc.public.{table}")
        .option("startingOffsets", starting_offsets)
        # Bound each batch so a backlog cannot produce one enormous first micro-batch.
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )

    stripped = raw.withColumn(
        "avro_payload", F.expr(f"substring(value, {WIRE_HEADER_BYTES + 1}, length(value))")
    )

    # PERMISSIVE: a row that does not match the schema decodes to null rather than killing
    # the batch. Those rows are routed to quarantine downstream.
    decoded = stripped.withColumn(
        "envelope", from_avro(F.col("avro_payload"), schema_json, {"mode": "PERMISSIVE"})
    )
    return decoded


def to_bronze_rows(decoded: DataFrame, table: str) -> DataFrame:
    """Flatten to the Bronze shape: full payload plus the offsets needed to rebuild."""
    pk_col = PRIMARY_KEYS[table]
    # A delete carries its key in `before`; everything else carries it in `after`.
    pk = F.coalesce(F.col(f"envelope.after.{pk_col}"), F.col(f"envelope.before.{pk_col}"))
    return decoded.select(
        F.lit(table).alias("source_table"),
        pk.cast("long").alias("pk"),
        F.col("envelope.source.lsn").cast("long").alias("lsn"),
        F.col("envelope.op").alias("op"),
        F.col("envelope.ts_ms").cast("long").alias("event_ts_ms"),
        F.to_json(F.col("envelope.before")).alias("before_json"),
        F.to_json(F.col("envelope.after")).alias("after_json"),
        F.to_json(F.col("envelope.source")).alias("source_json"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
        F.to_date(F.current_timestamp()).alias("ingest_date"),
    )


def classify_batch(batch_df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split one micro-batch into (tombstones, undecodable, decodable).

    The three are disjoint and together cover the batch, which is the point: every row has
    exactly one destination, and none can fall between the filters.

    **The test is `envelope.op`, not `envelope`.** `from_avro` in PERMISSIVE mode returns a
    struct whose fields are all null when it cannot decode a payload - not a null struct. So
    `envelope IS NULL` is false for a row that decoded to nothing, and the row sails past the
    quarantine filter into Bronze as a row of nulls. Two of them reached `raw_orders_cdc`
    before this was caught, and they were unkillable: the MERGE predicate `t.pk = s.pk` never
    matches when pk is null, so every replay inserted them again.

    `op` is the right probe because the Debezium envelope declares it as a required string,
    so a non-null `op` means the payload genuinely decoded.
    """
    is_tombstone = F.col("value").isNull()
    decoded = F.col("envelope.op").isNotNull()
    return (
        batch_df.filter(is_tombstone),
        batch_df.filter(~is_tombstone & ~decoded),
        batch_df.filter(~is_tombstone & decoded),
    )


def write_batch(batch_df: DataFrame, batch_id: int, table: str, spark: SparkSession) -> None:
    """Idempotent write of one micro-batch, plus the quarantine split.

    Called by foreachBatch, which is at-least-once: this function must tolerate being handed
    the same batch twice.
    """
    from delta.tables import DeltaTable

    batch_df.persist()
    try:
        # A null Kafka value is a tombstone, not a failure, and not Bronze's business either.
        tombstone_df, bad, good_df = classify_batch(batch_df)

        tombstones = tombstone_df.count()
        if tombstones:
            print(f"batch {batch_id}: skipped {tombstones} tombstone(s)")

        bad_count = bad.count()
        if bad_count:
            (
                bad.select("topic", "partition", "offset", "timestamp", "value")
                .withColumn("quarantined_at", F.current_timestamp())
                .withColumn("reason", F.lit("avro_decode_failed"))
                .write.format("delta")
                .mode("append")
                .save(f"{QUARANTINE}/bronze_{table}_cdc")
            )
            print(f"batch {batch_id}: quarantined {bad_count} undecodable row(s)")

        good = to_bronze_rows(good_df, table)

        # Two changes to the same row inside one batch would make the MERGE ambiguous, so
        # collapse to the latest LSN per key first. Same rule Phase 3's SCD2 needs.
        deduped = good.dropDuplicates(["source_table", "pk", "lsn"])

        target_path = f"{BRONZE}/raw_{table}_cdc"
        if DeltaTable.isDeltaTable(spark, target_path):
            (
                DeltaTable.forPath(spark, target_path)
                .alias("t")
                .merge(
                    deduped.alias("s"),
                    "t.source_table = s.source_table AND t.pk = s.pk AND t.lsn = s.lsn",
                )
                # INSERT only. Bronze is append-only; a matched row means this batch is a
                # replay and there is nothing to do.
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            (
                deduped.write.format("delta")
                .partitionBy("ingest_date")
                .mode("append")
                .save(target_path)
            )
        # Candidates, not insertions: a replayed batch merges the same rows and inserts
        # none of them. The authoritative count is in the Delta history.
        print(f"batch {batch_id}: {deduped.count()} candidate row(s) merged into {target_path}")
    finally:
        batch_df.unpersist()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stream Debezium CDC into Bronze Delta.")
    parser.add_argument("--table", default="orders", choices=sorted(PRIMARY_KEYS))
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        help="earliest (default) or latest. Ignored once a checkpoint exists.",
    )
    parser.add_argument("--once", action="store_true", help="process what is available, then stop")
    args = parser.parse_args(argv)

    table = args.table
    subject = f"cdc.public.{table}-value"

    spark = SparkSession.builder.appName(f"bronze-cdc-{table}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema_json = fetch_schema(subject)
    print(f"decoding {table} against {subject} (schema {len(schema_json)} bytes)")

    decoded = build_stream(spark, table, schema_json, args.starting_offsets)

    writer = (
        decoded.writeStream.foreachBatch(lambda df, bid: write_batch(df, bid, table, spark))
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_{table}_cdc")
        .queryName(f"bronze-cdc-{table}")
    )
    if args.once:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
