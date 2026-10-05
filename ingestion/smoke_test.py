"""Spark/Delta/S3A/Kafka smoke test.

Retires the configuration risk before bronze_cdc_stream.py is written: Delta jars resolving,
S3A reaching MinIO, the Delta transaction log actually working, and spark-sql-kafka loading.

Deliberately NOT a pytest: it must run inside the Spark container via spark-submit, which is
the same path the streaming job will take. If this passes, the streaming job's failures are
its own logic rather than the plumbing.

    docker compose ... exec -T spark-master /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 /opt/streamhouse/ingestion/smoke_test.py
"""

import sys

from pyspark.sql import SparkSession

TABLE = "s3a://bronze/_smoke"


def main() -> int:
    spark = SparkSession.builder.appName("streamhouse-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    failures = []

    print("\n=== 1. environment ===")
    print(f"spark   : {spark.version}")
    print(f"scala   : {spark.sparkContext._jvm.scala.util.Properties.versionNumberString()}")
    hadoop = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    print(f"hadoop  : {hadoop}")
    print(f"endpoint: {spark.conf.get('spark.hadoop.fs.s3a.endpoint')}")

    print("\n=== 2. write 5 rows as Delta to s3a://bronze/_smoke ===")
    rows = [(i, f"row-{i}", float(i) * 1.5) for i in range(1, 6)]
    df = spark.createDataFrame(rows, "id INT, label STRING, value DOUBLE")
    df.write.format("delta").mode("overwrite").save(TABLE)
    print(f"wrote {df.count()} rows")

    print("\n=== 3. read it back ===")
    back = spark.read.format("delta").load(TABLE)
    back.orderBy("id").show(truncate=False)
    n = back.count()
    print(f"row count: {n}")
    if n != 5:
        failures.append(f"expected 5 rows, read {n}")
    if [r.id for r in back.orderBy("id").collect()] != [1, 2, 3, 4, 5]:
        failures.append("row contents did not round-trip")

    print("\n=== 4. DESCRIBE HISTORY - proves this is Delta, not plain Parquet ===")
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`")
    hist.select("version", "operation", "operationMetrics").show(truncate=False)
    versions = [r.version for r in hist.collect()]
    print(f"versions in the transaction log: {versions}")
    if not versions:
        failures.append("DESCRIBE HISTORY returned no versions - not a Delta table")

    print("\n=== 5. second write -> the log must gain a version ===")
    spark.createDataFrame([(6, "row-6", 9.0)], "id INT, label STRING, value DOUBLE").write.format(
        "delta"
    ).mode("append").save(TABLE)
    v2 = [r.version for r in spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`").collect()]
    print(f"versions now: {v2}  (rows: {spark.read.format('delta').load(TABLE).count()})")
    if len(v2) <= len(versions):
        failures.append("append did not create a new Delta version")

    print("\n=== 6. time travel to version 0 ===")
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(TABLE)
    print(f"version 0 row count: {v0.count()} (expected 5, before the append)")
    if v0.count() != 5:
        failures.append("time travel to version 0 did not return the pre-append state")

    print("\n=== 7. spark-sql-kafka loads ===")
    try:
        reader = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "redpanda:9092")
            .option("subscribe", "cdc.public.orders")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
        )
        kdf = reader.load()
        print(f"kafka source loaded; schema: {[f.name for f in kdf.schema.fields]}")
        print(f"messages readable on cdc.public.orders: {kdf.count()}")
    except Exception as exc:
        failures.append(f"spark-sql-kafka failed: {type(exc).__name__}: {exc}")
        print(f"FAILED: {exc}")

    print("\n" + "=" * 60)
    if failures:
        print("SMOKE TEST FAILED")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("SMOKE TEST PASSED - Delta, S3A and Kafka all working")
    return 0


if __name__ == "__main__":
    spark_exit = main()
    SparkSession.builder.getOrCreate().stop()
    sys.exit(spark_exit)
