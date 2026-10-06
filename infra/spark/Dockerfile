# Spark 3.5.3 with Delta Lake, S3A and Kafka support baked in.
#
# The stock apache/spark:3.5.3 image ships none of them: no delta-spark, no hadoop-aws, no
# spark-sql-kafka. Resolving them at submit time with --packages needs network on every run
# and leaves the working configuration in a shell history instead of the repository, so they
# are pinned here.
#
# VERSIONS ARE NOT FREE CHOICES:
#   Scala 2.12.18  -> every _2.12 artifact must match
#   Hadoop 3.3.4   -> hadoop-aws MUST be exactly 3.3.4, or S3A fails on NoSuchMethodError
#   Spark 3.5.3    -> spark-sql-kafka-0-10 and its token provider must match exactly
#   Delta 3.2.1    -> the release built for Spark 3.5.x
#   spark-avro     -> not in the base image; from_avro() does not exist without it
#
# ON aws-java-sdk-bundle AND THE CLOUD-FREE RULE (CLAUDE.md rule 1):
# hadoop-aws is the only implementation of s3a://, and it requires the AWS SDK. CLAUDE.md
# explicitly allows "MinIO - self-hosted, S3A-compatible ... s3a:// URIs point at local MinIO
# only", so the client library that makes s3a:// work is covered by that allowance. It is a
# protocol client pinned at http://minio:9000 and never speaks to AWS. This is materially
# different from the jars stripped out of the Connect image, which were cloud KMS clients for
# a service we do not use. See ADR-0009.

FROM apache/spark:3.5.3

USER root

ARG DELTA_VERSION=3.2.1
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG SPARK_VERSION=3.5.3
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG SCALA_BIN=2.12

ENV MAVEN=https://repo1.maven.org/maven2

RUN set -eux; cd /opt/spark/jars; \
    curl -fsSLO "$MAVEN/io/delta/delta-spark_${SCALA_BIN}/${DELTA_VERSION}/delta-spark_${SCALA_BIN}-${DELTA_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BIN}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BIN}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/org/apache/spark/spark-avro_${SCALA_BIN}/${SPARK_VERSION}/spark-avro_${SCALA_BIN}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BIN}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BIN}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "$MAVEN/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

# The delta-spark JAR covers the Scala side, which is all `format("delta")` and SQL need.
# The Python DeltaTable API - required for MERGE in foreachBatch - lives in the PyPI package.
# --no-deps because its only dependency is pyspark, which the image already has; letting pip
# resolve it would install a second, conflicting copy.
RUN pip install --no-cache-dir --no-deps delta-spark==3.2.1

# Test tooling. The DataFrame transforms under ingestion/ are checked against this Spark, not
# a mock or a host install: the host venv has no JVM, and a pyspark wheel pinned to match this
# image would be a second copy to keep in step for no gain. `make test-spark` runs them here.
#
# pytest 8.4 requires Python 3.9; this image is on 3.8.10 (ADR-0009), so 8.3.5 is the last
# release that runs here. chispa gives DataFrame equality assertions - the CLAUDE.md
# convention - instead of hand-rolled collect() comparisons.
RUN pip install --no-cache-dir pytest==8.3.5 chispa==0.9.4

# dbt builds Gold (Phase 4). It runs here rather than on the host for the same reason the
# Spark jobs do: this is where Spark, Delta and the S3A client actually are.
#
# dbt-spark's `session` method creates a SparkSession in-process - no Thrift server, no extra
# container. It calls enableHiveSupport(), so it needs a metastore that survives between
# invocations: `dbt run` and `dbt test` are separate processes, and an in-memory catalog would
# forget every table in between.
RUN pip install --no-cache-dir "dbt-core==1.8.7" "dbt-spark[session]==1.8.0"

RUN mkdir -p /opt/spark/conf
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# A SECOND conf directory, used only by dbt via SPARK_CONF_DIR.
#
# The metastore is deliberately not in the default conf: switching the whole image to a Hive
# catalog would make every streaming job open a metastore connection at startup, for tables
# none of them reference - a new way for an ingestion job to fail, bought for nothing.
#
# Derby rather than Postgres, because Derby needs no credentials. A committed conf file with a
# password in it would breach CLAUDE.md's rule on secrets, and generating the file at runtime
# is more moving parts than this is worth. Derby is single-writer, which is fine: dbt is the
# only thing that uses it, one invocation at a time.
# Both of these are mount points for named volumes, and a fresh named volume inherits the
# ownership and mode of the image directory it covers. They must be writable by `spark` here,
# or the volume is created root-owned and dbt dies without printing anything at all - it
# cannot even open its log file to say why.
RUN mkdir -p /opt/dbt-conf /opt/metastore /opt/dbt-target \
    && chmod 777 /opt/metastore /opt/dbt-target
COPY spark-defaults.conf /opt/dbt-conf/spark-defaults.conf
RUN { \
      echo ""; \
      echo "# ---------------------------------------------------------------- dbt only"; \
      echo "spark.sql.catalogImplementation hive"; \
      echo "spark.sql.warehouse.dir s3a://gold/warehouse"; \
      echo "spark.hadoop.javax.jdo.option.ConnectionURL jdbc:derby:;databaseName=/opt/metastore/metastore_db;create=true"; \
      echo "spark.hadoop.javax.jdo.option.ConnectionDriverName org.apache.derby.jdbc.EmbeddedDriver"; \
      echo "spark.hadoop.hive.metastore.schema.verification false"; \
      echo "spark.hadoop.datanucleus.schema.autoCreateAll true"; \
      echo ""; \
      echo "# INSERT OVERWRITE must replace only the partitions the query produced, not the"; \
      echo "# whole table. Static mode would make a one-day backfill delete every other day,"; \
      echo "# which is the opposite of what agg_sla_daily's incremental strategy intends."; \
      echo "spark.sql.sources.partitionOverwriteMode dynamic"; \
    } >> /opt/dbt-conf/spark-defaults.conf

USER spark
