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

RUN mkdir -p /opt/spark/conf
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
