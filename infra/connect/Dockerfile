# Kafka Connect + Debezium, with Avro/schema-registry support added.
#
# The stock debezium/connect image ships no Avro converter at all - no avro-converter, no
# kafka-avro-serializer, no schema-registry-client. Confluent's cp-kafka-connect has them,
# so they are copied across rather than assembled jar-by-jar from Maven.
#
# CLOUD-FREE RULE. Confluent's serde bundle carries 13 cloud SDK jars for client-side field
# encryption against cloud KMS - the real aws-java-sdk, azure-identity,
# azure-security-keyvault-keys, tink-gcpkms and so on. CLAUDE.md forbids cloud client
# libraries anywhere in dependencies, and we use no KMS, so they are purged in the builder
# stage. Doing it there means they never exist in any layer of the shipped image.
#
# Verify with:
#   docker exec sh-connect sh -c 'ls /kafka/connect/avro | grep -icE "aws|azure|gcp|hcvault"'
# which must print 0.

FROM confluentinc/cp-kafka-connect:7.6.0 AS serde
USER root
WORKDIR /usr/share/java/kafka-serde-tools
# `find` is absent from Confluent's minimal UBI image, so plain shell globs it is.
RUN rm -f *aws* *azure* *gcp* *hcvault*

FROM debezium/connect:2.7.3.Final
COPY --from=serde /usr/share/java/kafka-serde-tools /kafka/connect/avro
