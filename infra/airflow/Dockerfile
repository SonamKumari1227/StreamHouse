# Airflow 3.0.1 with a Docker CLI, so DAGs can drive the Spark jobs.
#
# HOW AIRFLOW RUNS SPARK WORK, AND WHY THIS WAY
#
# The Silver jobs and dbt run inside the Spark image. Airflow needs to start them, and there
# are three ways to do that:
#
#   1. Install Spark in this image and submit to spark://spark-master:7077 as a client. A
#      400 MB download plus every jar from infra/spark/Dockerfile, pinned twice and able to
#      drift apart. The SparkSubmitOperator needs this.
#   2. Talk to the Spark master's REST submission API. Disabled by default, undocumented, and
#      it would not help with dbt at all.
#   3. Run the same `docker exec` the Makefile runs. One static binary, and Airflow executes
#      exactly the command a human would - so a DAG failure is reproducible by hand.
#
# Option 3. The cost is the Docker socket, mounted into these containers: anything that can
# talk to it can control the daemon, which is effectively host root. That is acceptable for a
# local single-user stack and would not be in a shared one - see ADR-0011.
#
# The CLI is the static binary rather than the apt repository: one pinned URL, no keyring
# dance, and it matches the daemon version this project runs against.

FROM apache/airflow:3.0.1

ARG DOCKER_CLI_VERSION=27.5.1

USER root

RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
      -o /tmp/docker.tgz \
    && tar -xzf /tmp/docker.tgz -C /tmp docker/docker \
    && mv /tmp/docker/docker /usr/local/bin/docker \
    && chmod +x /usr/local/bin/docker \
    && rm -rf /tmp/docker.tgz /tmp/docker

USER airflow
