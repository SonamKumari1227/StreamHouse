# ADR-0011: Airflow drives Spark through the Docker socket

- **Status:** Accepted
- **Date:** 2026-10-06
- **Phase:** 5

## Context

Airflow has to start work that runs somewhere else. Every Spark job in this project — the
Silver builds, the quality gate, the reference extract, the maintenance pass — runs inside the
Spark image via `spark-submit`, and dbt runs there too, with its own `SPARK_CONF_DIR` and a
Derby metastore (ADR-0010, and `transform/gold_dbt/README.md`). Airflow itself is a separate
container with none of that: no Spark, no Delta jars, no S3A client, no dbt.

So the question is not *what* Airflow should run — the Makefile already defines that — but how
a scheduler in one container starts a process in another.

## Options considered

| Option | Pros | Cons |
| --- | --- | --- |
| **A — Docker socket + `docker exec` (chosen)** | One static 50 MB binary in the Airflow image. Airflow issues the identical command a human issues, so any failing task is reproducible by hand, and there is one definition of how a job launches. Works for dbt and spark-submit alike. | Mounts `/var/run/docker.sock`. Anything that can talk to it controls the daemon, which is effectively host root. Ties the DAGs to this deployment shape. |
| B — Install Spark in the Airflow image, use `SparkSubmitOperator` | The idiomatic Airflow answer. The operator surfaces the application id and driver logs properly. | A ~400 MB Spark distribution plus every jar from `infra/spark/Dockerfile`, pinned in two Dockerfiles that can drift apart — and a version skew between client and cluster fails in ways that look like data problems. Does nothing for dbt, which would still need its own path. |
| C — Spark master REST submission API (port 6066) | No socket, no Spark client. | Disabled by default, effectively undocumented, and unstable across versions. Still no answer for dbt. |
| D — A sidecar that polls a queue and runs jobs | No socket in the Airflow container. | A bespoke job runner to write, test and operate. Inventing a worse Celery to avoid a mount. |

## Decision

Mount the Docker socket into the Airflow containers, install only the Docker CLI, and have the
DAGs run `docker exec` against the Spark container.

What drove it was keeping **one** definition of how a job starts. Option B would have meant the
Makefile and the DAGs launching the same job by different mechanisms, with different jars and
different failure modes — and the first time a DAG failed, the first question would be whether
it failed for a reason that `make` would also hit. With `docker exec`, a red task is a command
that can be pasted into a terminal.

The socket is a real cost, stated plainly: this is a single-user local stack where the user
already has Docker, so mounting it grants Airflow nothing the operator does not already have.
That reasoning does not survive contact with a shared environment, and **this is the thing to
change first if the project is ever deployed anywhere real** — which is exactly the kind of
local-to-cloud mapping Phase 7 is for.

## Consequences

- `infra/airflow/Dockerfile` adds one pinned static binary and nothing else.
- The containers join the socket's group via `SH_DOCKER_GID` in `.env`; it is machine-specific
  (`stat -c %g /var/run/docker.sock`) and defaults to 1001.
- `orchestration/dags/streamhouse_common.py` holds the command builders, so the shape of a
  submission is written once rather than in four DAGs.
- A DAG failure can be reproduced with the equivalent `make` target. That is the property the
  whole decision was bought for.
- Phase 7's portability exercise must record this as the component with no cloud equivalent:
  the managed answer is a Spark job operator or a container task, not a socket.
