"""Shared pieces for the StreamHouse DAGs.

HOW A DAG STARTS SPARK WORK

Airflow does not run Spark. It runs `docker exec` against the Spark container, issuing exactly
the command the Makefile issues - so a failing task can be reproduced by hand, and there is
one definition of how a job launches rather than two that drift apart. See ADR-0011 for why
the Docker socket, and what it costs.

ON SLAs

Airflow 3.0 **removed** the `sla` parameter and the SLA-miss callback; the replacement
(deadline alerts, AIP-86) is not in 3.0.1. Lateness is therefore enforced with
`execution_timeout`, which fails a task that overruns, plus `on_failure_callback` to record
it. That is narrower than SLA callbacks were - it fires on overrun, not on a task that never
started - so the freshness DAG covers the second case directly.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

SPARK_CONTAINER = os.environ.get("SH_SPARK_CONTAINER", "sh-spark-master")
REPO = os.environ.get("SH_REPO_IN_SPARK", "/opt/streamhouse")

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"

# One core per job, for the reason in docs/runbook.md 4.9: an uncapped streaming query takes
# every core in the cluster and never gives them back, and the next task then waits forever
# with no error to explain why.
STREAM_CORES = 1


def spark_job(script: str, args: str = "", cores: int = STREAM_CORES) -> str:
    """A bash command that submits one of the repo's Spark jobs."""
    return (
        f"docker exec {SPARK_CONTAINER} {SPARK_SUBMIT}"
        f" --master {SPARK_MASTER}"
        f" --conf spark.cores.max={cores}"
        f" {REPO}/{script} {args}"
    ).strip()


def quality_gate(table: str) -> str:
    """The gate exits 1 when any row is quarantined, which fails the task. That is the point.

    PYTHONPATH because spark-submit puts the script's directory on sys.path, not the repo
    root, and run_gate.py imports quality.expectations.
    """
    return (
        f"docker exec -e PYTHONPATH={REPO} {SPARK_CONTAINER} {SPARK_SUBMIT}"
        f" --master {SPARK_MASTER}"
        f" --conf spark.cores.max={STREAM_CORES}"
        f" --conf spark.executorEnv.PYTHONPATH={REPO}"
        f" {REPO}/quality/run_gate.py --table {table}"
    )


def dbt(command: str) -> str:
    """dbt runs inside the Spark image with its own SPARK_CONF_DIR. See transform/gold_dbt/."""
    return (
        "docker exec"
        " -e SPARK_CONF_DIR=/opt/dbt-conf"
        f" -e DBT_PROFILES_DIR={REPO}/transform/gold_dbt"
        f" -w {REPO}/transform/gold_dbt"
        f" {SPARK_CONTAINER} dbt {command}"
    )


def record_failure(context: dict[str, Any]) -> None:
    """What a Phase 6 alert will hang off.

    Deliberately only a log line for now: there is no alerting backend until Phase 6, and a
    callback that pretends to notify someone is worse than one that plainly does not.
    """
    task = context.get("task_instance")
    print(
        f"ALERT dag={getattr(task, 'dag_id', '?')} task={getattr(task, 'task_id', '?')} "
        f"run={context.get('run_id', '?')} reason={context.get('reason', 'task failed')}"
    )


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "streamhouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": record_failure,
    # Nothing here should take fifteen minutes. A task that does is stuck, and failing it
    # frees the cores for the next run instead of blocking the schedule behind it.
    "execution_timeout": timedelta(minutes=15),
}

# The Silver dimensions, mapped over rather than written out three times.
DIMENSIONS = ["restaurants", "riders", "menu_items"]

# Every Silver table the gate knows how to check.
GATED_TABLES = [
    "fact_order_state",
    "dim_restaurant_scd2",
    "dim_rider_scd2",
    "dim_menu_item_scd2",
    "gps_trips_sessionized",
]
