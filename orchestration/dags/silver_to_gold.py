"""silver_to_gold - the batch path, Bronze through to a tested star schema.

    Bronze CDC  ->  Silver (fact, 3 dimensions, trips)  ->  quality gate  ->  dbt build
                                                                               -> maintenance

DYNAMIC TASK MAPPING

The three dimension builds and the five quality gates are `.expand()`ed rather than written
out by hand. That is not decoration: the gate list is derived from the same constant the gate
module uses, so adding a Silver table adds its gate automatically instead of silently going
unchecked - which is exactly the kind of omission nobody notices until the data is wrong.

ORDERING

Silver before the gate, obviously; but also **the gate before dbt**. A quarantined row means
Silver disagrees with itself, and building a star schema on top of that produces a Gold layer
that is confidently wrong. The gate exits non-zero, the task fails, and dbt never runs.

Maintenance runs last and only on success: compacting files that a failed run may re-write is
wasted work.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from streamhouse_common import (
    DEFAULT_ARGS,
    DIMENSIONS,
    GATED_TABLES,
    dbt,
    quality_gate,
    spark_job,
)


@dag(
    dag_id="silver_to_gold",
    description="Bronze -> Silver -> quality gate -> Gold (dbt) -> maintenance.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Every task here is a spark-submit, and every spark-submit is a JVM driver. The cluster
    # has 2 cores and each job pins 1, so a third concurrent task queues inside Spark while
    # still holding its driver's memory. Two is what the cluster can actually use; more than
    # that exhausted the WSL VM during development and wedged the Docker daemon.
    max_active_tasks=2,
    default_args=DEFAULT_ARGS,
    tags=["streamhouse", "batch", "phase-5"],
)
def silver_to_gold() -> None:
    # ---------------------------------------------------------------- Silver
    #
    # Each job streams from its Bronze Delta table with `--once`: drain what is available,
    # then stop. Checkpointed, so a run picks up where the last one left off rather than
    # rebuilding, and re-running when nothing arrived is a no-op.
    fact = BashOperator(
        task_id="silver_fact_order_state",
        bash_command=spark_job("transform/silver/fact_order_state.py", "--once"),
    )

    @task(task_id="silver_dimension")
    def silver_dimension(dimension: str) -> str:
        """One SCD2 dimension. Mapped over DIMENSIONS."""
        import subprocess

        command = spark_job("transform/silver/dim_scd2.py", f"--dim {dimension} --once")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        tail = result.stdout.strip().splitlines()[-5:]
        print("\n".join(tail))
        if result.returncode != 0:
            raise RuntimeError(f"{dimension} failed ({result.returncode}): {result.stderr[-2000:]}")
        return dimension

    dimensions = silver_dimension.expand(dimension=DIMENSIONS)

    trips = BashOperator(
        task_id="silver_gps_trips",
        bash_command=spark_job("transform/silver/gps_trips_sessionized.py", "--once"),
    )

    # ---------------------------------------------------------------- the gate
    @task(task_id="quality_gate")
    def gate(table: str) -> str:
        """One Silver table against its expectation suite. Mapped over GATED_TABLES.

        A non-zero exit means rows were quarantined. The task fails and dbt does not run.
        """
        import subprocess

        result = subprocess.run(quality_gate(table), shell=True, capture_output=True, text=True)
        summary = "\n".join(
            line
            for line in result.stdout.splitlines()
            if line.startswith(("rows", "passed", "quarantined", "==="))
        )
        print(summary or result.stdout[-2000:])
        if result.returncode != 0:
            raise RuntimeError(f"{table} failed its expectations:\n{summary}")
        return table

    gates = gate.expand(table=GATED_TABLES)

    # ---------------------------------------------------------------- Gold
    reference = BashOperator(
        task_id="reference_data",
        bash_command=spark_job("ingestion/reference_data.py"),
    )

    gold = BashOperator(
        task_id="dbt_build",
        bash_command=dbt("build"),
    )

    docs = BashOperator(
        task_id="dbt_docs",
        bash_command=dbt("docs generate"),
    )

    maintain = BashOperator(
        task_id="silver_maintain",
        bash_command=spark_job("transform/silver/maintain.py"),
    )

    [fact, dimensions, trips] >> gates >> reference >> gold >> docs >> maintain


silver_to_gold()
