"""streaming_health - is the CDC path alive, and is Bronze still receiving data?

Runs every 15 minutes and gates nothing by itself. Its job is to answer, at any moment, the
question the batch path quietly assumes: *is the stream still running?*

WHY THIS IS A SEPARATE DAG

Phase 3 established the hard way that CDC not landed inside Kafka's 7-day retention is gone -
not degraded, gone, because the replication slot has advanced past it. A stream that silently
stops is therefore a deadline, and the thing most likely to go unnoticed, because nothing
fails: the batch pipeline keeps running happily on data that stopped arriving.

The three checks are deliberately independent. Each answers a different question, and one
failing should not hide the state of the others.

These check liveness, not correctness. Correctness is the quality gate's job.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from streamhouse_common import DEFAULT_ARGS, spark_job

# Bronze should never be this far behind. Generous, because the generator is not always
# running on a developer's laptop - the point is to catch "stopped", not "slow".
MAX_BRONZE_AGE_HOURS = 24


@dag(
    dag_id="streaming_health",
    description="Connector state, replication slot, and Bronze freshness.",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["streamhouse", "health", "phase-5"],
)
def streaming_health() -> None:
    # The connector is the single point of failure for every CDC topic. If its task has died,
    # nothing downstream notices on its own - Bronze simply stops growing.
    connector_running = BashOperator(
        task_id="connector_running",
        bash_command=(
            "state=$(docker exec sh-connect curl -sf"
            " http://localhost:8083/connectors/streamhouse-postgres/status"
            ' | python3 -c "import json,sys; d=json.load(sys.stdin);'
            " print(d['connector']['state'], d['tasks'][0]['state'])\"); "
            'echo "connector/task: $state"; '
            '[ "$state" = "RUNNING RUNNING" ]'
        ),
    )

    # An inactive replication slot retains WAL forever and fills the source disk - ADR-0001's
    # named hazard, and the failure that takes Postgres down rather than merely the pipeline.
    slot_active = BashOperator(
        task_id="replication_slot_active",
        bash_command=(
            "active=$(docker exec sh-postgres psql -U streamhouse -d streamhouse -tAc"
            " \"SELECT active FROM pg_replication_slots WHERE slot_name='streamhouse_slot'\"); "
            'echo "slot active: $active"; '
            '[ "$active" = "t" ]'
        ),
    )

    # Reads the newest ROW, not the newest file: OPTIMIZE rewrites every file in a table that
    # has received nothing for a week, so file times say nothing about ingestion.
    bronze_is_fresh = BashOperator(
        task_id="bronze_is_fresh",
        bash_command=spark_job(
            "quality/freshness_check.py",
            f"--path s3a://bronze/raw_orders_cdc --max-age-hours {MAX_BRONZE_AGE_HOURS}",
        ),
    )

    # No dependencies between them, on purpose: each answers a different question, and one
    # failing must not hide the state of the others. Declaring the tasks is enough - a bare
    # `[a, b, c]` expression here would set nothing up and ruff is right to call it useless.
    del connector_running, slot_active, bronze_is_fresh


streaming_health()
