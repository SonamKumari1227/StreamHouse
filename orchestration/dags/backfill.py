"""backfill - rebuild one day of Gold, and prove the rebuild is identical.

    Trigger with a config:  {"backfill_date": "2026-09-28"}

THE CORRECTNESS BAR

`orchestration/README.md` states it: *delete a day of Gold, trigger the backfill, get
bit-identical results.* This DAG does not merely perform a backfill - it demonstrates the
property, in five steps, and fails if it does not hold:

    checksum_before  ->  destroy_the_day  ->  rebuild  ->  checksum_after  ->  compare

Deleting the day on purpose is the point. A backfill that only ever runs over data already
present proves nothing; the question is whether the pipeline can reconstruct a day it has
lost, exactly, from upstream.

WHY IT CAN HOLD

`agg_sla_daily` is incremental with `insert_overwrite` over `order_date`, so a run scoped to
one date replaces exactly that partition. Every column in it is a pure aggregate of the fact
rows for that day: nothing reads the clock, nothing depends on arrival order. Those two
properties are what make "bit-identical" a guarantee rather than a coincidence.

Manual-trigger only. A schedule would be wrong for an operation whose first act is to delete
data.
"""

from __future__ import annotations

from typing import Any

import pendulum
from airflow.sdk import dag, task
from streamhouse_common import DEFAULT_ARGS, REPO, SPARK_CONTAINER, SPARK_MASTER, SPARK_SUBMIT, dbt

TABLE = "agg_sla_daily"


def _run(command: str) -> str:
    import subprocess

    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {result.stderr[-2000:]}")
    return result.stdout


def _backfill_tool(action: str, day: str) -> str:
    return (
        f"docker exec -e PYTHONPATH={REPO} {SPARK_CONTAINER} {SPARK_SUBMIT}"
        f" --master {SPARK_MASTER} --conf spark.cores.max=1"
        f" --conf spark.executorEnv.PYTHONPATH={REPO}"
        f" {REPO}/quality/backfill_check.py"
        f" --table {TABLE} --date {day} --action {action}"
    )


@dag(
    dag_id="backfill",
    description="Rebuild one day of Gold and assert the result is bit-identical.",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"backfill_date": "2026-09-28"},
    tags=["streamhouse", "backfill", "phase-5"],
)
def backfill() -> None:
    @task
    def target_date(**context: Any) -> str:
        day: str = context["params"]["backfill_date"]
        print(f"backfilling {TABLE} for {day}")
        return day

    @task
    def checksum_before(day: str) -> str:
        """What the day looks like now. Everything after this is measured against it."""
        line = next(
            line
            for line in _run(_backfill_tool("checksum", day)).splitlines()
            if line.startswith("CHECKSUM")
        )
        print(line)
        return line

    @task
    def destroy_the_day(day: str, _before: str) -> str:
        """Delete the partition, so the rebuild has to genuinely reconstruct it."""
        print(_run(_backfill_tool("delete", day)).strip().splitlines()[-1])
        return day

    @task
    def rebuild(day: str) -> str:
        """Re-run only this model, scoped to this one date.

        `--select` keeps it to the aggregate; the var scopes the incremental filter. Together
        they mean no other day's partition is even read, let alone rewritten.
        """
        command = dbt(f'run --select {TABLE} --vars \'{{"backfill_date": "{day}"}}\'')
        output = _run(command)
        print("\n".join(output.splitlines()[-12:]))
        return day

    @task
    def checksum_after(day: str) -> str:
        line = next(
            line
            for line in _run(_backfill_tool("checksum", day)).splitlines()
            if line.startswith("CHECKSUM")
        )
        print(line)
        return line

    @task
    def compare(before: str, after: str) -> str:
        """The assertion the whole DAG exists for."""
        print(f"before: {before}")
        print(f"after : {after}")
        if before != after:
            raise AssertionError(
                "backfill is NOT idempotent - the rebuilt day differs from the original.\n"
                f"  before: {before}\n  after : {after}"
            )
        verdict = f"bit-identical: {after}"
        print(verdict)
        return verdict

    day = target_date()
    before = checksum_before(day)
    destroyed = destroy_the_day(day, before)
    rebuilt = rebuild(destroyed)
    after = checksum_after(rebuilt)
    compare(before, after)


backfill()
