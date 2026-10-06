# orchestration/

Airflow 3 DAGs. Runs in the `orchestration` Compose profile, backed by `postgres-meta`.

```bash
make airflow-up                        # start it (separate from the core stack)
make airflow-dags                      # what parsed, and any import errors
make airflow-trigger DAG=silver_to_gold
make backfill BACKFILL_DATE=2026-09-28 # the correctness bar, below
make airflow-logs
```

UI at <http://localhost:8088>. **No login** — `SIMPLE_AUTH_MANAGER_ALL_ADMINS` is on, which is
right for a single-user laptop stack and nothing a shared one should copy. Airflow 3 removed
`airflow users create`; user management belongs to the auth manager now.

Airflow's metadata lives in **its own Postgres** (`postgres-meta`), not the OLTP one. Debezium
is replicating that database, and pointing Airflow at it would push thousands of task-instance
rows through the CDC pipeline — a scheduler writing into the system it schedules.

| DAG | Schedule | Purpose | Verified 2026-10-06 |
| --- | --- | --- | --- |
| `streaming_health` | every 15 min | Connector state, replication slot, Bronze freshness. | ✅ 3/3 tasks |
| `silver_to_gold` | hourly | Silver → quality gate → `dbt build` → docs → maintenance. | ✅ 14/14 tasks |
| `api_extracts` | daily 02:30 | Nager.Date + Open-Meteo, per-city checks mapped over a runtime list. | ✅ 10/10 tasks |
| `backfill` | manual | Rebuild one day of Gold and **prove** it is bit-identical. | ✅ 6/6 tasks |

All four were run to completion, not merely parsed.

## How a DAG starts Spark work

Airflow does not run Spark. It runs `docker exec` against the Spark container, issuing exactly
the command the Makefile issues — so a red task is a command you can paste into a terminal,
and there is one definition of how a job launches rather than two that drift apart. The cost
is the Docker socket, mounted into the Airflow containers; the trade-off is argued in
[ADR-0011](../docs/decisions/0011-airflow-drives-spark-through-the-docker-socket.md).

The command builders live in `dags/streamhouse_common.py`, written once rather than in four
DAGs.

**No Python is embedded in a DAG.** Every check calls a real script in the repo —
`quality/freshness_check.py`, `quality/weather_coverage.py`,
`ingestion/reference_data.py --print-cities`. An earlier version inlined a Spark script and
piped it to `spark-submit /dev/stdin`; it silently produced nothing and the task failed in two
seconds with an empty stdout and no usable error. Scripts in the repo are testable, runnable
by hand, and fail legibly.

## The correctness bar

> Delete a day of Gold, trigger the backfill, get bit-identical results. That property is what
> separates a real pipeline from a demo.

The `backfill` DAG does not merely perform a backfill — it demonstrates the property and fails
if it does not hold:

```
checksum_before -> destroy_the_day -> rebuild -> checksum_after -> compare
```

Deleting the day on purpose is the point: a backfill that only ever runs over data already
present proves nothing. **Proven 2026-10-06**, all six tasks green:

```
before : CHECKSUM table=agg_sla_daily date=2026-09-28 rows=80 hash=153839121457
DELETED agg_sla_daily 2026-09-28; rows now: 0
rebuild: OK created sql incremental model gold.agg_sla_daily
after  : CHECKSUM table=agg_sla_daily date=2026-09-28 rows=80 hash=153839121457
```

It can hold because `agg_sla_daily` is incremental with `insert_overwrite` over `order_date`,
so a run scoped to one date replaces exactly that partition and leaves every other day
untouched; and because every column in it is a pure aggregate of that day's fact rows —
nothing reads the clock, nothing depends on arrival order. The checksum is over row *content*,
summed so it is independent of row order, because "the same number of rows" is a test a wrong
rebuild passes easily.

## Concurrency is bounded by the cluster, not by Airflow

Every task here is a `spark-submit`, and every `spark-submit` is a JVM driver holding a few
hundred MB. The cluster has **2 cores and each job pins 1** (`spark.cores.max`), so a third
concurrent task cannot execute anyway — it queues *inside Spark* while still holding its
driver's memory.

The first `api_extracts` run expanded to eight concurrent drivers and took the WSL VM from
comfortable to 106 MiB available, wedging the Docker daemon. Both DAGs now carry
`max_active_tasks=2`, and the mapped task `max_active_tis_per_dag=2`. Verified afterwards: all
eight mapped tasks ran two at a time and memory never dropped below 1 GB free.

**Do not run `make test-spark` while the full stack and Airflow are up.** The core stack,
Airflow, mapped drivers and the test suite's own local Spark sessions do not fit in 9.7 GB
together.

## Dynamic task mapping

Two kinds, both load-bearing rather than decorative:

- **Over a static list** — `silver_to_gold` maps the three dimension builds and the five
  quality gates. The gate list comes from the same constant the gate module uses, so adding a
  Silver table adds its gate automatically instead of silently going unchecked.
- **Over a runtime list** — `api_extracts` reads the cities from `dim_restaurant_scd2` during
  the run and expands one verification task per city. The number of mapped tasks is not known
  when the DAG is parsed, which is the form worth demonstrating.

## SLAs: removed in Airflow 3

Airflow 3.0 **removed** the `sla` parameter and the SLA-miss callback; the replacement
(deadline alerts, AIP-86) is not in 3.0.1. Lateness is enforced instead with
`execution_timeout`, which fails a task that overruns, plus `on_failure_callback` to record
it. That is narrower than SLA callbacks were — it fires on overrun, not on a task that never
started — so `streaming_health` covers the second case directly, by asking whether data is
still arriving rather than whether a task is still running.

`on_failure_callback` currently logs. There is no alerting backend until Phase 6, and a
callback that pretends to notify someone is worse than one that plainly does not.
