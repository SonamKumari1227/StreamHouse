"""api_extracts - the daily public-API pulls, verified city by city.

    Nager.Date (holidays) + Open-Meteo (weather)  ->  Bronze  ->  per-city coverage check

DYNAMIC TASK MAPPING OVER CITIES

The city list is not hardcoded anywhere in this DAG. It is read at runtime from
`dim_restaurant_scd2` - restaurants carry lat/lon, so the cities that exist are the cities
restaurants are in - and the verification task is `.expand()`ed over whatever comes back. Add
a city to the generator and a check for it appears on the next run; a hardcoded list would
instead keep passing while silently covering nothing.

This is mapping over a list computed *during* the run, which is the form worth demonstrating:
the number of mapped tasks is not known when the DAG is parsed.

WHY THE FETCH ITSELF IS ONE TASK

Each city is a single HTTP request taking milliseconds, while each mapped task would be a
separate spark-submit costing ~20 seconds of JVM startup. Mapping the fetch would make the DAG
eight times slower to prove nothing. The verification is mapped because that is where per-city
failure is meaningful: one city missing weather is a real, isolable problem.

NO PYTHON EMBEDDED IN THIS FILE

Both helpers call real scripts in the repo. An earlier version inlined a Spark script and
piped it to `spark-submit /dev/stdin`, which silently produced nothing: the task failed in two
seconds with an empty stdout and no useful error. Scripts in the repo are testable, runnable
by hand, and fail legibly.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from streamhouse_common import DEFAULT_ARGS, spark_job


@dag(
    dag_id="api_extracts",
    description="Daily Nager.Date and Open-Meteo pulls, with per-city coverage checks.",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Bounded for the same reason as the mapped task below: concurrency past the cluster's
    # core count buys nothing and costs a JVM each.
    max_active_tasks=2,
    default_args=DEFAULT_ARGS,
    tags=["streamhouse", "reference", "phase-5"],
)
def api_extracts() -> None:
    fetch = BashOperator(
        task_id="fetch_reference_data",
        bash_command=spark_job("ingestion/reference_data.py"),
    )

    @task(task_id="cities_in_scope")
    def cities_in_scope() -> list[str]:
        """The cities that actually exist, read from the dimension at runtime."""
        import subprocess

        command = spark_job("ingestion/reference_data.py", "--print-cities")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        cities = [
            line.split(" ", 1)[1].strip()
            for line in result.stdout.splitlines()
            if line.startswith("CITY ")
        ]
        if result.returncode != 0 or not cities:
            raise RuntimeError(
                f"could not read the city list (exit {result.returncode})\n"
                f"stdout tail:\n{result.stdout[-1500:]}\nstderr tail:\n{result.stderr[-1500:]}"
            )
        print(f"{len(cities)} cities: {cities}")
        return cities

    # max_active_tis_per_dag=2 is not a nicety. Each mapped task is a separate spark-submit,
    # and therefore a separate JVM driver holding a few hundred MB. The cluster has 2 cores and
    # every job pins 1 (spark.cores.max), so a third concurrent task cannot run anyway - it
    # queues inside Spark while still holding its driver's memory. Eight at once exhausted the
    # WSL VM (9.7 GB) and wedged the Docker daemon; two is the number the cluster can actually
    # use.
    @task(task_id="verify_city_weather", max_active_tis_per_dag=2)
    def verify_city_weather(city: str) -> str:
        """One city's weather coverage. Mapped over whatever cities_in_scope returned.

        A city with no rows is reported, not tolerated: `dim_weather` left-joins, so a missing
        city produces null weather on every one of its orders rather than an error - the kind
        of gap that survives indefinitely unless something looks for it.
        """
        import subprocess

        command = spark_job("quality/weather_coverage.py", f'--city "{city}" --min-days 1')
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        line = next(
            (ln for ln in result.stdout.splitlines() if ln.startswith("COVERAGE")),
            "COVERAGE (no output)",
        )
        print(f"{city}: {line}")
        if result.returncode != 0:
            raise RuntimeError(f"weather coverage failed for {city}: {line}")
        return line

    fetch >> verify_city_weather.expand(city=cities_in_scope())


api_extracts()
