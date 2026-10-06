"""Does one city have weather in the reference extract?

    make weather-coverage CITY=Bengaluru

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

Exits 1 when a city has fewer than `--min-days` rows, which makes it usable as a mapped
Airflow task: one task per city, and the failure names the city.

WHY A MISSING CITY NEEDS LOOKING FOR

`dim_weather` left-joins onto the fact, so a city with no weather rows does not error - every
order in that city simply gets a null `weather_sk`, and the weather aggregates quietly exclude
it. That is a gap that can survive indefinitely, because nothing downstream is shaped to
notice it. This is the thing that notices.
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WEATHER = "s3a://bronze/ref_weather"


def days_for_city(spark: SparkSession, city: str, path: str = WEATHER) -> int:
    rows = spark.read.format("delta").load(path).filter(F.col("city") == city)
    return int(rows.count())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check one city's weather coverage.")
    parser.add_argument("--city", required=True)
    parser.add_argument("--min-days", type=int, default=1)
    parser.add_argument("--path", default=WEATHER)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName(f"weather-coverage-{args.city}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    days = days_for_city(spark, args.city, args.path)
    # One parseable line, so the DAG task does not have to scrape prose.
    print(f"COVERAGE city={args.city} days={days} min={args.min_days}")
    spark.stop()

    if days < args.min_days:
        print(f"MISSING: {args.city} has {days} day(s) of weather, expected >= {args.min_days}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
