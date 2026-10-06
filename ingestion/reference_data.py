"""Public reference data: holidays from Nager.Date, daily weather from Open-Meteo.

    make reference-data
    make reference-data YEARS=2026 CITIES_FROM_SILVER=1

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

Both APIs are free, keyless and explicitly allowed by CLAUDE.md's cloud-free rule. Neither is
a stream: this is a small batch extract, re-runnable, that overwrites its output.

WHAT THIS IS FOR

`dim_date` needs to know which days are holidays, because delivery demand on a public holiday
does not look like a Tuesday. `fact_delivery` needs weather, because "median transit time in
rain vs clear" is one of the Gold aggregates and the single most plausible external driver of
a delivery SLA breach.

CITY COORDINATES COME FROM THE DATA, NOT A HARDCODED LIST

Restaurants carry lat/lon, so the cities to fetch weather for - and where they are - are
derived from `dim_restaurant_scd2`. A hardcoded table would drift the moment the generator's
city list changed, and silently: the join would simply match nothing.

DEGRADING WHEN AN API IS UNREACHABLE

Neither table is required for the pipeline to run. If a fetch fails the job says so, writes an
empty table with the right schema, and exits 0 - so `dim_date` still builds (without holiday
flags) and `fact_delivery` still builds (with null weather, via a left join). A reference
extract that could take Gold down with it would be a poor trade for a nice-to-have column.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

BRONZE = "s3a://bronze"
SILVER = "s3a://silver"

# Row shapes, named once so the fetchers and the Spark schemas below cannot drift apart.
# typing.Tuple rather than the builtin: these are evaluated at runtime on Python 3.8.
# Row shapes are written inline in the signatures below rather than as module-level aliases.
#
# An alias like `WeatherRow = tuple[str, date, ...]` is evaluated at import time, and this
# module runs on Python 3.8, where a builtin `tuple` is not subscriptable - `TypeError: 'type'
# object is not subscriptable`, at import, before anything else can go wrong. Annotations are
# safe because `from __future__ import annotations` leaves them as strings; the alias is not
# an annotation. ruff targets py311 and will happily rewrite typing.Tuple into the builtin, so
# the only durable fix is to have no runtime-evaluated generic here at all.

HOLIDAYS = f"{BRONZE}/ref_holidays"
WEATHER = f"{BRONZE}/ref_weather"

NAGER_URL = "https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"
OPEN_METEO_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude={lat}&longitude={lon}&start_date={start}&end_date={end}"
    "&daily=precipitation_sum,temperature_2m_max,temperature_2m_min&timezone=UTC"
)

HOLIDAY_SCHEMA = StructType(
    [
        StructField("holiday_date", DateType()),
        StructField("holiday_name", StringType()),
        StructField("country_code", StringType()),
    ]
)

WEATHER_SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("weather_date", DateType()),
        StructField("precipitation_mm", DoubleType()),
        StructField("temp_max_c", DoubleType()),
        StructField("temp_min_c", DoubleType()),
    ]
)


def fetch_json(url: str, timeout: int = 20) -> Any:
    """Fetch and parse JSON. Returns None for HTTP 204, which is a valid empty answer."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        if response.status == 204:
            return None
        return json.load(response)


def fetch_holidays(years: list[int], country: str) -> list[tuple[date, str, str]]:
    """Public holidays from Nager.Date.

    **Nager.Date does not cover India** - it is absent from /AvailableCountries, and
    /PublicHolidays/<year>/IN answers 204 No Content rather than an error. That is not a
    failure to retry or report as broken; it is the API saying it has nothing. The Indian
    national holidays come from the `india_holidays` dbt seed instead, and this stays in place
    because it works for every country Nager does cover (verified: US 2026 returns 200).
    """
    rows = []
    for year in years:
        url = NAGER_URL.format(year=year, country=country)
        try:
            payload = fetch_json(url)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print(f"holidays {year}: unreachable ({exc}); continuing without them")
            continue

        if not payload:
            print(
                f"holidays {year}: Nager.Date has no data for {country} (HTTP 204). "
                "dim_date falls back to the india_holidays seed."
            )
            continue

        for item in payload:
            rows.append(
                (
                    datetime.strptime(item["date"], "%Y-%m-%d").date(),
                    item.get("localName") or item.get("name"),
                    country,
                )
            )
        print(f"holidays {year}: {len(payload)} day(s)")
    return rows


def fetch_weather(
    cities: list[tuple[str, float, float]], start: date, end: date
) -> list[tuple[str, date, float | None, float | None, float | None]]:
    rows = []
    for city, lat, lon in cities:
        url = OPEN_METEO_URL.format(lat=lat, lon=lon, start=start, end=end)
        try:
            payload = fetch_json(url)
            daily = payload["daily"]
        except (urllib.error.URLError, OSError, ValueError, KeyError) as exc:
            print(f"weather {city}: unreachable ({exc}); continuing without it")
            continue

        for i, day in enumerate(daily["time"]):
            rows.append(
                (
                    city,
                    datetime.strptime(day, "%Y-%m-%d").date(),
                    _at(daily, "precipitation_sum", i),
                    _at(daily, "temperature_2m_max", i),
                    _at(daily, "temperature_2m_min", i),
                )
            )
        print(f"weather {city}: {len(daily['time'])} day(s)")
    return rows


def _at(daily: dict[str, Any], key: str, index: int) -> float | None:
    values = daily.get(key) or []
    return values[index] if index < len(values) else None


def cities_from_silver(spark: SparkSession) -> list[tuple[str, float, float]]:
    """Distinct cities and their centroid, taken from the restaurants that are there."""
    restaurants = spark.read.format("delta").load(f"{SILVER}/dim_restaurant_scd2")
    rows = (
        restaurants.filter(F.col("is_current"))
        .groupBy("city")
        .agg(F.avg("lat").alias("lat"), F.avg("lon").alias("lon"))
        .orderBy("city")
        .collect()
    )
    return [(r["city"], float(r["lat"]), float(r["lon"])) for r in rows]


def order_date_range(spark: SparkSession) -> tuple[date, date]:
    """The span the fact table actually covers, widened a day each way for timezone edges."""
    fact = spark.read.format("delta").load(f"{SILVER}/fact_order_state")
    bounds = fact.agg(
        F.min(F.to_date("placed_ts")).alias("lo"), F.max(F.to_date("placed_ts")).alias("hi")
    ).head()
    lo = bounds["lo"] or date.today() - timedelta(days=7)
    hi = bounds["hi"] or date.today()
    return lo - timedelta(days=1), hi + timedelta(days=1)


def write(spark: SparkSession, rows: list[Any], schema: StructType, path: str) -> DataFrame:
    """Overwrite, not append: this is a re-runnable extract, not an event stream."""
    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch holiday and weather reference data.")
    parser.add_argument("--country", default="IN", help="ISO country for holidays (default: IN)")
    parser.add_argument("--years", default="", help="comma-separated; default: years in the data")
    parser.add_argument(
        "--print-cities",
        action="store_true",
        help="print the cities in scope as 'CITY <name>' lines and exit, fetching nothing",
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("reference-data").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Used by the api_extracts DAG to build its mapped tasks. It lives here rather than as a
    # script embedded in the DAG so there is one definition of "which cities exist", and so
    # it is covered by the same tests as the rest of this module.
    if args.print_cities:
        for city, _lat, _lon in cities_from_silver(spark):
            print(f"CITY {city}")
        spark.stop()
        return 0

    start, end = order_date_range(spark)
    years = (
        [int(y) for y in args.years.split(",") if y]
        if args.years
        else sorted({start.year, end.year})
    )
    print(f"date range: {start} .. {end}  years: {years}")

    holidays = fetch_holidays(years, args.country)
    write(spark, holidays, HOLIDAY_SCHEMA, HOLIDAYS)
    print(f"wrote {len(holidays)} holiday row(s) -> {HOLIDAYS}")

    cities = cities_from_silver(spark)
    print(f"cities from dim_restaurant_scd2: {[c[0] for c in cities]}")
    weather = fetch_weather(cities, start, end)
    write(spark, weather, WEATHER_SCHEMA, WEATHER)
    print(f"wrote {len(weather)} weather row(s) -> {WEATHER}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
