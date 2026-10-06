"""The holiday and weather extract.

Run with `make test-spark`.

The HTTP calls are not tested - a test that needs Nager.Date to be up is a test that fails for
reasons having nothing to do with this repository. What is tested is everything around them:
the schemas the models depend on, and the behaviour when an API returns nothing, which is not
hypothetical - Nager.Date genuinely has no data for India and answers 204.

PYTHON 3.8 - this runs inside the Spark image. See ADR-0009.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from typing import Any

import pytest
from pyspark.sql import SparkSession

from ingestion.reference_data import (
    HOLIDAY_SCHEMA,
    WEATHER_SCHEMA,
    _at,
    write,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("reference-data-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# --------------------------------------------------------------------------- _at


def test_at_reads_the_requested_index() -> None:
    daily = {"precipitation_sum": [0.0, 4.2, 11.5]}
    assert _at(daily, "precipitation_sum", 1) == 4.2


def test_at_returns_none_for_a_missing_series() -> None:
    """Open-Meteo omits a series entirely when it has no data for it, rather than nulling it."""
    assert _at({"time": ["2026-10-01"]}, "precipitation_sum", 0) is None


def test_at_returns_none_past_the_end() -> None:
    """A series shorter than `time` would otherwise raise IndexError mid-extract."""
    assert _at({"precipitation_sum": [1.0]}, "precipitation_sum", 5) is None


def test_at_preserves_a_genuine_null() -> None:
    """None inside the series means "no reading", which must not become zero: zero is a claim
    that it did not rain, and dim_weather would report CLEAR on a day nobody measured."""
    assert _at({"precipitation_sum": [None, 1.0]}, "precipitation_sum", 0) is None


# --------------------------------------------------------------------------- empty extracts


def test_an_empty_holiday_extract_still_writes_a_usable_table(
    spark: SparkSession, tmp_path: Any
) -> None:
    """The India case, and the whole reason dim_date left-joins.

    Nager.Date has no data for IN and answers 204. The extract must still produce a table with
    the right schema, or every dbt run would fail on a missing relation rather than simply
    reporting no holidays.
    """
    path = str(tmp_path / "ref_holidays")
    write(spark, [], HOLIDAY_SCHEMA, path)

    df = spark.read.format("delta").load(path)
    assert df.count() == 0
    assert df.columns == ["holiday_date", "holiday_name", "country_code"]


def test_an_empty_weather_extract_still_writes_a_usable_table(
    spark: SparkSession, tmp_path: Any
) -> None:
    path = str(tmp_path / "ref_weather")
    write(spark, [], WEATHER_SCHEMA, path)

    df = spark.read.format("delta").load(path)
    assert df.count() == 0
    assert "precipitation_mm" in df.columns


def test_the_extract_overwrites_rather_than_appends(spark: SparkSession, tmp_path: Any) -> None:
    """Re-running must not double the reference data.

    This is a re-runnable batch extract, not an event stream: the same day fetched twice is
    the same fact, and appending it would quietly double every weather join downstream.
    """
    path = str(tmp_path / "ref_weather")
    rows = [("Bengaluru", date(2026, 10, 1), 4.2, 29.0, 21.0)]
    write(spark, rows, WEATHER_SCHEMA, path)
    write(spark, rows, WEATHER_SCHEMA, path)

    assert spark.read.format("delta").load(path).count() == 1


def test_weather_rows_keep_their_types(spark: SparkSession, tmp_path: Any) -> None:
    path = str(tmp_path / "ref_weather")
    write(spark, [("Pune", date(2026, 10, 2), 0.0, 31.5, 22.1)], WEATHER_SCHEMA, path)

    row = spark.read.format("delta").load(path).head()
    assert row["city"] == "Pune"
    assert row["weather_date"] == date(2026, 10, 2)
    assert row["temp_max_c"] == pytest.approx(31.5)
