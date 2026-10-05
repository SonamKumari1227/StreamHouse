"""Expectation suites gating the Bronze -> Silver boundary.

PYTHON 3.8 - runs via spark-submit inside the Spark image. See ADR-0009.

An expectation is a predicate that must hold for every row. A suite is a named set of them.
Validation splits a DataFrame into what passed and what did not, and the failures carry the
names of the expectations they broke - so the quarantine says *why*, not merely *that*.

**Failures quarantine, they do not crash.** A job that dies on one bad row stops the pipeline
for everyone and loses the rows it had already handled; one that drops bad rows silently lies
about its own completeness. Routing them to a Delta table with their violations attached is
what makes a quality metric possible at all in Phase 6.

This is deliberately not Great Expectations - see ADR-0010. The shape here (suite, validation
result, quarantine) is GE's, so the swap stays open.
"""

from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

QUARANTINE = "s3a://quarantine"

ORDER_STATUSES = ("PLACED", "ACCEPTED", "PICKED_UP", "DELIVERED", "CANCELLED")

# Money is decimal(10,2), so equality has to tolerate the last paisa rather than demand exact
# arithmetic across a sum of rounded components.
PAISA = 0.01


class Expectation(NamedTuple):
    """A predicate that must be TRUE for every row, and a name for when it is not."""

    name: str
    predicate: Column

    def violated(self) -> Column:
        """True when this row breaks the expectation.

        A null predicate counts as a violation. SQL's three-valued logic would otherwise let a
        row with a null in the wrong place pass a check it never actually satisfied - which is
        exactly the row worth catching.
        """
        return ~F.coalesce(self.predicate, F.lit(False))


def fact_order_state_suite() -> tuple[Expectation, ...]:
    """What has to be true of an order once it reaches Silver."""
    total = F.col("subtotal_inr") + F.col("delivery_fee_inr") - F.col("discount_inr")
    terminal_with_rider = F.col("status").isin("PICKED_UP", "DELIVERED")
    return (
        Expectation("order_id_present", F.col("order_id").isNotNull()),
        Expectation("status_is_known", F.col("status").isin(*ORDER_STATUSES)),
        Expectation("placed_ts_present", F.col("placed_ts").isNotNull()),
        Expectation("total_not_negative", F.col("total_inr") >= 0),
        # The invariant the generator's own state machine maintains; if it ever breaks, either
        # the source or the parsing is wrong, and both are worth stopping for.
        Expectation("total_reconciles", F.abs(F.col("total_inr") - total) <= PAISA),
        Expectation(
            "promised_after_placed",
            F.col("promised_ts") >= F.col("placed_ts"),
        ),
        # Null delivered_ts is fine - most orders are not delivered yet. Delivered *before*
        # placed is not.
        Expectation(
            "delivered_after_placed",
            F.col("delivered_ts").isNull() | (F.col("delivered_ts") >= F.col("placed_ts")),
        ),
        Expectation(
            "rider_assigned_once_picked_up",
            ~terminal_with_rider | F.col("rider_id").isNotNull(),
        ),
    )


def scd2_suite(key: str) -> tuple[Expectation, ...]:
    """What has to be true of any SCD2 dimension, whatever it is a dimension of."""
    return (
        Expectation("key_present", F.col(key).isNotNull()),
        Expectation("valid_from_present", F.col("valid_from").isNotNull()),
        # A window that closes before it opens is not a window.
        Expectation(
            "window_is_forward",
            F.col("valid_to").isNull() | (F.col("valid_to") > F.col("valid_from")),
        ),
        # The open version is the one with no end, and only that one.
        Expectation(
            "current_is_open_ended",
            ~F.col("is_current") | F.col("valid_to").isNull(),
        ),
        Expectation(
            "closed_is_not_current",
            F.col("valid_to").isNull() | ~F.col("is_current"),
        ),
    )


def gps_trips_suite() -> tuple[Expectation, ...]:
    """What has to be true of a sessionized trip."""
    return (
        Expectation("trip_id_present", F.col("trip_id").isNotNull()),
        Expectation("rider_id_present", F.col("rider_id").isNotNull()),
        Expectation("has_pings", F.col("ping_count") > 0),
        Expectation("duration_not_negative", F.col("duration_s") >= 0),
        Expectation("ends_after_it_starts", F.col("ended_ts") >= F.col("started_ts")),
        Expectation("avg_speed_within_max", F.col("avg_speed_kmph") <= F.col("max_speed_kmph")),
        # A delivery rider has not crossed a country. This catches a bad lat/lon far more
        # cheaply than any downstream dashboard would.
        Expectation("distance_is_plausible", F.col("straight_line_km") < 500),
    )


SUITES = {
    "fact_order_state": fact_order_state_suite,
    "gps_trips_sessionized": gps_trips_suite,
}


class ValidationResult(NamedTuple):
    passed: DataFrame
    failed: DataFrame
    """Failing rows, with a `violations` array naming every expectation they broke."""


def validate(df: DataFrame, suite: tuple[Expectation, ...]) -> ValidationResult:
    """Split a DataFrame into rows that meet every expectation and rows that do not.

    Every expectation is evaluated for every row rather than stopping at the first failure:
    one row breaking four rules is far more informative than learning it broke one.
    """
    violations = F.array_compact(F.array(*[F.when(e.violated(), F.lit(e.name)) for e in suite]))
    marked = df.withColumn("violations", violations)
    return ValidationResult(
        passed=marked.filter(F.size("violations") == 0).drop("violations"),
        failed=marked.filter(F.size("violations") > 0),
    )


def quarantine_path(table: str) -> str:
    return f"{QUARANTINE}/silver_{table}"


def write_failures(failed: DataFrame, table: str) -> int:
    """Append failing rows to the table's quarantine, with the reason attached."""
    count: int = failed.count()
    if count:
        (
            failed.withColumn("quarantined_at", F.current_timestamp())
            .withColumn("source_table", F.lit(table))
            .write.format("delta")
            .mode("append")
            .save(quarantine_path(table))
        )
    return count
