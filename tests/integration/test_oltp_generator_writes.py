"""The OLTP generator against a real PostgreSQL.

The unit tests prove the scheduling and the money arithmetic against a fake repository. They
cannot prove that the SQL parses, that foreign keys hold, that CHECK constraints accept what
the generator produces, or that NUMERIC columns keep the values written to them. That is what
this file is for.

Everything runs in a transaction that is rolled back, so the developer stack is left as found.

Run with:  pytest -m integration
Needs the core stack up:  make up
"""

from __future__ import annotations

import os
import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from generator.config import LoadConfig, Range, SeedVolumes
from generator.oltp_generator import ManualClock, OltpGenerator
from generator.repository import PostgresRepository
from generator.seed import build_reference_data, seed_database
from generator.state_machine import MachineConfig

psycopg = pytest.importorskip("psycopg", reason="psycopg is not installed")

pytestmark = pytest.mark.integration

T0 = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)

SMALL = SeedVolumes(
    customers=40,
    restaurants=12,
    riders=15,
    menu_items_per_restaurant=Range(low=4.0, high=8.0, mode=6.0),
)

ALL_TABLES = (
    "order_items",
    "payments",
    "orders",
    "menu_items",
    "restaurants",
    "customers",
    "riders",
)


def dsn() -> str:
    if override := os.environ.get("SH_TEST_DSN"):
        return override
    return (
        f"host={os.environ.get('SH_POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('SH_POSTGRES_PORT', '5432')} "
        f"user={os.environ.get('SH_POSTGRES_USER', 'streamhouse')} "
        f"password={os.environ.get('SH_POSTGRES_PASSWORD', 'streamhouse_local_dev')} "
        f"dbname={os.environ.get('SH_POSTGRES_DB', 'streamhouse')}"
    )


@pytest.fixture
def seeded_cursor() -> Iterator[Any]:
    """A seeded database inside a transaction that is always rolled back."""
    try:
        conn = psycopg.connect(dsn(), connect_timeout=5)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL is not reachable ({exc.__class__.__name__}); run `make up`")

    with conn:
        with conn.cursor() as cur:
            for table in ALL_TABLES:
                cur.execute(f"DELETE FROM {table}")
            seed_database(cur, build_reference_data(SMALL, seed=7, now=T0))
            yield cur
        conn.rollback()
    conn.close()


def build_generator(cur: Any, seed: int = 4242, orders_per_day: int = 50_000) -> OltpGenerator:
    repo = PostgresRepository(cur)
    return OltpGenerator(
        repository=repo,
        load=LoadConfig(orders_per_day=orders_per_day, speed=20.0),
        machine=MachineConfig(speed=20.0),
        rng=random.Random(seed),
        clock=ManualClock(T0),
    )


def scalar(cur: Any, sql: str, params: tuple[Any, ...] = ()) -> Any:
    cur.execute(sql, params)
    return cur.fetchone()[0]


# --------------------------------------------------------------------------- catalog


def test_catalog_loads_from_the_seeded_database(seeded_cursor: Any) -> None:
    catalog = PostgresRepository(seeded_cursor).load_catalog()
    assert len(catalog.restaurants) == SMALL.restaurants
    assert len(catalog.rider_ids) == SMALL.riders
    assert 0 < len(catalog.customer_ids) <= SMALL.customers  # only active customers
    assert sum(len(v) for v in catalog.menu_by_restaurant.values()) > 0


# --------------------------------------------------------------------------- writes land


def test_orders_items_and_payments_actually_land(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor)
    report = gen.run(max_orders=25)

    assert scalar(seeded_cursor, "SELECT count(*) FROM orders") == report.orders_placed == 25
    assert scalar(seeded_cursor, "SELECT count(*) FROM order_items") >= 25
    assert scalar(seeded_cursor, "SELECT count(*) FROM payments") == 25


def test_every_order_ends_in_a_terminal_state(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=40)

    assert (
        scalar(
            seeded_cursor,
            "SELECT count(*) FROM orders WHERE status NOT IN ('DELIVERED', 'CANCELLED')",
        )
        == 0
    )
    assert scalar(seeded_cursor, "SELECT count(*) FROM orders WHERE status = 'DELIVERED'") > 0


def test_every_order_carries_its_payment_id(seeded_cursor: Any) -> None:
    """The second UPDATE at placement must actually happen."""
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=20)
    assert scalar(seeded_cursor, "SELECT count(*) FROM orders WHERE payment_id IS NULL") == 0


def test_referential_integrity_holds_across_every_table(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=30)

    orphans = scalar(
        seeded_cursor,
        """
        SELECT
          (SELECT count(*) FROM order_items oi
             LEFT JOIN orders o USING (order_id) WHERE o.order_id IS NULL)
        + (SELECT count(*) FROM order_items oi
             LEFT JOIN menu_items m USING (menu_item_id) WHERE m.menu_item_id IS NULL)
        + (SELECT count(*) FROM payments p
             LEFT JOIN orders o USING (order_id) WHERE o.order_id IS NULL)
        + (SELECT count(*) FROM orders o
             LEFT JOIN customers c USING (customer_id) WHERE c.customer_id IS NULL)
        + (SELECT count(*) FROM orders o
             LEFT JOIN restaurants r USING (restaurant_id) WHERE r.restaurant_id IS NULL)
        """,
    )
    assert orphans == 0


# --------------------------------------------------------------------------- data quality


def test_delivered_orders_have_the_full_timestamp_chain(seeded_cursor: Any) -> None:
    """The dbt singular test from the spec, asserted at the source."""
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=40)

    assert (
        scalar(
            seeded_cursor,
            """
        SELECT count(*) FROM orders
        WHERE status = 'DELIVERED'
          AND (accepted_ts IS NULL OR picked_up_ts IS NULL OR delivered_ts IS NULL)
        """,
        )
        == 0
    )


def test_no_order_has_out_of_sequence_timestamps(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=40)

    assert (
        scalar(
            seeded_cursor,
            """
        SELECT count(*) FROM orders
        WHERE (accepted_ts  IS NOT NULL AND accepted_ts  < placed_ts)
           OR (picked_up_ts IS NOT NULL AND picked_up_ts < accepted_ts)
           OR (delivered_ts IS NOT NULL AND delivered_ts < picked_up_ts)
        """,
        )
        == 0
    )


def test_money_is_consistent_in_the_database(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=30)

    mismatched = scalar(
        seeded_cursor,
        """
        SELECT count(*) FROM (
            SELECT o.order_id, o.subtotal_inr,
                   SUM(oi.qty * oi.unit_price_inr) AS computed
            FROM orders o JOIN order_items oi USING (order_id)
            GROUP BY o.order_id, o.subtotal_inr
        ) t WHERE abs(t.subtotal_inr - t.computed) > 0.01
        """,
    )
    assert mismatched == 0
    assert scalar(seeded_cursor, "SELECT count(*) FROM orders WHERE total_inr < 0") == 0


def test_cancelled_orders_have_a_reason_and_a_settled_payment(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=40)

    assert (
        scalar(
            seeded_cursor,
            "SELECT count(*) FROM orders WHERE status = 'CANCELLED' AND cancel_reason IS NULL",
        )
        == 0
    )
    assert (
        scalar(
            seeded_cursor,
            """
        SELECT count(*) FROM orders o JOIN payments p USING (order_id)
        WHERE o.status = 'CANCELLED' AND p.status NOT IN ('REFUNDED', 'FAILED')
        """,
        )
        == 0
    )
    assert (
        scalar(
            seeded_cursor,
            """
        SELECT count(*) FROM orders o JOIN payments p USING (order_id)
        WHERE o.status = 'DELIVERED' AND p.status <> 'CAPTURED'
        """,
        )
        == 0
    )


def test_riders_are_assigned_to_orders_that_got_accepted(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor)
    gen.run(max_orders=40)

    assert (
        scalar(
            seeded_cursor,
            "SELECT count(*) FROM orders WHERE accepted_ts IS NOT NULL AND rider_id IS NULL",
        )
        == 0
    )


# --------------------------------------------------------------------------- CDC readiness


def test_orders_are_updated_several_times_each(seeded_cursor: Any) -> None:
    """Phase 2 needs multiple CDC events per order, not one insert each."""
    gen = build_generator(seeded_cursor)
    report = gen.run(max_orders=25)
    # 1 insert + 1 payment_id update + one update per transition.
    assert report.transitions >= report.orders_placed
    assert scalar(seeded_cursor, "SELECT count(*) FROM orders WHERE updated_at > placed_ts") == 25


def test_dimension_rows_are_mutated_for_scd2(seeded_cursor: Any) -> None:
    """Phase 3's SCD2 dimensions need the reference tables to actually change.

    Driven by a deadline, not an order count. Mutations fire on simulated time, so a high
    arrival rate finishes the order budget *before* the first mutation is due — which is
    exactly how the first version of this test failed.
    """
    gen = build_generator(seeded_cursor, orders_per_day=8_640)  # 2/s at 20x
    report = gen.run(until=T0 + timedelta(seconds=90))

    assert report.total_mutations > 0
    changed = scalar(
        seeded_cursor,
        """
        SELECT (SELECT count(*) FROM menu_items  WHERE updated_at > %s)
             + (SELECT count(*) FROM restaurants WHERE updated_at > %s)
             + (SELECT count(*) FROM riders      WHERE updated_at > %s)
        """,
        (T0, T0, T0),
    )
    assert changed > 0, "no dimension row was mutated; SCD2 would have nothing to track"


# --------------------------------------------------------------------------- recovery


def test_recovery_picks_up_orders_left_in_flight(seeded_cursor: Any) -> None:
    """Stop mid-flight, then prove a fresh generator drives the survivors to terminal."""
    first = build_generator(seeded_cursor, seed=11)
    first.run(max_events=60)
    in_flight = scalar(
        seeded_cursor,
        "SELECT count(*) FROM orders WHERE status NOT IN ('DELIVERED', 'CANCELLED')",
    )
    assert in_flight > 0, "test needs orders still in flight to be meaningful"

    second = build_generator(seeded_cursor, seed=12)
    assert second.recover() == in_flight
    second.run(max_orders=1)

    assert (
        scalar(
            seeded_cursor,
            "SELECT count(*) FROM orders WHERE status NOT IN ('DELIVERED', 'CANCELLED')",
        )
        == 0
    )


# --------------------------------------------------------------------------- safety


def test_advance_order_refuses_an_unknown_timestamp_column(seeded_cursor: Any) -> None:
    """The column name is interpolated into SQL, so it is validated against an allowlist."""
    repo = PostgresRepository(seeded_cursor)
    with pytest.raises(ValueError, match="refusing to stamp unknown column"):
        repo.advance_order(
            order_id=1,
            status="ACCEPTED",
            stamp_field="updated_at; DROP TABLE orders",
            stamp_ts=T0,
            rider_id=None,
            cancel_reason=None,
            updated_at=T0,
        )


def test_a_longer_run_stays_consistent(seeded_cursor: Any) -> None:
    gen = build_generator(seeded_cursor, seed=99, orders_per_day=8_640)
    report = gen.run(until=T0 + timedelta(seconds=45))

    assert report.orders_placed > 10
    assert scalar(seeded_cursor, "SELECT count(*) FROM orders") == report.orders_placed
    assert scalar(seeded_cursor, "SELECT count(*) FROM payments") == report.orders_placed
    assert scalar(seeded_cursor, "SELECT count(DISTINCT status) FROM orders") >= 1
