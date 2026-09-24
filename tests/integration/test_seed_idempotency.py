"""Seeding against a real PostgreSQL.

The unit tests exercise a fake cursor that models what `ON CONFLICT (pk) DO NOTHING` is
*believed* to do. This file proves it against the actual database, along with the things a
fake cannot check at all: CHECK constraints, foreign keys, NUMERIC precision, and sequence
resynchronisation.

Everything runs inside one transaction that is rolled back, so the developer stack is left
exactly as it was found. The one exception is sequence values: `setval` is not transactional
in PostgreSQL and survives the rollback. That is harmless here — the sequence simply points
past ids that no longer exist — but it is worth knowing before it surprises you.

Run with:  pytest -m integration
Needs the core stack up:  make up
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from generator.config import Range, SeedVolumes
from generator.seed import build_reference_data, seed_database

psycopg = pytest.importorskip("psycopg", reason="psycopg is not installed")

pytestmark = pytest.mark.integration

SMALL = SeedVolumes(
    customers=30,
    restaurants=10,
    riders=12,
    menu_items_per_restaurant=Range(low=3.0, high=7.0, mode=5.0),
)

SEED_TABLES = ("menu_items", "restaurants", "customers", "riders")
#: Cleared first because they hold foreign keys into the reference tables. Safe only
#: because the whole fixture is rolled back — see the module docstring.
DEPENDENT_TABLES = ("order_items", "payments", "orders")


def dsn() -> str:
    """Connection string for the local dev stack, overridable for CI."""
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
def cursor() -> Iterator[object]:
    """A cursor in a transaction that is always rolled back.

    Clears the reference tables first so insert counts are meaningful, which is safe only
    because nothing is committed.
    """
    try:
        conn = psycopg.connect(dsn(), connect_timeout=5)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL is not reachable ({exc.__class__.__name__}); run `make up`")

    with conn:
        with conn.cursor() as cur:
            # Dependents first, then the reference tables. An earlier version skipped the
            # whole file when orders existed, which turned a generator demo run into eight
            # silently skipped tests — a skip reads as success, so it hid the coverage.
            for table in (*DEPENDENT_TABLES, *SEED_TABLES):
                cur.execute(f"DELETE FROM {table}")
            yield cur
        conn.rollback()
    conn.close()


def count(cur: object, table: str) -> int:
    cur.execute(f"SELECT count(*) FROM {table}")  # type: ignore[attr-defined]
    return int(cur.fetchone()[0])  # type: ignore[attr-defined]


def test_seeding_a_clean_database_inserts_every_row(cursor: object) -> None:
    data = build_reference_data(SMALL, seed=7)
    report = seed_database(cursor, data)  # type: ignore[arg-type]

    assert report.total_inserted == data.total_rows
    assert count(cursor, "customers") == SMALL.customers
    assert count(cursor, "restaurants") == SMALL.restaurants
    assert count(cursor, "riders") == SMALL.riders
    assert count(cursor, "menu_items") == len(data.menu_items)


def test_seeding_twice_inserts_nothing_the_second_time(cursor: object) -> None:
    """Idempotency, against the real ON CONFLICT rather than a model of it."""
    data = build_reference_data(SMALL, seed=7)

    first = seed_database(cursor, data)  # type: ignore[arg-type]
    after_first = {t: count(cursor, t) for t in SEED_TABLES}

    second = seed_database(cursor, data)  # type: ignore[arg-type]
    after_second = {t: count(cursor, t) for t in SEED_TABLES}

    assert first.total_inserted == data.total_rows
    assert second.total_inserted == 0
    assert second.was_noop
    assert after_first == after_second


def test_raising_a_volume_tops_up_without_duplicating(cursor: object) -> None:
    seed_database(cursor, build_reference_data(SMALL, seed=7))  # type: ignore[arg-type]

    bigger = SeedVolumes(
        customers=SMALL.customers + 15,
        restaurants=SMALL.restaurants,
        riders=SMALL.riders,
        menu_items_per_restaurant=SMALL.menu_items_per_restaurant,
    )
    report = seed_database(cursor, build_reference_data(bigger, seed=7))  # type: ignore[arg-type]

    assert report.inserted["customers"] == 15
    assert count(cursor, "customers") == bigger.customers


def test_every_menu_item_points_at_a_real_restaurant(cursor: object) -> None:
    """The FK exists in the DDL, so a violation would have raised. This proves it ran."""
    seed_database(cursor, build_reference_data(SMALL, seed=7))  # type: ignore[arg-type]
    cursor.execute(  # type: ignore[attr-defined]
        """
        SELECT count(*) FROM menu_items m
        LEFT JOIN restaurants r USING (restaurant_id)
        WHERE r.restaurant_id IS NULL
        """
    )
    assert cursor.fetchone()[0] == 0  # type: ignore[attr-defined]


def test_stored_values_survive_the_round_trip(cursor: object) -> None:
    """NUMERIC(9,6) on coordinates and NUMERIC(10,2) on price both silently round."""
    data = build_reference_data(SMALL, seed=7)
    seed_database(cursor, data)  # type: ignore[arg-type]

    cursor.execute(  # type: ignore[attr-defined]
        "SELECT restaurant_id, lat, lon, rating, commission_pct FROM restaurants ORDER BY 1"
    )
    stored = {r[0]: r[1:] for r in cursor.fetchall()}  # type: ignore[attr-defined]
    for restaurant in data.restaurants:
        lat, lon, rating, commission = stored[restaurant.restaurant_id]
        assert float(lat) == restaurant.lat
        assert float(lon) == restaurant.lon
        assert float(rating) == restaurant.rating
        assert float(commission) == restaurant.commission_pct

    cursor.execute("SELECT menu_item_id, price_inr FROM menu_items ORDER BY 1")  # type: ignore[attr-defined]
    prices = {r[0]: float(r[1]) for r in cursor.fetchall()}  # type: ignore[attr-defined]
    for item in data.menu_items:
        assert prices[item.menu_item_id] == item.price_inr


def test_sequences_point_past_the_seeded_ids(cursor: object) -> None:
    """Otherwise the first generator-created order collides on the primary key."""
    data = build_reference_data(SMALL, seed=7)
    seed_database(cursor, data)  # type: ignore[arg-type]

    for table, pk, expected in (
        ("customers", "customer_id", SMALL.customers),
        ("restaurants", "restaurant_id", SMALL.restaurants),
        ("riders", "rider_id", SMALL.riders),
        ("menu_items", "menu_item_id", len(data.menu_items)),
    ):
        cursor.execute(  # type: ignore[attr-defined]
            "SELECT last_value FROM pg_sequences "
            "WHERE sequencename = pg_get_serial_sequence(%s, %s)::regclass::text",
            (table, pk),
        )
        row = cursor.fetchone()  # type: ignore[attr-defined]
        assert row is not None, f"no sequence found for {table}.{pk}"
        assert int(row[0]) == expected, f"{table} sequence at {row[0]}, expected {expected}"


def test_orders_replica_identity_is_full(cursor: object) -> None:
    """ADR-0001: without FULL, Debezium emits no before-image on UPDATE in Phase 2."""
    cursor.execute(  # type: ignore[attr-defined]
        "SELECT relreplident FROM pg_class WHERE relname = 'orders'"
    )
    assert cursor.fetchone()[0] == "f"  # type: ignore[attr-defined]


def test_wal_level_is_logical(cursor: object) -> None:
    """The Phase 0 condition, re-asserted from the code that will depend on it."""
    cursor.execute("SHOW wal_level")  # type: ignore[attr-defined]
    assert cursor.fetchone()[0] == "logical"  # type: ignore[attr-defined]
