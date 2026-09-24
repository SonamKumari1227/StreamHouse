"""Unit tests for reference-data seeding.

No database. The builders are pure, and the writer is exercised against a fake cursor that
models the one behaviour that matters: `ON CONFLICT (pk) DO NOTHING`.

The real Postgres round trip lives in tests/integration/test_seed_idempotency.py.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from faker import Faker

from generator.config import (
    CITIES,
    COMMISSION_PCT,
    CUISINES,
    CUSTOMER_TIERS,
    DISHES,
    MENU_PRICE_INR,
    RESTAURANT_RATING,
    RIDER_TIERS,
    VEHICLE_TYPES,
    Range,
    SeedVolumes,
    dish_names,
)
from generator.seed import (
    ReferenceData,
    build_customers,
    build_menu_items,
    build_reference_data,
    build_restaurants,
    build_riders,
    seed_database,
)

NOW = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)
CITY_NAMES = {c.name for c in CITIES}
SMALL = SeedVolumes(
    customers=40,
    restaurants=12,
    riders=15,
    menu_items_per_restaurant=Range(low=3.0, high=8.0, mode=5.0),
)


def faker_for(seed: int = 1) -> Faker:
    fake = Faker("en_IN")
    fake.seed_instance(seed)
    return fake


# --------------------------------------------------------------------------- a fake cursor


@dataclass
class FakeCursor:
    """Models a table keyed by primary key, with ON CONFLICT DO NOTHING semantics."""

    tables: dict[str, dict[Any, tuple[Any, ...]]] = field(default_factory=dict)
    sequences: dict[str, int] = field(default_factory=dict)
    _last: int = 0

    def execute(self, query: str, params: Any = None) -> None:
        stripped = query.strip()
        if stripped.upper().startswith("SELECT COUNT(*) FROM "):
            table = stripped.split()[-1]
            self._last = len(self.tables.get(table, {}))
        elif "setval" in stripped:
            table = stripped.split("pg_get_serial_sequence('")[1].split("'")[0]
            self.sequences[table] = max(self.tables.get(table, {}) or [0])
        else:  # pragma: no cover - guards against an unexpected statement
            raise AssertionError(f"unexpected statement: {stripped[:60]}")

    def executemany(self, query: str, params_seq: Any) -> None:
        table = query.strip().split("INSERT INTO")[1].split()[0]
        store = self.tables.setdefault(table, {})
        for params in params_seq:
            pk = params[0]
            if pk not in store:  # ON CONFLICT (pk) DO NOTHING
                store[pk] = tuple(params)

    def fetchone(self) -> tuple[int]:
        return (self._last,)


# --------------------------------------------------------------------------- customers


def test_customers_have_contiguous_ids_from_one() -> None:
    rows = build_customers(50, random.Random(1), faker_for(), NOW)
    assert [r.customer_id for r in rows] == list(range(1, 51))


def test_customers_use_valid_tiers_and_cities() -> None:
    rows = build_customers(300, random.Random(2), faker_for(), NOW)
    assert {r.tier for r in rows} <= set(CUSTOMER_TIERS)
    assert {r.city for r in rows} <= CITY_NAMES


def test_customer_phone_numbers_look_indian() -> None:
    rows = build_customers(100, random.Random(3), faker_for(), NOW)
    for row in rows:
        assert row.phone.startswith("+91")
        assert len(row.phone) == 13
        assert row.phone[3:].isdigit()


def test_customers_signed_up_in_the_past_within_two_years() -> None:
    rows = build_customers(200, random.Random(4), faker_for(), NOW)
    for row in rows:
        assert row.signup_ts <= NOW
        assert (NOW - row.signup_ts).days <= 730


def test_most_customers_are_active_but_not_all() -> None:
    rows = build_customers(1_000, random.Random(5), faker_for(), NOW)
    active = sum(1 for r in rows if r.is_active)
    assert 0 < active < 1_000
    assert 0.85 < active / 1_000 < 0.98


# --------------------------------------------------------------------------- restaurants


def test_restaurants_have_contiguous_ids_and_valid_enums() -> None:
    rows = build_restaurants(60, random.Random(6), faker_for(), NOW)
    assert [r.restaurant_id for r in rows] == list(range(1, 61))
    assert {r.cuisine for r in rows} <= set(CUISINES)
    assert {r.city for r in rows} <= CITY_NAMES


def test_restaurant_coordinates_sit_near_their_city_centre() -> None:
    rows = build_restaurants(200, random.Random(7), faker_for(), NOW)
    centres = {c.name: (c.lat, c.lon) for c in CITIES}
    for row in rows:
        lat, lon = centres[row.city]
        assert abs(row.lat - lat) <= 0.08 + 1e-9
        assert abs(row.lon - lon) <= 0.08 + 1e-9


def test_restaurant_coordinates_fit_numeric_9_6() -> None:
    rows = build_restaurants(200, random.Random(8), faker_for(), NOW)
    for row in rows:
        for value in (row.lat, row.lon):
            assert round(value, 6) == value


def test_restaurant_rating_and_commission_satisfy_ddl_constraints() -> None:
    rows = build_restaurants(300, random.Random(9), faker_for(), NOW)
    for row in rows:
        assert 0 <= row.rating <= 5
        assert round(row.rating, 1) == row.rating, "rating must fit NUMERIC(2,1)"
        assert RESTAURANT_RATING.low - 0.05 <= row.rating <= RESTAURANT_RATING.high
        assert 0 <= row.commission_pct <= 100
        assert COMMISSION_PCT.low <= row.commission_pct <= COMMISSION_PCT.high


def test_some_restaurants_start_closed() -> None:
    """Phase 3 needs is_open to actually vary, or dim_restaurant_scd2 tracks nothing."""
    rows = build_restaurants(500, random.Random(10), faker_for(), NOW)
    open_count = sum(1 for r in rows if r.is_open)
    assert 0 < open_count < 500


# --------------------------------------------------------------------------- menu items


def test_every_restaurant_gets_at_least_one_menu_item() -> None:
    restaurants = build_restaurants(40, random.Random(11), faker_for(), NOW)
    items = build_menu_items(restaurants, SMALL, random.Random(11), NOW)
    covered = {i.restaurant_id for i in items}
    assert covered == {r.restaurant_id for r in restaurants}


def test_menu_item_ids_are_contiguous() -> None:
    restaurants = build_restaurants(20, random.Random(12), faker_for(), NOW)
    items = build_menu_items(restaurants, SMALL, random.Random(12), NOW)
    assert [i.menu_item_id for i in items] == list(range(1, len(items) + 1))


def test_menu_items_reference_only_real_restaurants() -> None:
    restaurants = build_restaurants(25, random.Random(13), faker_for(), NOW)
    items = build_menu_items(restaurants, SMALL, random.Random(13), NOW)
    valid = {r.restaurant_id for r in restaurants}
    assert all(i.restaurant_id in valid for i in items)


def test_dish_names_come_from_the_restaurants_own_cuisine() -> None:
    restaurants = build_restaurants(40, random.Random(14), faker_for(), NOW)
    items = build_menu_items(restaurants, SMALL, random.Random(14), NOW)
    cuisine_by_id = {r.restaurant_id: r.cuisine for r in restaurants}
    for item in items:
        assert item.name in dish_names(cuisine_by_id[item.restaurant_id])


def test_no_restaurant_lists_the_same_dish_twice() -> None:
    restaurants = build_restaurants(40, random.Random(15), faker_for(), NOW)
    items = build_menu_items(restaurants, SMALL, random.Random(15), NOW)
    seen: set[tuple[int, str]] = set()
    for item in items:
        key = (item.restaurant_id, item.name)
        assert key not in seen, f"duplicate dish {item.name} at restaurant {item.restaurant_id}"
        seen.add(key)


def test_menu_size_is_capped_by_the_cuisine_vocabulary() -> None:
    """Asking for more items than a cuisine has dishes must not duplicate or crash."""
    restaurants = build_restaurants(30, random.Random(16), faker_for(), NOW)
    greedy = SeedVolumes(menu_items_per_restaurant=Range(low=50.0, high=80.0, mode=60.0))
    items = build_menu_items(restaurants, greedy, random.Random(16), NOW)
    per_restaurant: dict[int, int] = {}
    for item in items:
        per_restaurant[item.restaurant_id] = per_restaurant.get(item.restaurant_id, 0) + 1
    for restaurant in restaurants:
        assert per_restaurant[restaurant.restaurant_id] == len(DISHES[restaurant.cuisine])


def test_menu_prices_are_two_decimal_places_and_in_range() -> None:
    restaurants = build_restaurants(40, random.Random(17), faker_for(), NOW)
    items = build_menu_items(restaurants, SMALL, random.Random(17), NOW)
    for item in items:
        assert MENU_PRICE_INR.low <= item.price_inr <= MENU_PRICE_INR.high
        assert round(item.price_inr, 2) == item.price_inr
        assert item.price_inr >= 0


# --------------------------------------------------------------------------- riders


def test_riders_have_contiguous_ids_and_valid_enums() -> None:
    rows = build_riders(80, random.Random(18), faker_for(), NOW)
    assert [r.rider_id for r in rows] == list(range(1, 81))
    assert {r.tier for r in rows} <= set(RIDER_TIERS)
    assert {r.vehicle_type for r in rows} <= set(VEHICLE_TYPES)
    assert {r.city for r in rows} <= CITY_NAMES


def test_rider_shifts_are_spread_across_the_day() -> None:
    rows = build_riders(300, random.Random(19), faker_for(), NOW)
    hours = {r.shift_start.hour for r in rows}
    assert len(hours) > 12, "shift starts are too clustered for rider utilisation analysis"
    assert all(r.shift_start <= NOW for r in rows)


def test_riders_are_a_mix_of_online_and_offline() -> None:
    rows = build_riders(400, random.Random(20), faker_for(), NOW)
    online = sum(1 for r in rows if r.is_online)
    assert 0 < online < 400


# --------------------------------------------------------------------------- whole world


def test_build_reference_data_honours_the_requested_volumes() -> None:
    data = build_reference_data(SMALL, seed=42, now=NOW)
    assert len(data.customers) == SMALL.customers
    assert len(data.restaurants) == SMALL.restaurants
    assert len(data.riders) == SMALL.riders
    assert len(data.menu_items) >= SMALL.restaurants
    assert data.total_rows == (
        len(data.customers) + len(data.restaurants) + len(data.menu_items) + len(data.riders)
    )


def test_build_reference_data_is_deterministic() -> None:
    a = build_reference_data(SMALL, seed=99, now=NOW)
    b = build_reference_data(SMALL, seed=99, now=NOW)
    assert a == b


def test_different_seeds_produce_different_worlds() -> None:
    a = build_reference_data(SMALL, seed=1, now=NOW)
    b = build_reference_data(SMALL, seed=2, now=NOW)
    assert a != b


def test_build_reference_data_defaults_are_usable() -> None:
    data = build_reference_data(now=NOW)
    assert len(data.customers) == 500
    assert len(data.restaurants) == 80
    assert len(data.riders) == 120
    assert data.total_rows > 700


# --------------------------------------------------------------------------- the writer


def test_seed_database_inserts_everything_on_a_clean_database() -> None:
    data = build_reference_data(SMALL, seed=7, now=NOW)
    cursor = FakeCursor()
    report = seed_database(cursor, data)

    assert report.inserted["customers"] == SMALL.customers
    assert report.inserted["restaurants"] == SMALL.restaurants
    assert report.inserted["riders"] == SMALL.riders
    assert report.inserted["menu_items"] == len(data.menu_items)
    assert report.total_inserted == data.total_rows
    assert not report.was_noop
    assert all(v == 0 for v in report.skipped.values())


def test_seeding_twice_is_a_no_op() -> None:
    """The property the whole module exists to guarantee."""
    data = build_reference_data(SMALL, seed=7, now=NOW)
    cursor = FakeCursor()

    first = seed_database(cursor, data)
    second = seed_database(cursor, data)

    assert first.total_inserted == data.total_rows
    assert second.total_inserted == 0
    assert second.was_noop
    assert second.skipped["customers"] == SMALL.customers
    # And the tables did not grow.
    assert len(cursor.tables["customers"]) == SMALL.customers
    assert len(cursor.tables["menu_items"]) == len(data.menu_items)


def test_raising_a_volume_tops_up_rather_than_duplicating() -> None:
    cursor = FakeCursor()
    small = build_reference_data(SMALL, seed=7, now=NOW)
    seed_database(cursor, small)

    bigger = SeedVolumes(
        customers=SMALL.customers + 10,
        restaurants=SMALL.restaurants,
        riders=SMALL.riders,
        menu_items_per_restaurant=SMALL.menu_items_per_restaurant,
    )
    report = seed_database(cursor, build_reference_data(bigger, seed=7, now=NOW))

    assert report.inserted["customers"] == 10
    assert len(cursor.tables["customers"]) == bigger.customers


def test_sequences_are_resynced_past_the_explicit_ids() -> None:
    """Without this the first generator-created order collides on the primary key."""
    data = build_reference_data(SMALL, seed=7, now=NOW)
    cursor = FakeCursor()
    seed_database(cursor, data)

    assert cursor.sequences["customers"] == SMALL.customers
    assert cursor.sequences["restaurants"] == SMALL.restaurants
    assert cursor.sequences["riders"] == SMALL.riders
    assert cursor.sequences["menu_items"] == len(data.menu_items)


def test_restaurants_are_written_before_menu_items() -> None:
    """menu_items has a foreign key to restaurants; the wrong order fails on a real DB."""
    data = build_reference_data(SMALL, seed=7, now=NOW)
    order: list[str] = []

    class OrderTrackingCursor(FakeCursor):
        def executemany(self, query: str, params_seq: Any) -> None:
            order.append(query.strip().split("INSERT INTO")[1].split()[0])
            super().executemany(query, params_seq)

    seed_database(OrderTrackingCursor(), data)
    assert order.index("restaurants") < order.index("menu_items")


def test_seed_database_handles_empty_batches() -> None:
    cursor = FakeCursor()
    report = seed_database(cursor, ReferenceData((), (), (), ()))
    assert report.total_inserted == 0
    assert report.was_noop


def test_seed_report_totals() -> None:
    data = build_reference_data(SMALL, seed=7, now=NOW)
    report = seed_database(FakeCursor(), data)
    assert report.total_inserted == sum(report.inserted.values())
    assert set(report.inserted) == set(report.skipped)


# --------------------------------------------------------------------------- vocabulary


def test_every_cuisine_has_dishes() -> None:
    assert set(DISHES) == set(CUISINES)
    for cuisine, dishes in DISHES.items():
        names = [d.name for d in dishes]
        assert len(dishes) >= 5, f"{cuisine} has too few dishes to build a menu"
        assert len(set(names)) == len(names), f"{cuisine} lists a duplicate dish"


def test_dish_names_fit_in_a_reasonable_column() -> None:
    for dishes in DISHES.values():
        for dish in dishes:
            assert 0 < len(dish.name) <= 60
