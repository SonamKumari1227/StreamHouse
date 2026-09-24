"""Reference data seeding: customers, restaurants, menu items, riders.

Split deliberately in two halves:

* **Builders** (`build_*`) are pure. Given a seed they produce identical rows every time,
  with no database anywhere near them, so the interesting logic unit-tests in milliseconds.
* **The writer** (`seed_database`) does nothing but persist what a builder produced.

**Idempotency.** Primary keys are assigned explicitly as 1..N rather than left to BIGSERIAL,
and every INSERT carries `ON CONFLICT (pk) DO NOTHING`. Re-running is therefore a no-op, and
raising a volume tops the table up instead of duplicating it. The sequences are resynced
afterwards so that later BIGSERIAL inserts — orders, payments — do not collide with the
explicit ids used here.

Orders, order_items and payments are *not* seeded. Those are produced by the running
generator via the state machine, which is the only thing allowed to create them.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

from faker import Faker

from generator.config import (
    COMMISSION_PCT,
    CUISINES,
    CUSTOMER_TIER_WEIGHTS,
    CUSTOMER_TIERS,
    DEFAULT_RNG_SEED,
    DISHES,
    RESTAURANT_RATING,
    RIDER_TIER_WEIGHTS,
    RIDER_TIERS,
    VEHICLE_TYPE_WEIGHTS,
    VEHICLE_TYPES,
    City,
    SeedVolumes,
    pick_city,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "CustomerRow",
    "MenuItemRow",
    "ReferenceData",
    "RestaurantRow",
    "RiderRow",
    "SeedReport",
    "build_customers",
    "build_menu_items",
    "build_reference_data",
    "build_restaurants",
    "build_riders",
    "seed_database",
]

#: How far a restaurant or rider sits from its city centre, in degrees (~±9 km).
_CITY_JITTER_DEG = 0.08

#: Coordinates are stored as NUMERIC(9,6); anything finer is silently rounded by Postgres.
_COORD_DP = 6


class _Cursor(Protocol):
    """The slice of a DB-API cursor this module uses.

    A Protocol rather than an import so the writer can be exercised with a fake, and so
    `psycopg` is not a hard import for anyone who only wants the builders.
    """

    def execute(self, query: str, params: Sequence[Any] | None = ...) -> Any: ...
    def executemany(self, query: str, params_seq: Sequence[Any]) -> Any: ...
    def fetchone(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class CustomerRow:
    customer_id: int
    name: str
    phone: str
    city: str
    signup_ts: datetime
    tier: str
    is_active: bool
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class RestaurantRow:
    restaurant_id: int
    name: str
    city: str
    lat: float
    lon: float
    cuisine: str
    rating: float
    commission_pct: float
    is_open: bool
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class MenuItemRow:
    menu_item_id: int
    restaurant_id: int
    name: str
    price_inr: float
    is_available: bool
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class RiderRow:
    rider_id: int
    name: str
    city: str
    vehicle_type: str
    tier: str
    shift_start: datetime
    is_online: bool
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ReferenceData:
    customers: tuple[CustomerRow, ...]
    restaurants: tuple[RestaurantRow, ...]
    menu_items: tuple[MenuItemRow, ...]
    riders: tuple[RiderRow, ...]

    @property
    def total_rows(self) -> int:
        return len(self.customers) + len(self.restaurants) + len(self.menu_items) + len(self.riders)


@dataclass(frozen=True, slots=True)
class SeedReport:
    """What `seed_database` actually changed. Counted by row totals before and after,
    because `ON CONFLICT DO NOTHING` makes a driver's rowcount unreliable."""

    inserted: dict[str, int]
    skipped: dict[str, int]

    @property
    def total_inserted(self) -> int:
        return sum(self.inserted.values())

    @property
    def was_noop(self) -> bool:
        """True when every row already existed — what a second run must report."""
        return self.total_inserted == 0


def _jitter(value: float, rng: random.Random) -> float:
    return round(value + rng.uniform(-_CITY_JITTER_DEG, _CITY_JITTER_DEG), _COORD_DP)


def _weighted(rng: random.Random, values: tuple[str, ...], weights: tuple[float, ...]) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def build_customers(
    count: int,
    rng: random.Random,
    faker: Faker,
    now: datetime,
) -> tuple[CustomerRow, ...]:
    rows = []
    for customer_id in range(1, count + 1):
        city: City = pick_city(rng)
        # Signed up anywhere in the last two years, which gives cohort analysis in Gold
        # something to work with.
        signup = now - timedelta(days=rng.randint(0, 730), seconds=rng.randint(0, 86_399))
        rows.append(
            CustomerRow(
                customer_id=customer_id,
                name=faker.name(),
                phone=f"+91{faker.msisdn()[-10:]}",
                city=city.name,
                signup_ts=signup,
                tier=_weighted(rng, CUSTOMER_TIERS, CUSTOMER_TIER_WEIGHTS),
                is_active=rng.random() < 0.92,
                updated_at=now,
            )
        )
    return tuple(rows)


def build_restaurants(
    count: int,
    rng: random.Random,
    faker: Faker,
    now: datetime,
) -> tuple[RestaurantRow, ...]:
    rows = []
    for restaurant_id in range(1, count + 1):
        city = pick_city(rng)
        rows.append(
            RestaurantRow(
                restaurant_id=restaurant_id,
                name=f"{faker.last_name()} {rng.choice(_RESTAURANT_SUFFIXES)}",
                city=city.name,
                lat=_jitter(city.lat, rng),
                lon=_jitter(city.lon, rng),
                cuisine=rng.choice(CUISINES),
                rating=round(RESTAURANT_RATING.sample(rng), 1),
                commission_pct=round(COMMISSION_PCT.sample(rng), 2),
                # Most are open at seed time; the generator flips these later, which is
                # what gives dim_restaurant_scd2 something to track in Phase 3.
                is_open=rng.random() < 0.85,
                updated_at=now,
            )
        )
    return tuple(rows)


_RESTAURANT_SUFFIXES: tuple[str, ...] = (
    "Kitchen",
    "Dhaba",
    "Bistro",
    "Cafe",
    "House",
    "Corner",
    "Express",
    "Tiffins",
    "Darbar",
    "Grill",
)


def build_menu_items(
    restaurants: Sequence[RestaurantRow],
    volumes: SeedVolumes,
    rng: random.Random,
    now: datetime,
) -> tuple[MenuItemRow, ...]:
    """One menu per restaurant, drawn from its own cuisine's vocabulary."""
    rows = []
    menu_item_id = 0
    for restaurant in restaurants:
        dishes = DISHES[restaurant.cuisine]
        wanted = volumes.menu_items_per_restaurant.sample_int(rng)
        # Never more items than the cuisine has distinct dishes: a duplicate dish name
        # within one restaurant would be indistinguishable in the Gold star schema.
        count = max(1, min(wanted, len(dishes)))
        for dish in rng.sample(dishes, count):
            menu_item_id += 1
            rows.append(
                MenuItemRow(
                    menu_item_id=menu_item_id,
                    restaurant_id=restaurant.restaurant_id,
                    name=dish.name,
                    # Priced from the dish's own band, not one flat menu-wide distribution.
                    price_inr=dish.sample_price(rng),
                    is_available=rng.random() < 0.93,
                    updated_at=now,
                )
            )
    return tuple(rows)


def build_riders(
    count: int,
    rng: random.Random,
    faker: Faker,
    now: datetime,
) -> tuple[RiderRow, ...]:
    rows = []
    for rider_id in range(1, count + 1):
        city = pick_city(rng)
        # Shifts start across the day so rider utilisation in Gold is not degenerate.
        shift_start = now.replace(minute=0, second=0, microsecond=0) - timedelta(
            hours=rng.randint(0, 23)
        )
        rows.append(
            RiderRow(
                rider_id=rider_id,
                name=faker.name(),
                city=city.name,
                vehicle_type=_weighted(rng, VEHICLE_TYPES, VEHICLE_TYPE_WEIGHTS),
                tier=_weighted(rng, RIDER_TIERS, RIDER_TIER_WEIGHTS),
                shift_start=shift_start,
                is_online=rng.random() < 0.60,
                updated_at=now,
            )
        )
    return tuple(rows)


def build_reference_data(
    volumes: SeedVolumes | None = None,
    seed: int = DEFAULT_RNG_SEED,
    now: datetime | None = None,
) -> ReferenceData:
    """Build the whole reference world. Deterministic for a given (volumes, seed, now)."""
    volumes = volumes or SeedVolumes()
    now = now or datetime.now(UTC)

    rng = random.Random(seed)
    faker = Faker("en_IN")
    faker.seed_instance(seed)

    customers = build_customers(volumes.customers, rng, faker, now)
    restaurants = build_restaurants(volumes.restaurants, rng, faker, now)
    menu_items = build_menu_items(restaurants, volumes, rng, now)
    riders = build_riders(volumes.riders, rng, faker, now)

    return ReferenceData(
        customers=customers,
        restaurants=restaurants,
        menu_items=menu_items,
        riders=riders,
    )


# ---------------------------------------------------------------- persistence

_INSERTS: dict[str, str] = {
    "customers": """
        INSERT INTO customers
            (customer_id, name, phone, city, signup_ts, tier, is_active, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """,
    "restaurants": """
        INSERT INTO restaurants
            (restaurant_id, name, city, lat, lon, cuisine, rating,
             commission_pct, is_open, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (restaurant_id) DO NOTHING
    """,
    "menu_items": """
        INSERT INTO menu_items
            (menu_item_id, restaurant_id, name, price_inr, is_available, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (menu_item_id) DO NOTHING
    """,
    "riders": """
        INSERT INTO riders
            (rider_id, name, city, vehicle_type, tier, shift_start, is_online, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (rider_id) DO NOTHING
    """,
}

#: Table -> primary key, used to resync BIGSERIAL sequences after explicit-id inserts.
_PRIMARY_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "restaurants": "restaurant_id",
    "menu_items": "menu_item_id",
    "riders": "rider_id",
}

#: Insertion order respects foreign keys: menu_items references restaurants.
_TABLE_ORDER: tuple[str, ...] = ("customers", "restaurants", "menu_items", "riders")


def _row_count(cursor: _Cursor, table: str) -> int:
    # Table names come from _TABLE_ORDER, never from user input.
    cursor.execute(f"SELECT count(*) FROM {table}")
    result = cursor.fetchone()
    return int(result[0])


def _as_params(row: object) -> tuple[Any, ...]:
    return tuple(getattr(row, f) for f in row.__slots__)  # type: ignore[attr-defined]


def seed_database(cursor: _Cursor, data: ReferenceData) -> SeedReport:
    """Insert reference data, skipping anything already present.

    Counts by row totals before and after rather than trusting a driver's rowcount, which
    `ON CONFLICT DO NOTHING` makes ambiguous across drivers.
    """
    batches: dict[str, tuple[Any, ...]] = {
        "customers": data.customers,
        "restaurants": data.restaurants,
        "menu_items": data.menu_items,
        "riders": data.riders,
    }

    inserted: dict[str, int] = {}
    skipped: dict[str, int] = {}

    for table in _TABLE_ORDER:
        rows = batches[table]
        before = _row_count(cursor, table)
        if rows:
            cursor.executemany(_INSERTS[table], [_as_params(r) for r in rows])
        after = _row_count(cursor, table)

        inserted[table] = after - before
        skipped[table] = len(rows) - inserted[table]

    _resync_sequences(cursor)
    return SeedReport(inserted=inserted, skipped=skipped)


def _resync_sequences(cursor: _Cursor) -> None:
    """Point each BIGSERIAL sequence past the explicit ids just inserted.

    Without this, the first generator-created order would try to reuse id 1 and fail on the
    primary key. Table and column names are module constants, never user input.
    """
    for table, pk in _PRIMARY_KEYS.items():
        cursor.execute(
            f"SELECT setval(pg_get_serial_sequence('{table}', '{pk}'), "
            f"COALESCE((SELECT MAX({pk}) FROM {table}), 1))"
        )
