"""Unit tests for generator configuration.

The most valuable test here is `test_enumerations_match_the_ddl_check_constraints`: it parses
the real schema file and fails if a Python tuple and a database CHECK constraint ever drift
apart. Without it, a drift stays invisible until a generator run dies mid-insert on a
constraint violation.
"""

from __future__ import annotations

import random
import re
from collections import Counter
from pathlib import Path

import pytest

from generator.config import (
    CITIES,
    COMMISSION_PCT,
    CUISINES,
    CUSTOMER_TIER_WEIGHTS,
    CUSTOMER_TIERS,
    DISHES,
    ITEMS_PER_ORDER,
    MENU_PRICE_INR,
    PAYMENT_METHOD_WEIGHTS,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    RESTAURANT_RATING,
    RIDER_TIER_WEIGHTS,
    RIDER_TIERS,
    VEHICLE_TYPE_WEIGHTS,
    VEHICLE_TYPES,
    City,
    Dish,
    LoadConfig,
    Range,
    SeedVolumes,
    dish_names,
    pick_city,
)
from generator.state_machine import OrderStatus

SCHEMA_SQL = Path(__file__).parents[2] / "infra" / "postgres" / "init" / "01-schema.sql"


def ddl_check_constraints() -> list[tuple[str, frozenset[str]]]:
    """Every `CHECK (col IN ('A', 'B', ...))` in the schema, as (column, values)."""
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    found = []
    for column, body in re.findall(r"CHECK\s*\(\s*(\w+)\s+IN\s*\(([^)]+)\)\s*\)", sql):
        found.append((column, frozenset(re.findall(r"'([^']+)'", body))))
    return found


# --------------------------------------------------------------------------- DDL parity


def test_schema_file_exists_and_has_check_constraints() -> None:
    assert SCHEMA_SQL.is_file(), f"schema not found at {SCHEMA_SQL}"
    assert len(ddl_check_constraints()) >= 5


@pytest.mark.parametrize(
    ("column", "values"),
    [
        ("tier", CUSTOMER_TIERS),
        ("tier", RIDER_TIERS),
        ("vehicle_type", VEHICLE_TYPES),
        ("method", PAYMENT_METHODS),
        ("status", PAYMENT_STATUSES),
    ],
    ids=["customer_tier", "rider_tier", "vehicle_type", "payment_method", "payment_status"],
)
def test_enumerations_match_the_ddl_check_constraints(column: str, values: tuple[str, ...]) -> None:
    constraints = ddl_check_constraints()
    assert (column, frozenset(values)) in constraints, (
        f"config {column}={sorted(values)} has no matching CHECK constraint in the DDL. "
        f"Found: {[(c, sorted(v)) for c, v in constraints if c == column]}"
    )


def test_order_statuses_match_the_ddl() -> None:
    """The state machine and the database must agree on the set of order states."""
    machine = frozenset(s.value for s in OrderStatus)
    assert ("status", machine) in ddl_check_constraints()


# --------------------------------------------------------------------------- cities


def test_city_names_are_unique() -> None:
    names = [c.name for c in CITIES]
    assert len(names) == len(set(names))


def test_city_weights_sum_to_one() -> None:
    assert sum(c.weight for c in CITIES) == pytest.approx(1.0, abs=1e-9)


def test_city_coordinates_fit_numeric_9_6() -> None:
    """The DDL stores lat/lon as NUMERIC(9,6); more precision would be silently rounded."""
    for city in CITIES:
        for value in (city.lat, city.lon):
            decimals = str(value)[::-1].find(".")
            assert decimals <= 6, f"{city.name}: {value} has more than 6 decimal places"


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"name": "", "lat": 12.0, "lon": 77.0, "weight": 1.0}, "must not be empty"),
        ({"name": "X", "lat": 51.5, "lon": 77.0, "weight": 1.0}, "latitude"),
        ({"name": "X", "lat": 12.0, "lon": 2.3, "weight": 1.0}, "longitude"),
        ({"name": "X", "lat": 12.0, "lon": 77.0, "weight": 0.0}, "weight must be > 0"),
    ],
    ids=["empty name", "latitude outside India", "longitude outside India", "zero weight"],
)
def test_city_rejects_invalid_values(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        City(**kwargs)  # type: ignore[arg-type]


def test_pick_city_respects_weights() -> None:
    rng = random.Random(5)
    counts = Counter(pick_city(rng).name for _ in range(20_000))
    for city in CITIES:
        share = counts[city.name] / 20_000
        assert share == pytest.approx(
            city.weight, abs=0.02
        ), f"{city.name}: sampled {share:.3f}, configured {city.weight}"


def test_pick_city_rejects_an_empty_city_list() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        pick_city(random.Random(1), cities=())


# --------------------------------------------------------------------------- cuisines


def test_cuisines_are_unique_and_non_empty() -> None:
    assert len(CUISINES) == len(set(CUISINES))
    assert all(c.strip() for c in CUISINES)


# --------------------------------------------------------------------------- weights


@pytest.mark.parametrize(
    ("values", "weights"),
    [
        (CUSTOMER_TIERS, CUSTOMER_TIER_WEIGHTS),
        (RIDER_TIERS, RIDER_TIER_WEIGHTS),
        (VEHICLE_TYPES, VEHICLE_TYPE_WEIGHTS),
        (PAYMENT_METHODS, PAYMENT_METHOD_WEIGHTS),
    ],
    ids=["customer tier", "rider tier", "vehicle type", "payment method"],
)
def test_weight_tuples_line_up_with_their_values(
    values: tuple[str, ...], weights: tuple[float, ...]
) -> None:
    assert len(values) == len(weights)
    assert sum(weights) == pytest.approx(1.0, abs=1e-9)
    assert all(w > 0 for w in weights)


# --------------------------------------------------------------------------- Range


@pytest.mark.parametrize(
    ("name", "rng_spec"),
    [
        ("MENU_PRICE_INR", MENU_PRICE_INR),
        ("COMMISSION_PCT", COMMISSION_PCT),
        ("RESTAURANT_RATING", RESTAURANT_RATING),
        ("ITEMS_PER_ORDER", ITEMS_PER_ORDER),
    ],
)
def test_range_samples_stay_within_bounds(name: str, rng_spec: Range) -> None:
    rng = random.Random(3)
    for _ in range(5_000):
        value = rng_spec.sample(rng)
        assert rng_spec.low <= value <= rng_spec.high, f"{name} produced {value}"


def test_range_rejects_an_impossible_distribution() -> None:
    with pytest.raises(ValueError, match="low <= mode <= high"):
        Range(low=10, high=1, mode=5)


def test_sample_int_rounds_and_stays_in_bounds() -> None:
    rng = random.Random(9)
    spec = Range(low=1.0, high=6.0, mode=2.0)
    values = [spec.sample_int(rng) for _ in range(2_000)]
    assert all(isinstance(v, int) for v in values)
    assert min(values) >= 1
    assert max(values) <= 6


def test_restaurant_rating_fits_the_ddl_constraint() -> None:
    """DDL: rating NUMERIC(2,1) CHECK (rating >= 0 AND rating <= 5)."""
    assert RESTAURANT_RATING.low >= 0
    assert RESTAURANT_RATING.high <= 5


def test_commission_fits_the_ddl_constraint() -> None:
    """DDL: commission_pct CHECK (commission_pct >= 0 AND commission_pct <= 100)."""
    assert COMMISSION_PCT.low >= 0
    assert COMMISSION_PCT.high <= 100


# --------------------------------------------------------------------------- SeedVolumes


def test_seed_volumes_defaults_are_modest() -> None:
    """A large first snapshot is the documented way Debezium fails in Phase 2."""
    volumes = SeedVolumes()
    assert volumes.customers <= 5_000
    assert volumes.restaurants <= 1_000
    assert volumes.riders <= 2_000


@pytest.mark.parametrize("field", ["customers", "restaurants", "riders"])
def test_seed_volumes_rejects_non_positive_counts(field: str) -> None:
    with pytest.raises(ValueError, match=f"{field} must be > 0"):
        SeedVolumes(**{field: 0})  # type: ignore[arg-type]


def test_seed_volumes_requires_at_least_one_menu_item() -> None:
    with pytest.raises(ValueError, match="at least one menu item"):
        SeedVolumes(menu_items_per_restaurant=Range(low=0.0, high=5.0, mode=2.0))


# --------------------------------------------------------------------------- LoadConfig


def test_orders_per_second_accounts_for_speed() -> None:
    real_time = LoadConfig(orders_per_day=86_400, speed=1.0)
    assert real_time.orders_per_second == pytest.approx(1.0)
    compressed = LoadConfig(orders_per_day=86_400, speed=20.0)
    assert compressed.orders_per_second == pytest.approx(20.0)


def test_default_load_is_plausible_for_a_demo() -> None:
    load = LoadConfig()
    assert load.orders_per_day == 50_000
    # ~11.6/s at 20x: a realistic status mix within minutes, not a full day.
    assert 10.0 < load.orders_per_second < 13.0


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"orders_per_day": 0}, "orders_per_day must be > 0"),
        ({"speed": 0}, "speed must be > 0"),
        ({"gps_ping_interval_s": 0}, "gps_ping_interval_s must be > 0"),
        ({"gps_max_msgs_per_second": 0}, "gps_max_msgs_per_second must be > 0"),
    ],
    ids=["orders_per_day", "speed", "ping interval", "gps rate"],
)
def test_load_config_rejects_invalid_values(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        LoadConfig(**kwargs)  # type: ignore[arg-type]


def test_gps_defaults_match_the_spec() -> None:
    """Spec: ~1 ping / 5s / rider, throttled to ~200 msg/s."""
    load = LoadConfig()
    assert load.gps_ping_interval_s == 5.0
    assert load.gps_max_msgs_per_second == 200


# --------------------------------------------------------------------------- dish pricing


def test_every_dish_band_sits_inside_the_global_envelope() -> None:
    """A mistyped band should fail here, not reach the database."""
    for cuisine, dishes in DISHES.items():
        for dish in dishes:
            assert dish.low_inr >= MENU_PRICE_INR.low, f"{cuisine}/{dish.name} too cheap"
            assert dish.high_inr <= MENU_PRICE_INR.high, f"{cuisine}/{dish.name} too expensive"


def test_sampled_prices_stay_inside_their_own_band() -> None:
    rng = random.Random(21)
    for dishes in DISHES.values():
        for dish in dishes:
            for _ in range(200):
                price = dish.sample_price(rng)
                assert dish.low_inr <= price <= dish.high_inr
                assert round(price, 2) == price, "price must fit NUMERIC(10,2)"


def test_a_bread_never_costs_more_than_a_biryani() -> None:
    """The exact realism bug this replaced: one flat band priced naan above mutton biryani."""
    naan = next(d for d in DISHES["North Indian"] if d.name == "Butter Naan")
    biryani = next(d for d in DISHES["Biryani"] if d.name == "Mutton Biryani")
    assert naan.high_inr < biryani.low_inr


def test_sides_are_cheaper_than_mains_within_a_cuisine() -> None:
    for cuisine, side, main in (
        ("North Indian", "Butter Naan", "Paneer Butter Masala"),
        ("Chinese", "Hot and Sour Soup", "Chicken Lollipop"),
        ("Pizza", "Cheese Garlic Bread", "Pepperoni"),
        ("Street Food", "Vada Pav", "Kathi Roll"),
    ):
        s = next(d for d in DISHES[cuisine] if d.name == side)
        m = next(d for d in DISHES[cuisine] if d.name == main)
        assert s.mode_inr < m.mode_inr, f"{side} should cost less than {main}"


def test_non_veg_biryani_costs_more_than_veg() -> None:
    veg = next(d for d in DISHES["Biryani"] if d.name == "Veg Dum Biryani")
    mutton = next(d for d in DISHES["Biryani"] if d.name == "Mutton Biryani")
    assert veg.mode_inr < mutton.mode_inr


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"name": "", "low_inr": 10, "high_inr": 20, "mode_inr": 15}, "must not be empty"),
        ({"name": "X", "low_inr": 0, "high_inr": 20, "mode_inr": 15}, "low_inr must be > 0"),
        ({"name": "X", "low_inr": 30, "high_inr": 20, "mode_inr": 25}, "low <= mode <= high"),
    ],
    ids=["empty name", "zero floor", "inverted band"],
)
def test_dish_rejects_invalid_bands(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        Dish(**kwargs)  # type: ignore[arg-type]


def test_dish_names_helper_matches_the_dishes() -> None:
    for cuisine, dishes in DISHES.items():
        assert dish_names(cuisine) == tuple(d.name for d in dishes)
