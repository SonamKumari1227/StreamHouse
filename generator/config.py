"""Static configuration for the synthetic source generators.

One place to retune the simulated world: which cities exist, what people order, how many
of everything to seed, and how fast the clock runs.

**The enumerated value tuples here must match the CHECK constraints in
`infra/postgres/init/01-schema.sql` exactly.** A test asserts that by parsing the DDL, so
the two cannot drift apart silently — a drift would only surface later as a constraint
violation in the middle of a generator run.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from generator.state_machine import DEFAULT_SPEED

__all__ = [
    "CITIES",
    "COMMISSION_PCT",
    "CUISINES",
    "CUSTOMER_TIERS",
    "CUSTOMER_TIER_WEIGHTS",
    "DEFAULT_RNG_SEED",
    "DISHES",
    "ITEMS_PER_ORDER",
    "MENU_ITEMS_PER_RESTAURANT",
    "MENU_PRICE_INR",
    "PAYMENT_METHODS",
    "PAYMENT_METHOD_WEIGHTS",
    "PAYMENT_STATUSES",
    "RESTAURANT_RATING",
    "RIDER_TIERS",
    "RIDER_TIER_WEIGHTS",
    "VEHICLE_TYPES",
    "VEHICLE_TYPE_WEIGHTS",
    "City",
    "LoadConfig",
    "Range",
    "SeedVolumes",
    "pick_city",
]

#: Default seed for every RNG in the generator. Fixed so a run is reproducible; override
#: on the CLI when you want a different world.
DEFAULT_RNG_SEED: int = 20260924

# India's bounding box, used to sanity-check city coordinates.
_LAT_RANGE = (6.0, 37.5)
_LON_RANGE = (68.0, 97.5)


@dataclass(frozen=True, slots=True)
class Range:
    """A triangular distribution over plain numbers.

    Deliberately separate from `state_machine.Delay`, which returns a timedelta and is
    scaled by `speed`. Keeping them apart lets `state_machine` stay a leaf module with no
    imports from the rest of the generator.
    """

    low: float
    high: float
    mode: float

    def __post_init__(self) -> None:
        if not self.low <= self.mode <= self.high:
            raise ValueError(
                f"require low <= mode <= high, got {self.low} / {self.mode} / {self.high}"
            )

    def sample(self, rng: random.Random) -> float:
        return rng.triangular(self.low, self.high, self.mode)

    def sample_int(self, rng: random.Random) -> int:
        return int(round(self.sample(rng)))


@dataclass(frozen=True, slots=True)
class City:
    """A city the marketplace operates in.

    `weight` is its relative share of order volume, not its population — Bengaluru punches
    well above its size for quick commerce.
    """

    name: str
    lat: float
    lon: float
    weight: float

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("city name must not be empty")
        if not _LAT_RANGE[0] <= self.lat <= _LAT_RANGE[1]:
            raise ValueError(f"{self.name}: latitude {self.lat} is outside India")
        if not _LON_RANGE[0] <= self.lon <= _LON_RANGE[1]:
            raise ValueError(f"{self.name}: longitude {self.lon} is outside India")
        if self.weight <= 0:
            raise ValueError(f"{self.name}: weight must be > 0, got {self.weight}")


CITIES: tuple[City, ...] = (
    City("Bengaluru", 12.971600, 77.594600, weight=0.24),
    City("Mumbai", 19.076000, 72.877700, weight=0.19),
    City("Delhi", 28.613900, 77.209000, weight=0.17),
    City("Hyderabad", 17.385000, 78.486700, weight=0.12),
    City("Pune", 18.520400, 73.856700, weight=0.09),
    City("Chennai", 13.082700, 80.270700, weight=0.08),
    City("Kolkata", 22.572600, 88.363900, weight=0.06),
    City("Ahmedabad", 23.022500, 72.571400, weight=0.05),
)

CUISINES: tuple[str, ...] = (
    "North Indian",
    "South Indian",
    "Biryani",
    "Chinese",
    "Pizza",
    "Burgers",
    "Street Food",
    "Desserts",
    "Healthy",
    "Cafe",
)

#: Menu vocabulary per cuisine. Every cuisine in CUISINES must have an entry; a test
#: enforces that, so adding a cuisine cannot silently produce restaurants with no menu.
DISHES: dict[str, tuple[str, ...]] = {
    "North Indian": (
        "Paneer Butter Masala",
        "Dal Makhani",
        "Chole Bhature",
        "Rajma Chawal",
        "Butter Naan",
        "Kadhai Paneer",
        "Aloo Paratha",
        "Malai Kofta",
    ),
    "South Indian": (
        "Masala Dosa",
        "Idli Sambar",
        "Medu Vada",
        "Rava Upma",
        "Filter Coffee",
        "Pongal",
        "Uttapam",
        "Lemon Rice",
    ),
    "Biryani": (
        "Hyderabadi Chicken Biryani",
        "Mutton Biryani",
        "Veg Dum Biryani",
        "Egg Biryani",
        "Prawn Biryani",
        "Mirchi ka Salan",
        "Double ka Meetha",
    ),
    "Chinese": (
        "Veg Hakka Noodles",
        "Chilli Paneer",
        "Schezwan Fried Rice",
        "Manchurian Gravy",
        "Spring Rolls",
        "Chicken Lollipop",
        "Hot and Sour Soup",
    ),
    "Pizza": (
        "Margherita",
        "Farmhouse",
        "Peppy Paneer",
        "Chicken Tikka Pizza",
        "Cheese Garlic Bread",
        "Pepperoni",
        "Veggie Supreme",
    ),
    "Burgers": (
        "Aloo Tikki Burger",
        "Chicken Maharaja",
        "Veg Cheese Burger",
        "Crispy Chicken Burger",
        "Peri Peri Fries",
        "Double Patty Burger",
    ),
    "Street Food": (
        "Pani Puri",
        "Bhel Puri",
        "Vada Pav",
        "Pav Bhaji",
        "Samosa Chaat",
        "Dahi Puri",
        "Momos",
        "Kathi Roll",
    ),
    "Desserts": (
        "Gulab Jamun",
        "Rasmalai",
        "Chocolate Brownie",
        "Gajar ka Halwa",
        "Tiramisu",
        "Kulfi Falooda",
        "Cheesecake Slice",
    ),
    "Healthy": (
        "Quinoa Bowl",
        "Grilled Chicken Salad",
        "Sprouts Salad",
        "Oats Smoothie Bowl",
        "Paneer Tikka Salad",
        "Fruit Bowl",
    ),
    "Cafe": (
        "Cappuccino",
        "Cold Coffee",
        "Chicken Sandwich",
        "Veg Club Sandwich",
        "Blueberry Muffin",
        "Croissant",
        "Iced Latte",
    ),
}

# ---------------------------------------------------------------- enumerations
# These MUST mirror the CHECK constraints in infra/postgres/init/01-schema.sql.
# tests/unit/test_config.py parses the DDL and fails if they drift.

CUSTOMER_TIERS: tuple[str, ...] = ("STANDARD", "PLUS", "PRO")
CUSTOMER_TIER_WEIGHTS: tuple[float, ...] = (0.78, 0.18, 0.04)

RIDER_TIERS: tuple[str, ...] = ("BRONZE", "SILVER", "GOLD")
RIDER_TIER_WEIGHTS: tuple[float, ...] = (0.55, 0.33, 0.12)

VEHICLE_TYPES: tuple[str, ...] = ("BIKE", "SCOOTER", "BICYCLE", "CAR")
VEHICLE_TYPE_WEIGHTS: tuple[float, ...] = (0.62, 0.24, 0.10, 0.04)

PAYMENT_METHODS: tuple[str, ...] = ("UPI", "CARD", "NETBANKING", "COD", "WALLET")
PAYMENT_METHOD_WEIGHTS: tuple[float, ...] = (0.58, 0.19, 0.04, 0.13, 0.06)

PAYMENT_STATUSES: tuple[str, ...] = (
    "PENDING",
    "AUTHORISED",
    "CAPTURED",
    "FAILED",
    "REFUNDED",
)

# ---------------------------------------------------------------- value distributions

#: Menu item price. Long right tail: most items are cheap, a few are a family platter.
MENU_PRICE_INR: Range = Range(low=49.0, high=899.0, mode=189.0)

#: Platform commission taken from the restaurant.
COMMISSION_PCT: Range = Range(low=12.0, high=28.0, mode=20.0)

#: Restaurant star rating.
RESTAURANT_RATING: Range = Range(low=2.8, high=5.0, mode=4.2)

#: Distinct menu items per order.
ITEMS_PER_ORDER: Range = Range(low=1.0, high=6.0, mode=2.0)

#: Menu size per restaurant. A module constant rather than an inline dataclass default,
#: which ruff flags (RUF009) because a call in a default is evaluated once at import.
MENU_ITEMS_PER_RESTAURANT: Range = Range(low=6.0, high=18.0, mode=11.0)


@dataclass(frozen=True, slots=True)
class SeedVolumes:
    """How much reference data to create. Small on purpose.

    The spec warns that a large initial Debezium snapshot never finishes; seeding a
    modest world keeps Phase 2's first snapshot quick.
    """

    customers: int = 500
    restaurants: int = 80
    riders: int = 120
    menu_items_per_restaurant: Range = MENU_ITEMS_PER_RESTAURANT

    def __post_init__(self) -> None:
        for field in ("customers", "restaurants", "riders"):
            value = getattr(self, field)
            if value <= 0:
                raise ValueError(f"{field} must be > 0, got {value}")
        if self.menu_items_per_restaurant.low < 1:
            raise ValueError("every restaurant needs at least one menu item")


@dataclass(frozen=True, slots=True)
class LoadConfig:
    """Throughput knobs for the running generator."""

    orders_per_day: int = 50_000
    speed: float = DEFAULT_SPEED
    gps_ping_interval_s: float = 5.0
    gps_max_msgs_per_second: int = 200

    def __post_init__(self) -> None:
        if self.orders_per_day <= 0:
            raise ValueError(f"orders_per_day must be > 0, got {self.orders_per_day}")
        if self.speed <= 0:
            raise ValueError(f"speed must be > 0, got {self.speed}")
        if self.gps_ping_interval_s <= 0:
            raise ValueError(f"gps_ping_interval_s must be > 0, got {self.gps_ping_interval_s}")
        if self.gps_max_msgs_per_second <= 0:
            raise ValueError(
                f"gps_max_msgs_per_second must be > 0, got {self.gps_max_msgs_per_second}"
            )

    @property
    def orders_per_second(self) -> float:
        """Placement rate in wall-clock seconds, after speed compression.

        At the default 50k/day and 20x, that is ~11.6 orders/second — enough to build a
        realistic status distribution within a few minutes.
        """
        return self.orders_per_day / 86_400.0 * self.speed


def pick_city(rng: random.Random, cities: tuple[City, ...] = CITIES) -> City:
    """Choose a city weighted by its share of order volume."""
    if not cities:
        raise ValueError("cities must not be empty")
    return rng.choices(cities, weights=[c.weight for c in cities], k=1)[0]
