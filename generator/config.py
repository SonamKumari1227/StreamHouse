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
    "Dish",
    "LoadConfig",
    "Range",
    "SeedVolumes",
    "dish_names",
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


@dataclass(frozen=True, slots=True)
class Dish:
    """A menu item and its own price band, in INR.

    Prices are per dish rather than one distribution across the whole menu. A single flat
    band produced a naan costing more than a biryani, which is harmless for pipeline
    mechanics and immediately silly in a screenshot.
    """

    name: str
    low_inr: float
    high_inr: float
    mode_inr: float

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("dish name must not be empty")
        if self.low_inr <= 0:
            raise ValueError(f"{self.name}: low_inr must be > 0, got {self.low_inr}")
        if not self.low_inr <= self.mode_inr <= self.high_inr:
            raise ValueError(
                f"{self.name}: require low <= mode <= high, got "
                f"{self.low_inr} / {self.mode_inr} / {self.high_inr}"
            )

    def sample_price(self, rng: random.Random) -> float:
        return round(rng.triangular(self.low_inr, self.high_inr, self.mode_inr), 2)


def dish_names(cuisine: str) -> tuple[str, ...]:
    return tuple(d.name for d in DISHES[cuisine])


#: Menu vocabulary per cuisine, each dish carrying its own price band. Every cuisine in
#: CUISINES must have an entry, and every band must sit inside MENU_PRICE_INR; tests enforce
#: both, so adding a cuisine cannot silently produce restaurants with no menu.
DISHES: dict[str, tuple[Dish, ...]] = {
    "North Indian": (
        Dish("Paneer Butter Masala", 220, 380, 280),
        Dish("Dal Makhani", 180, 320, 240),
        Dish("Chole Bhature", 120, 240, 170),
        Dish("Rajma Chawal", 130, 250, 180),
        Dish("Butter Naan", 40, 90, 55),
        Dish("Kadhai Paneer", 230, 390, 290),
        Dish("Aloo Paratha", 70, 150, 100),
        Dish("Malai Kofta", 220, 370, 285),
    ),
    "South Indian": (
        Dish("Masala Dosa", 90, 190, 130),
        Dish("Idli Sambar", 60, 140, 90),
        Dish("Medu Vada", 60, 130, 85),
        Dish("Rava Upma", 60, 130, 85),
        Dish("Filter Coffee", 40, 90, 55),
        Dish("Pongal", 80, 160, 110),
        Dish("Uttapam", 90, 180, 125),
        Dish("Lemon Rice", 70, 150, 100),
    ),
    "Biryani": (
        Dish("Hyderabadi Chicken Biryani", 260, 480, 340),
        Dish("Mutton Biryani", 340, 620, 430),
        Dish("Veg Dum Biryani", 190, 340, 240),
        Dish("Egg Biryani", 180, 320, 230),
        Dish("Prawn Biryani", 320, 580, 410),
        Dish("Mirchi ka Salan", 70, 150, 100),
        Dish("Double ka Meetha", 80, 170, 115),
    ),
    "Chinese": (
        Dish("Veg Hakka Noodles", 140, 260, 185),
        Dish("Chilli Paneer", 190, 340, 250),
        Dish("Schezwan Fried Rice", 150, 280, 200),
        Dish("Manchurian Gravy", 170, 300, 225),
        Dish("Spring Rolls", 110, 220, 155),
        Dish("Chicken Lollipop", 200, 360, 265),
        Dish("Hot and Sour Soup", 100, 200, 140),
    ),
    "Pizza": (
        Dish("Margherita", 180, 340, 240),
        Dish("Farmhouse", 280, 520, 370),
        Dish("Peppy Paneer", 290, 540, 385),
        Dish("Chicken Tikka Pizza", 320, 600, 430),
        Dish("Cheese Garlic Bread", 110, 220, 155),
        Dish("Pepperoni", 340, 640, 460),
        Dish("Veggie Supreme", 300, 560, 400),
    ),
    "Burgers": (
        Dish("Aloo Tikki Burger", 60, 130, 85),
        Dish("Chicken Maharaja", 180, 330, 245),
        Dish("Veg Cheese Burger", 100, 200, 140),
        Dish("Crispy Chicken Burger", 150, 280, 205),
        Dish("Peri Peri Fries", 90, 180, 125),
        Dish("Double Patty Burger", 200, 370, 270),
    ),
    "Street Food": (
        Dish("Pani Puri", 40, 100, 60),
        Dish("Bhel Puri", 50, 110, 70),
        Dish("Vada Pav", 25, 70, 40),
        Dish("Pav Bhaji", 90, 190, 130),
        Dish("Samosa Chaat", 60, 130, 85),
        Dish("Dahi Puri", 60, 130, 85),
        Dish("Momos", 90, 190, 130),
        Dish("Kathi Roll", 110, 220, 155),
    ),
    "Desserts": (
        Dish("Gulab Jamun", 60, 140, 90),
        Dish("Rasmalai", 80, 170, 115),
        Dish("Chocolate Brownie", 100, 210, 145),
        Dish("Gajar ka Halwa", 90, 190, 130),
        Dish("Tiramisu", 180, 340, 245),
        Dish("Kulfi Falooda", 90, 190, 130),
        Dish("Cheesecake Slice", 170, 330, 235),
    ),
    "Healthy": (
        Dish("Quinoa Bowl", 220, 400, 300),
        Dish("Grilled Chicken Salad", 240, 430, 320),
        Dish("Sprouts Salad", 120, 230, 165),
        Dish("Oats Smoothie Bowl", 180, 330, 240),
        Dish("Paneer Tikka Salad", 210, 380, 285),
        Dish("Fruit Bowl", 110, 220, 155),
    ),
    "Cafe": (
        Dish("Cappuccino", 130, 260, 180),
        Dish("Cold Coffee", 140, 270, 190),
        Dish("Chicken Sandwich", 160, 300, 220),
        Dish("Veg Club Sandwich", 140, 270, 195),
        Dish("Blueberry Muffin", 90, 190, 130),
        Dish("Croissant", 100, 210, 145),
        Dish("Iced Latte", 150, 290, 205),
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

#: The overall envelope every per-dish band must sit inside. Not used to price anything —
#: it exists so a badly-entered Dish band fails a test instead of reaching the database.
MENU_PRICE_INR: Range = Range(low=20.0, high=700.0, mode=189.0)

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
