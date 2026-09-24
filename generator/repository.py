"""Database access for the OLTP generator.

The generator talks to Postgres only through `Repository`. That boundary is what lets the
daemon's scheduling and money logic be unit-tested against an in-memory fake, while the SQL
itself is proven separately against a real database.

Write order matters and is enforced here rather than left to the caller:

1. `orders` (payment_id NULL, rider_id NULL)
2. `order_items`
3. `payments` — has a foreign key to orders, so the order must exist first
4. `orders.payment_id` — a second UPDATE, which is also what a real system does

That second update is not waste. It produces an extra CDC event per order, which is exactly
the kind of multi-event-per-entity traffic Phase 3 has to deduplicate correctly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "Catalog",
    "MenuItem",
    "NewOrder",
    "OpenOrder",
    "OrderItem",
    "PostgresRepository",
    "Repository",
    "Restaurant",
]


@dataclass(frozen=True, slots=True)
class MenuItem:
    menu_item_id: int
    restaurant_id: int
    price_inr: float
    is_available: bool


@dataclass(frozen=True, slots=True)
class Restaurant:
    restaurant_id: int
    city: str
    is_open: bool


@dataclass(frozen=True, slots=True)
class Catalog:
    """The reference world, loaded once at startup.

    Held in memory because the generator reads it constantly and it changes only when the
    generator itself mutates it. Re-querying per order would add load that says nothing
    interesting about the pipeline.
    """

    customer_ids: tuple[int, ...]
    restaurants: tuple[Restaurant, ...]
    menu_by_restaurant: dict[int, tuple[MenuItem, ...]]
    rider_ids: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.customer_ids:
            raise ValueError("catalog has no customers; run the seeder first")
        if not self.restaurants:
            raise ValueError("catalog has no restaurants; run the seeder first")
        if not self.rider_ids:
            raise ValueError("catalog has no riders; run the seeder first")
        sellable = [r for r in self.restaurants if self.menu_by_restaurant.get(r.restaurant_id)]
        if not sellable:
            raise ValueError("no restaurant has any menu items; run the seeder first")


@dataclass(frozen=True, slots=True)
class OrderItem:
    menu_item_id: int
    qty: int
    unit_price_inr: float


@dataclass(frozen=True, slots=True)
class NewOrder:
    customer_id: int
    restaurant_id: int
    placed_ts: datetime
    promised_ts: datetime
    subtotal_inr: float
    delivery_fee_inr: float
    discount_inr: float
    total_inr: float
    items: tuple[OrderItem, ...]
    payment_method: str

    def __post_init__(self) -> None:
        if not self.items:
            raise ValueError("an order must have at least one item")
        if self.total_inr < 0:
            raise ValueError(f"total_inr must be >= 0, got {self.total_inr}")


@dataclass(frozen=True, slots=True)
class OpenOrder:
    """A non-terminal order recovered from the database at startup."""

    order_id: int
    status: str
    placed_ts: datetime
    promised_ts: datetime
    accepted_ts: datetime | None
    picked_up_ts: datetime | None


class Repository(Protocol):
    """Everything the generator needs from storage."""

    def load_catalog(self) -> Catalog: ...

    def load_open_orders(self) -> tuple[OpenOrder, ...]: ...

    def place_order(self, order: NewOrder) -> int:
        """Insert order, items and payment. Returns the new order_id."""
        ...

    def advance_order(
        self,
        order_id: int,
        status: str,
        stamp_field: str | None,
        stamp_ts: datetime | None,
        rider_id: int | None,
        cancel_reason: str | None,
        updated_at: datetime,
    ) -> None: ...

    def settle_payment(self, order_id: int, status: str, updated_at: datetime) -> None: ...

    def update_menu_price(
        self, menu_item_id: int, price_inr: float, updated_at: datetime
    ) -> None: ...

    def set_restaurant_open(
        self, restaurant_id: int, is_open: bool, updated_at: datetime
    ) -> None: ...

    def update_rider(
        self, rider_id: int, tier: str, is_online: bool, updated_at: datetime
    ) -> None: ...


class _Cursor(Protocol):
    def execute(self, query: str, params: Sequence[Any] | None = ...) -> Any: ...
    def executemany(self, query: str, params_seq: Sequence[Any]) -> Any: ...
    def fetchone(self) -> Any: ...
    def fetchall(self) -> Any: ...


#: Columns stamped by each transition. Mirrors state_machine.Rule.stamps, and the generator
#: passes the field name through rather than deciding it here.
_STAMPABLE: frozenset[str] = frozenset({"accepted_ts", "picked_up_ts", "delivered_ts"})


class PostgresRepository:
    """`Repository` over a psycopg cursor.

    Does not own the connection or the transaction. The caller decides when to commit, which
    lets tests wrap a whole run in a transaction and roll it back.
    """

    def __init__(self, cursor: _Cursor) -> None:
        self._cur = cursor

    # ---------------------------------------------------------------- reads

    def load_catalog(self) -> Catalog:
        self._cur.execute("SELECT customer_id FROM customers WHERE is_active ORDER BY customer_id")
        customer_ids = tuple(int(r[0]) for r in self._cur.fetchall())

        self._cur.execute("SELECT restaurant_id, city, is_open FROM restaurants ORDER BY 1")
        restaurants = tuple(
            Restaurant(restaurant_id=int(r[0]), city=str(r[1]), is_open=bool(r[2]))
            for r in self._cur.fetchall()
        )

        self._cur.execute(
            "SELECT menu_item_id, restaurant_id, price_inr, is_available "
            "FROM menu_items ORDER BY restaurant_id, menu_item_id"
        )
        menu: dict[int, list[MenuItem]] = {}
        for row in self._cur.fetchall():
            item = MenuItem(
                menu_item_id=int(row[0]),
                restaurant_id=int(row[1]),
                price_inr=float(row[2]),
                is_available=bool(row[3]),
            )
            menu.setdefault(item.restaurant_id, []).append(item)

        self._cur.execute("SELECT rider_id FROM riders ORDER BY rider_id")
        rider_ids = tuple(int(r[0]) for r in self._cur.fetchall())

        return Catalog(
            customer_ids=customer_ids,
            restaurants=restaurants,
            menu_by_restaurant={k: tuple(v) for k, v in menu.items()},
            rider_ids=rider_ids,
        )

    def load_open_orders(self) -> tuple[OpenOrder, ...]:
        """Non-terminal orders left behind by a previous run.

        Without this, restarting the generator would strand every in-flight order in a
        non-terminal state forever, and Phase 3 would see dimensions that never close.
        """
        self._cur.execute(
            """
            SELECT order_id, status, placed_ts, promised_ts, accepted_ts, picked_up_ts
            FROM orders
            WHERE status NOT IN ('DELIVERED', 'CANCELLED')
            ORDER BY order_id
            """
        )
        return tuple(
            OpenOrder(
                order_id=int(r[0]),
                status=str(r[1]),
                placed_ts=r[2],
                promised_ts=r[3],
                accepted_ts=r[4],
                picked_up_ts=r[5],
            )
            for r in self._cur.fetchall()
        )

    # ---------------------------------------------------------------- writes

    def place_order(self, order: NewOrder) -> int:
        self._cur.execute(
            """
            INSERT INTO orders
                (customer_id, restaurant_id, status, placed_ts, promised_ts,
                 subtotal_inr, delivery_fee_inr, discount_inr, total_inr, updated_at)
            VALUES (%s, %s, 'PLACED', %s, %s, %s, %s, %s, %s, %s)
            RETURNING order_id
            """,
            (
                order.customer_id,
                order.restaurant_id,
                order.placed_ts,
                order.promised_ts,
                order.subtotal_inr,
                order.delivery_fee_inr,
                order.discount_inr,
                order.total_inr,
                order.placed_ts,
            ),
        )
        row = self._cur.fetchone()
        order_id = int(row[0])

        self._cur.executemany(
            """
            INSERT INTO order_items (order_id, menu_item_id, qty, unit_price_inr, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (order_id, i.menu_item_id, i.qty, i.unit_price_inr, order.placed_ts)
                for i in order.items
            ],
        )

        self._cur.execute(
            """
            INSERT INTO payments (order_id, method, status, amount_inr, updated_at)
            VALUES (%s, %s, 'PENDING', %s, %s)
            RETURNING payment_id
            """,
            (order_id, order.payment_method, order.total_inr, order.placed_ts),
        )
        payment_row = self._cur.fetchone()

        self._cur.execute(
            "UPDATE orders SET payment_id = %s, updated_at = %s WHERE order_id = %s",
            (int(payment_row[0]), order.placed_ts, order_id),
        )
        return order_id

    def advance_order(
        self,
        order_id: int,
        status: str,
        stamp_field: str | None,
        stamp_ts: datetime | None,
        rider_id: int | None,
        cancel_reason: str | None,
        updated_at: datetime,
    ) -> None:
        sets = ["status = %s", "updated_at = %s"]
        params: list[Any] = [status, updated_at]

        if stamp_field is not None:
            if stamp_field not in _STAMPABLE:
                raise ValueError(f"refusing to stamp unknown column {stamp_field!r}")
            sets.append(f"{stamp_field} = %s")
            params.append(stamp_ts)
        if rider_id is not None:
            sets.append("rider_id = %s")
            params.append(rider_id)
        if cancel_reason is not None:
            sets.append("cancel_reason = %s")
            params.append(cancel_reason)

        params.append(order_id)
        self._cur.execute(
            f"UPDATE orders SET {', '.join(sets)} WHERE order_id = %s",
            params,
        )

    def settle_payment(self, order_id: int, status: str, updated_at: datetime) -> None:
        self._cur.execute(
            "UPDATE payments SET status = %s, gateway_ref = %s, updated_at = %s "
            "WHERE order_id = %s",
            (status, f"gw_{order_id:012d}", updated_at, order_id),
        )

    def update_menu_price(self, menu_item_id: int, price_inr: float, updated_at: datetime) -> None:
        self._cur.execute(
            "UPDATE menu_items SET price_inr = %s, updated_at = %s WHERE menu_item_id = %s",
            (price_inr, updated_at, menu_item_id),
        )

    def set_restaurant_open(self, restaurant_id: int, is_open: bool, updated_at: datetime) -> None:
        self._cur.execute(
            "UPDATE restaurants SET is_open = %s, updated_at = %s WHERE restaurant_id = %s",
            (is_open, updated_at, restaurant_id),
        )

    def update_rider(self, rider_id: int, tier: str, is_online: bool, updated_at: datetime) -> None:
        self._cur.execute(
            "UPDATE riders SET tier = %s, is_online = %s, updated_at = %s WHERE rider_id = %s",
            (tier, is_online, updated_at, rider_id),
        )
