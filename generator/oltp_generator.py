"""The OLTP generator daemon.

A single-threaded event loop over three independent sources, merged by due time:

* **Arrivals** — a Poisson process at `LoadConfig.orders_per_second`. Exponential
  inter-arrival gaps, not fixed spacing: a perfectly regular stream would let Phase 3's
  watermarking and late-arrival handling pass against a distribution that never occurs.
* **Transitions** — a min-heap keyed on each order's `next_due_at`, from `state_machine`.
* **Dimension mutations** — menu prices, restaurant open/closed, rider tier and online flag,
  on their own slower timer. These produce no orders; they exist so Phase 3's SCD2
  dimensions have real history to track.

**Why the scheduler is in memory.** There is no `next_due_at` column on `orders`, and there
should not be — it is generator bookkeeping, not business data, and it would push a column
into the CDC stream that every downstream layer would have to ignore. So the heap lives
here and Postgres is write-only in the hot path.

The cost is that a restart would strand in-flight orders. `recover()` handles that by
reloading non-terminal orders and rescheduling them, which is what a real system has to do
too.

Arrival rate is deliberately independent of how many orders are in flight. Letting a
backlog throttle new arrivals would be backwards: load does not politely wait.
"""

from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from generator.config import (
    DEFAULT_RNG_SEED,
    ITEMS_PER_ORDER,
    PAYMENT_METHOD_WEIGHTS,
    PAYMENT_METHODS,
    RIDER_TIER_WEIGHTS,
    RIDER_TIERS,
    LoadConfig,
    Range,
)
from generator.repository import Catalog, NewOrder, OrderItem, Repository
from generator.state_machine import (
    DEFAULT_CONFIG,
    MachineConfig,
    OrderState,
    OrderStatus,
    Transition,
    advance,
    apply_transition,
    is_terminal,
    place_order,
    rule_for,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

__all__ = [
    "Clock",
    "GeneratorReport",
    "ManualClock",
    "OltpGenerator",
    "RealClock",
]

#: Delivery fee, in INR.
DELIVERY_FEE_INR = Range(low=19.0, high=79.0, mode=35.0)

#: Discount as a fraction of subtotal, when one applies at all.
DISCOUNT_FRACTION = Range(low=0.05, high=0.40, mode=0.12)
DISCOUNT_PROBABILITY = 0.22

#: Per-item quantity.
QTY = Range(low=1.0, high=4.0, mode=1.0)

#: How often a dimension mutation fires, in simulated seconds before speed compression.
MUTATION_INTERVAL_S = 90.0

#: Relative frequency of each kind of dimension mutation.
_MUTATION_KINDS = ("menu_price", "restaurant_open", "rider")
_MUTATION_WEIGHTS = (0.5, 0.2, 0.3)

#: Longest the loop will sleep in one go, so shutdown stays responsive.
MAX_TICK_S = 0.25


class Clock(Protocol):
    def now(self) -> datetime: ...
    def sleep(self, seconds: float) -> None: ...


class RealClock:
    """Wall-clock time."""

    def now(self) -> datetime:
        return datetime.now(UTC)

    def sleep(self, seconds: float) -> None:
        import time

        time.sleep(max(0.0, seconds))


class ManualClock:
    """A clock the tests drive. `sleep` jumps forward instead of waiting."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        self._now += timedelta(seconds=max(0.0, seconds))

    def advance(self, seconds: float) -> None:
        self.sleep(seconds)


@dataclass
class GeneratorReport:
    """What a run actually did. Returned rather than logged so tests can assert on it."""

    orders_placed: int = 0
    transitions: int = 0
    delivered: int = 0
    cancelled: int = 0
    mutations: dict[str, int] = field(default_factory=dict)
    recovered: int = 0

    @property
    def terminal(self) -> int:
        return self.delivered + self.cancelled

    @property
    def total_mutations(self) -> int:
        return sum(self.mutations.values())


@dataclass(order=True)
class _Scheduled:
    """Heap entry. `seq` breaks ties so datetimes never have to compare OrderState."""

    due_at: datetime
    seq: int
    order_id: int = field(compare=False)


class OltpGenerator:
    def __init__(
        self,
        repository: Repository,
        catalog: Catalog | None = None,
        load: LoadConfig | None = None,
        machine: MachineConfig | None = None,
        rng: random.Random | None = None,
        clock: Clock | None = None,
        transition_hook: Callable[[Transition], Transition] | None = None,
    ) -> None:
        # The single seam chaos uses. None means every transition passes through untouched,
        # so a run without --chaos is byte-identical to one before chaos existed.
        self.transition_hook = transition_hook
        self.repo = repository
        self.load = load or LoadConfig()
        self.machine = machine or DEFAULT_CONFIG
        self.rng = rng or random.Random(DEFAULT_RNG_SEED)
        self.clock = clock or RealClock()
        self.catalog = catalog if catalog is not None else repository.load_catalog()

        self.report = GeneratorReport()
        self._states: dict[int, OrderState] = {}
        self._heap: list[_Scheduled] = []
        self._seq = 0
        self._payment_status: dict[int, str] = {}

        now = self.clock.now()
        self._next_arrival_at = now + self._arrival_gap()
        self._next_mutation_at = now + self._mutation_gap()

        # Restaurants that can actually sell something.
        self._sellable = tuple(
            r
            for r in self.catalog.restaurants
            if any(i.is_available for i in self.catalog.menu_by_restaurant.get(r.restaurant_id, ()))
        )
        if not self._sellable:
            raise ValueError("no restaurant has an available menu item")

    # ---------------------------------------------------------------- timing

    def _arrival_gap(self) -> timedelta:
        """Exponential inter-arrival time — a Poisson arrival process."""
        return timedelta(seconds=self.rng.expovariate(self.load.orders_per_second))

    def _mutation_gap(self) -> timedelta:
        return timedelta(seconds=MUTATION_INTERVAL_S / self.load.speed)

    def _schedule(self, state: OrderState) -> None:
        if state.next_due_at is None:
            return
        self._seq += 1
        heapq.heappush(self._heap, _Scheduled(state.next_due_at, self._seq, state.order_id))

    def _next_event_at(self) -> datetime:
        candidates = [self._next_arrival_at, self._next_mutation_at]
        if self._heap:
            candidates.append(self._heap[0].due_at)
        return min(candidates)

    # ---------------------------------------------------------------- recovery

    def recover(self) -> int:
        """Reschedule non-terminal orders left behind by a previous run.

        Their remaining delay is drawn fresh rather than reconstructed: the original draw
        was never persisted, and inventing a precise one would be fiction.
        """
        now = self.clock.now()
        recovered = 0
        for open_order in self.repo.load_open_orders():
            status = OrderStatus(open_order.status)
            if is_terminal(status):
                continue
            rule = rule_for(status, self.machine)
            state = OrderState(
                order_id=open_order.order_id,
                status=status,
                placed_ts=open_order.placed_ts,
                promised_ts=open_order.promised_ts,
                accepted_ts=open_order.accepted_ts,
                picked_up_ts=open_order.picked_up_ts,
                next_due_at=now + rule.delay.sample(self.rng, self.machine.speed),
            )
            self._states[state.order_id] = state
            self._schedule(state)
            recovered += 1

        self.report.recovered = recovered
        return recovered

    # ---------------------------------------------------------------- order construction

    def _build_order(self, now: datetime) -> NewOrder:
        restaurant = self.rng.choice(self._sellable)
        available = [
            i for i in self.catalog.menu_by_restaurant[restaurant.restaurant_id] if i.is_available
        ]
        wanted = min(ITEMS_PER_ORDER.sample_int(self.rng), len(available))
        chosen = self.rng.sample(available, max(1, wanted))

        items = tuple(
            OrderItem(
                menu_item_id=item.menu_item_id,
                qty=QTY.sample_int(self.rng),
                unit_price_inr=item.price_inr,
            )
            for item in chosen
        )
        subtotal = round(sum(i.qty * i.unit_price_inr for i in items), 2)
        delivery_fee = round(DELIVERY_FEE_INR.sample(self.rng), 2)
        discount = 0.0
        if self.rng.random() < DISCOUNT_PROBABILITY:
            discount = round(subtotal * DISCOUNT_FRACTION.sample(self.rng), 2)
        # The DDL has CHECK (total_inr >= 0); a discount must never drive it negative.
        total = round(max(0.0, subtotal + delivery_fee - discount), 2)

        promised = place_order(0, now, self.rng, self.machine).promised_ts
        return NewOrder(
            customer_id=self.rng.choice(self.catalog.customer_ids),
            restaurant_id=restaurant.restaurant_id,
            placed_ts=now,
            promised_ts=promised,
            subtotal_inr=subtotal,
            delivery_fee_inr=delivery_fee,
            discount_inr=discount,
            total_inr=total,
            items=items,
            payment_method=self.rng.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS, k=1)[
                0
            ],
        )

    # ---------------------------------------------------------------- events

    def _do_arrival(self, now: datetime) -> None:
        new_order = self._build_order(now)
        order_id = self.repo.place_order(new_order)

        state = place_order(order_id, now, self.rng, self.machine)
        state = OrderState(
            order_id=order_id,
            status=state.status,
            placed_ts=now,
            promised_ts=new_order.promised_ts,
            next_due_at=state.next_due_at,
        )
        self._states[order_id] = state
        self._schedule(state)
        self._payment_status[order_id] = "PENDING"

        self.report.orders_placed += 1
        self._next_arrival_at = now + self._arrival_gap()

    def _do_transition(self, order_id: int, now: datetime) -> None:
        state = self._states.get(order_id)
        if state is None or is_terminal(state.status):
            return

        transition = advance(state, now, self.rng, self.machine)
        if transition is None:
            return
        if self.transition_hook is not None:
            transition = self.transition_hook(transition)

        # A rider is assigned at the moment the restaurant accepts, not before.
        rider_id = (
            self.rng.choice(self.catalog.rider_ids)
            if transition.to_status is OrderStatus.ACCEPTED
            else None
        )

        self.repo.advance_order(
            order_id=order_id,
            status=str(transition.to_status),
            stamp_field=transition.stamps,
            stamp_ts=transition.occurred_at,
            rider_id=rider_id,
            cancel_reason=transition.cancel_reason,
            updated_at=transition.occurred_at,
        )

        if transition.to_status is OrderStatus.DELIVERED:
            self.repo.settle_payment(order_id, "CAPTURED", transition.occurred_at)
            self._payment_status[order_id] = "CAPTURED"
            self.report.delivered += 1
        elif transition.to_status is OrderStatus.CANCELLED:
            # Money only comes back if it was ever taken. Before ACCEPTED nothing was
            # captured, so the payment fails rather than refunds.
            settled = "REFUNDED" if transition.from_status is not OrderStatus.PLACED else "FAILED"
            self.repo.settle_payment(order_id, settled, transition.occurred_at)
            self._payment_status[order_id] = settled
            self.report.cancelled += 1

        new_state = apply_transition(state, transition)
        self.report.transitions += 1

        if is_terminal(new_state.status):
            del self._states[order_id]
        else:
            self._states[order_id] = new_state
            self._schedule(new_state)

    def _do_mutation(self, now: datetime) -> None:
        kind = self.rng.choices(_MUTATION_KINDS, weights=_MUTATION_WEIGHTS, k=1)[0]

        if kind == "menu_price":
            restaurant = self.rng.choice(self._sellable)
            item = self.rng.choice(self.catalog.menu_by_restaurant[restaurant.restaurant_id])
            # +/-5-15%, which is what gives dim_menu_item_scd2 a price history.
            factor = 1.0 + self.rng.choice((-1, 1)) * self.rng.uniform(0.05, 0.15)
            self.repo.update_menu_price(
                item.menu_item_id, round(max(1.0, item.price_inr * factor), 2), now
            )
        elif kind == "restaurant_open":
            restaurant = self.rng.choice(self.catalog.restaurants)
            self.repo.set_restaurant_open(restaurant.restaurant_id, not restaurant.is_open, now)
        else:
            rider_id = self.rng.choice(self.catalog.rider_ids)
            self.repo.update_rider(
                rider_id,
                self.rng.choices(RIDER_TIERS, weights=RIDER_TIER_WEIGHTS, k=1)[0],
                self.rng.random() < 0.6,
                now,
            )

        self.report.mutations[kind] = self.report.mutations.get(kind, 0) + 1
        self._next_mutation_at = now + self._mutation_gap()

    # ---------------------------------------------------------------- the loop

    def step(self) -> str | None:
        """Handle the single earliest due event. Returns its kind, or None if none is due.

        Separate from `run` so tests can drive the loop one event at a time.
        """
        now = self.clock.now()
        due_at = self._next_event_at()
        if due_at > now:
            return None

        if self._heap and self._heap[0].due_at <= min(
            self._next_arrival_at, self._next_mutation_at
        ):
            entry = heapq.heappop(self._heap)
            self._do_transition(entry.order_id, now)
            return "transition"
        if self._next_arrival_at <= self._next_mutation_at:
            self._do_arrival(now)
            return "arrival"
        self._do_mutation(now)
        return "mutation"

    def run(
        self,
        max_orders: int | None = None,
        max_events: int | None = None,
        until: datetime | None = None,
    ) -> GeneratorReport:
        """Run until a stopping condition is met. At least one must be given.

        `max_orders` counts placements, not events; in-flight orders are still advanced
        after the last placement so the run does not end with everything stuck in PLACED.
        """
        if max_orders is None and max_events is None and until is None:
            raise ValueError("run() needs max_orders, max_events or until")

        events = 0
        while True:
            if max_events is not None and events >= max_events:
                break
            if until is not None and self.clock.now() >= until:
                break
            if max_orders is not None and self.report.orders_placed >= max_orders:
                # Stop arriving, but drain what is already in flight.
                if not self._heap:
                    break
                self._next_arrival_at = datetime.max.replace(tzinfo=UTC)
                self._next_mutation_at = datetime.max.replace(tzinfo=UTC)

            kind = self.step()
            if kind is None:
                gap = (self._next_event_at() - self.clock.now()).total_seconds()
                self.clock.sleep(min(gap, MAX_TICK_S))
                continue
            events += 1

        return self.report

    # ---------------------------------------------------------------- introspection

    @property
    def in_flight(self) -> int:
        return len(self._states)

    def open_states(self) -> Iterator[OrderState]:
        return iter(self._states.values())
