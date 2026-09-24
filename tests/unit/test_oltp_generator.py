"""Unit tests for the OLTP generator daemon.

A fake repository records every call, and `ManualClock` makes the loop run in zero wall-clock
time. What the fake cannot prove — that the SQL is valid, that foreign keys and CHECK
constraints hold, that rows actually land — is covered in
tests/integration/test_oltp_generator_writes.py.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any

import pytest

from generator.config import PAYMENT_METHODS, LoadConfig
from generator.oltp_generator import ManualClock, OltpGenerator, RealClock
from generator.repository import (
    Catalog,
    MenuItem,
    NewOrder,
    OpenOrder,
    OrderItem,
    Restaurant,
)
from generator.state_machine import MachineConfig, OrderState, OrderStatus

T0 = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)


def make_catalog(restaurants: int = 5, items_each: int = 4, riders: int = 6) -> Catalog:
    menu: dict[int, tuple[MenuItem, ...]] = {}
    item_id = 0
    for r in range(1, restaurants + 1):
        items = []
        for _ in range(items_each):
            item_id += 1
            items.append(
                MenuItem(
                    menu_item_id=item_id,
                    restaurant_id=r,
                    price_inr=100.0 + item_id,
                    is_available=True,
                )
            )
        menu[r] = tuple(items)
    return Catalog(
        customer_ids=tuple(range(1, 21)),
        restaurants=tuple(
            Restaurant(restaurant_id=r, city="Bengaluru", is_open=True)
            for r in range(1, restaurants + 1)
        ),
        menu_by_restaurant=menu,
        rider_ids=tuple(range(1, riders + 1)),
    )


@dataclass
class FakeRepo:
    """Records calls and models just enough state to answer the generator's reads."""

    catalog: Catalog = field(default_factory=make_catalog)
    open_orders: tuple[OpenOrder, ...] = ()
    placed: list[NewOrder] = field(default_factory=list)
    advances: list[dict[str, Any]] = field(default_factory=list)
    settlements: list[tuple[int, str]] = field(default_factory=list)
    price_updates: list[tuple[int, float]] = field(default_factory=list)
    open_updates: list[tuple[int, bool]] = field(default_factory=list)
    rider_updates: list[tuple[int, str, bool]] = field(default_factory=list)
    _next_id: int = 0

    def load_catalog(self) -> Catalog:
        return self.catalog

    def load_open_orders(self) -> tuple[OpenOrder, ...]:
        return self.open_orders

    def place_order(self, order: NewOrder) -> int:
        self._next_id += 1
        self.placed.append(order)
        return self._next_id

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
        self.advances.append(
            {
                "order_id": order_id,
                "status": status,
                "stamp_field": stamp_field,
                "stamp_ts": stamp_ts,
                "rider_id": rider_id,
                "cancel_reason": cancel_reason,
                "updated_at": updated_at,
            }
        )

    def settle_payment(self, order_id: int, status: str, updated_at: datetime) -> None:
        self.settlements.append((order_id, status))

    def update_menu_price(self, menu_item_id: int, price_inr: float, updated_at: datetime) -> None:
        self.price_updates.append((menu_item_id, price_inr))

    def set_restaurant_open(self, restaurant_id: int, is_open: bool, updated_at: datetime) -> None:
        self.open_updates.append((restaurant_id, is_open))

    def update_rider(self, rider_id: int, tier: str, is_online: bool, updated_at: datetime) -> None:
        self.rider_updates.append((rider_id, tier, is_online))


def make_generator(
    repo: FakeRepo | None = None,
    seed: int = 1234,
    speed: float = 20.0,
    orders_per_day: int = 50_000,
) -> tuple[OltpGenerator, FakeRepo, ManualClock]:
    repo = repo or FakeRepo()
    clock = ManualClock(T0)
    gen = OltpGenerator(
        repository=repo,
        catalog=repo.catalog,
        load=LoadConfig(orders_per_day=orders_per_day, speed=speed),
        machine=MachineConfig(speed=speed),
        rng=random.Random(seed),
        clock=clock,
    )
    return gen, repo, clock


# --------------------------------------------------------------------------- clock


def test_manual_clock_jumps_instead_of_waiting() -> None:
    clock = ManualClock(T0)
    clock.sleep(30)
    assert clock.now() == T0 + timedelta(seconds=30)


def test_manual_clock_ignores_negative_sleep() -> None:
    clock = ManualClock(T0)
    clock.sleep(-5)
    assert clock.now() == T0


def test_real_clock_is_timezone_aware() -> None:
    now = RealClock().now()
    assert now.tzinfo is not None
    RealClock().sleep(0)


# --------------------------------------------------------------------------- construction


def test_generator_rejects_a_catalog_with_nothing_sellable() -> None:
    menu: dict[int, tuple[MenuItem, ...]] = {
        1: (MenuItem(menu_item_id=1, restaurant_id=1, price_inr=100.0, is_available=False),)
    }
    catalog = Catalog(
        customer_ids=(1,),
        restaurants=(Restaurant(restaurant_id=1, city="Pune", is_open=True),),
        menu_by_restaurant=menu,
        rider_ids=(1,),
    )
    with pytest.raises(ValueError, match="available menu item"):
        OltpGenerator(FakeRepo(catalog=catalog), catalog=catalog, clock=ManualClock(T0))


def test_generator_loads_the_catalog_when_not_given_one() -> None:
    repo = FakeRepo()
    gen = OltpGenerator(repo, clock=ManualClock(T0))
    assert gen.catalog is repo.catalog


@pytest.mark.parametrize(
    "kwargs",
    [{"customer_ids": ()}, {"restaurants": ()}, {"rider_ids": ()}, {"menu_by_restaurant": {}}],
    ids=["no customers", "no restaurants", "no riders", "no menus"],
)
def test_catalog_rejects_an_unseeded_world(kwargs: dict[str, Any]) -> None:
    base = make_catalog()
    fields = {
        "customer_ids": base.customer_ids,
        "restaurants": base.restaurants,
        "menu_by_restaurant": base.menu_by_restaurant,
        "rider_ids": base.rider_ids,
    }
    fields.update(kwargs)
    with pytest.raises(ValueError, match="seeder"):
        Catalog(**fields)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- arrivals


def test_placing_an_order_writes_it_and_schedules_a_transition() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_events=1)
    assert len(repo.placed) == 1
    assert gen.in_flight == 1


def test_orders_reference_only_catalogue_entities() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=60)
    valid_customers = set(repo.catalog.customer_ids)
    valid_restaurants = {r.restaurant_id for r in repo.catalog.restaurants}
    for order in repo.placed:
        assert order.customer_id in valid_customers
        assert order.restaurant_id in valid_restaurants
        menu_ids = {i.menu_item_id for i in repo.catalog.menu_by_restaurant[order.restaurant_id]}
        assert {i.menu_item_id for i in order.items} <= menu_ids


def test_order_money_adds_up_and_is_never_negative() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=120)
    assert repo.placed
    for order in repo.placed:
        expected_subtotal = round(sum(i.qty * i.unit_price_inr for i in order.items), 2)
        assert order.subtotal_inr == expected_subtotal
        assert order.total_inr >= 0
        assert order.delivery_fee_inr >= 0
        assert order.discount_inr >= 0
        assert order.total_inr == pytest.approx(
            max(0.0, order.subtotal_inr + order.delivery_fee_inr - order.discount_inr), abs=0.01
        )


def test_some_orders_are_discounted_and_some_are_not() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=200)
    discounted = sum(1 for o in repo.placed if o.discount_inr > 0)
    assert 0 < discounted < len(repo.placed)


def test_every_order_has_items_with_positive_quantities() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=80)
    for order in repo.placed:
        assert order.items
        assert all(i.qty > 0 for i in order.items)
        assert len({i.menu_item_id for i in order.items}) == len(order.items)


def test_payment_methods_come_from_the_allowed_set() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=120)
    assert {o.payment_method for o in repo.placed} <= set(PAYMENT_METHODS)


def test_promised_ts_is_after_placement() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=50)
    for order in repo.placed:
        assert order.promised_ts > order.placed_ts


def test_arrivals_are_poisson_not_evenly_spaced() -> None:
    """Fixed spacing would let Phase 3 watermarking pass against an unreal distribution."""
    gen, repo, _ = make_generator()
    gen.run(max_orders=300)
    gaps = [(b.placed_ts - a.placed_ts).total_seconds() for a, b in pairwise(repo.placed)]
    assert len(set(gaps)) > len(gaps) * 0.9, "inter-arrival gaps look fixed, not exponential"
    # An exponential distribution has stdev equal to its mean.
    mean = sum(gaps) / len(gaps)
    variance = sum((g - mean) ** 2 for g in gaps) / len(gaps)
    assert 0.6 < (variance**0.5) / mean < 1.6


def test_arrival_rate_tracks_the_configured_load() -> None:
    fast, fast_repo, _ = make_generator(orders_per_day=86_400, speed=1.0)
    fast.run(max_orders=400)
    span = (fast_repo.placed[-1].placed_ts - fast_repo.placed[0].placed_ts).total_seconds()
    observed = (len(fast_repo.placed) - 1) / span
    assert observed == pytest.approx(1.0, rel=0.25)


# --------------------------------------------------------------------------- lifecycle


def test_orders_progress_through_the_state_machine_to_terminal() -> None:
    gen, repo, _ = make_generator()
    report = gen.run(max_orders=150)
    assert report.orders_placed == 150
    assert report.terminal > 0
    assert report.delivered > 0
    assert report.cancelled > 0
    assert gen.in_flight == 0, "run() should drain in-flight orders before returning"


def test_every_transition_is_written_with_its_timestamp_column() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=100)
    stamps = {"ACCEPTED": "accepted_ts", "PICKED_UP": "picked_up_ts", "DELIVERED": "delivered_ts"}
    for call in repo.advances:
        if call["status"] in stamps:
            assert call["stamp_field"] == stamps[call["status"]]
            assert call["stamp_ts"] is not None
        else:
            assert call["status"] == "CANCELLED"
            assert call["stamp_field"] is None
            assert call["cancel_reason"] is not None


def test_a_rider_is_assigned_exactly_at_acceptance() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=100)
    valid = set(repo.catalog.rider_ids)
    for call in repo.advances:
        if call["status"] == "ACCEPTED":
            assert call["rider_id"] in valid
        else:
            assert call["rider_id"] is None


def test_delivered_orders_capture_payment_and_cancellations_do_not() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=150)
    by_order: dict[int, list[str]] = {}
    for order_id, status in repo.settlements:
        by_order.setdefault(order_id, []).append(status)

    final_status = {c["order_id"]: c["status"] for c in repo.advances}
    for order_id, settlements in by_order.items():
        if final_status[order_id] == "DELIVERED":
            assert settlements[-1] == "CAPTURED"
        else:
            assert settlements[-1] in {"REFUNDED", "FAILED"}


def test_cancelling_before_acceptance_fails_the_payment_rather_than_refunding() -> None:
    """Nothing was captured yet, so there is nothing to refund."""
    gen, repo, _ = make_generator()
    gen.run(max_orders=300)

    cancels = {c["order_id"]: c for c in repo.advances if c["status"] == "CANCELLED"}
    reached_accepted = {c["order_id"] for c in repo.advances if c["status"] == "ACCEPTED"}
    settle = {order_id: status for order_id, status in repo.settlements}

    saw_failed = saw_refunded = False
    for order_id in cancels:
        if order_id in reached_accepted:
            assert settle[order_id] == "REFUNDED"
            saw_refunded = True
        else:
            assert settle[order_id] == "FAILED"
            saw_failed = True
    assert saw_failed and saw_refunded, "both cancellation paths should occur in 300 orders"


def test_transition_timestamps_never_go_backwards() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=120)
    seen: dict[int, datetime] = {}
    for call in repo.advances:
        order_id = call["order_id"]
        if order_id in seen:
            assert call["updated_at"] >= seen[order_id]
        seen[order_id] = call["updated_at"]


# --------------------------------------------------------------------------- mutations


def test_dimension_mutations_fire_on_their_own_timer() -> None:
    gen, repo, _ = make_generator()
    report = gen.run(max_orders=400)
    assert report.total_mutations > 0
    assert repo.price_updates or repo.open_updates or repo.rider_updates


def test_menu_price_changes_stay_positive_and_move_by_a_sane_amount() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=600)
    original = {
        i.menu_item_id: i.price_inr
        for items in repo.catalog.menu_by_restaurant.values()
        for i in items
    }
    assert repo.price_updates
    for menu_item_id, price in repo.price_updates:
        assert price > 0
        ratio = price / original[menu_item_id]
        assert 0.84 <= ratio <= 1.16, f"price moved {ratio:.2f}x, expected within +/-15%"


def test_rider_mutations_use_valid_tiers() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=600)
    assert repo.rider_updates
    valid_riders = set(repo.catalog.rider_ids)
    for rider_id, tier, is_online in repo.rider_updates:
        assert rider_id in valid_riders
        assert tier in {"BRONZE", "SILVER", "GOLD"}
        assert isinstance(is_online, bool)


def test_restaurant_open_flag_is_flipped() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_orders=600)
    assert repo.open_updates
    current = {r.restaurant_id: r.is_open for r in repo.catalog.restaurants}
    for restaurant_id, is_open in repo.open_updates:
        assert is_open is not current[restaurant_id]


# --------------------------------------------------------------------------- recovery


def test_recover_reschedules_orders_left_in_flight() -> None:
    open_orders = (
        OpenOrder(
            order_id=901,
            status="ACCEPTED",
            placed_ts=T0 - timedelta(minutes=20),
            promised_ts=T0 + timedelta(minutes=20),
            accepted_ts=T0 - timedelta(minutes=18),
            picked_up_ts=None,
        ),
        OpenOrder(
            order_id=902,
            status="PLACED",
            placed_ts=T0 - timedelta(minutes=2),
            promised_ts=T0 + timedelta(minutes=30),
            accepted_ts=None,
            picked_up_ts=None,
        ),
    )
    gen, repo, _ = make_generator(repo=FakeRepo(open_orders=open_orders))
    assert gen.recover() == 2
    assert gen.in_flight == 2
    assert {s.order_id for s in gen.open_states()} == {901, 902}
    assert all(s.next_due_at is not None for s in gen.open_states())


def test_recover_skips_orders_that_already_finished() -> None:
    open_orders = (
        OpenOrder(
            order_id=903,
            status="DELIVERED",
            placed_ts=T0,
            promised_ts=T0,
            accepted_ts=T0,
            picked_up_ts=T0,
        ),
    )
    gen, _, _ = make_generator(repo=FakeRepo(open_orders=open_orders))
    assert gen.recover() == 0
    assert gen.in_flight == 0


def test_recovered_orders_are_driven_to_completion() -> None:
    open_orders = tuple(
        OpenOrder(
            order_id=900 + n,
            status="PLACED",
            placed_ts=T0 - timedelta(minutes=1),
            promised_ts=T0 + timedelta(minutes=30),
            accepted_ts=None,
            picked_up_ts=None,
        )
        for n in range(5)
    )
    gen, repo, _ = make_generator(repo=FakeRepo(open_orders=open_orders))
    gen.recover()
    gen.run(max_orders=1)
    assert {c["order_id"] for c in repo.advances} >= {900, 901, 902, 903, 904}


# --------------------------------------------------------------------------- run()


def test_run_requires_a_stopping_condition() -> None:
    gen, _, _ = make_generator()
    with pytest.raises(ValueError, match="max_orders, max_events or until"):
        gen.run()


def test_run_until_a_deadline() -> None:
    gen, repo, clock = make_generator()
    gen.run(until=T0 + timedelta(seconds=30))
    assert clock.now() >= T0 + timedelta(seconds=30)
    assert repo.placed


def test_run_respects_max_events() -> None:
    gen, _, _ = make_generator()
    report = gen.run(max_events=25)
    assert report.orders_placed + report.transitions + report.total_mutations == 25


def test_step_returns_none_when_nothing_is_due() -> None:
    gen, _, _ = make_generator()
    assert gen.step() is None


def test_report_totals_are_consistent() -> None:
    gen, repo, _ = make_generator()
    report = gen.run(max_orders=100)
    assert report.orders_placed == len(repo.placed)
    assert report.transitions == len(repo.advances)
    assert report.terminal == report.delivered + report.cancelled
    assert report.total_mutations == sum(report.mutations.values())


def test_the_same_seed_produces_the_same_run() -> None:
    a_gen, a_repo, _ = make_generator(seed=77)
    b_gen, b_repo, _ = make_generator(seed=77)
    a_gen.run(max_orders=40)
    b_gen.run(max_orders=40)
    assert [o.total_inr for o in a_repo.placed] == [o.total_inr for o in b_repo.placed]
    assert [c["status"] for c in a_repo.advances] == [c["status"] for c in b_repo.advances]


def test_status_distribution_is_plausible() -> None:
    gen, _, _ = make_generator()
    report = gen.run(max_orders=500)
    cancel_rate = report.cancelled / report.terminal
    assert 0.03 < cancel_rate < 0.15, f"implausible cancel rate {cancel_rate:.3f}"
    assert report.transitions > report.orders_placed, "most orders take several transitions"


def test_advancing_an_unknown_order_is_ignored() -> None:
    """A stale heap entry must not raise; it is simply no longer interesting."""
    gen, repo, _ = make_generator()
    gen.run(max_events=1)
    order_id = next(iter(gen.open_states())).order_id
    gen._states.pop(order_id)
    gen._do_transition(order_id, T0 + timedelta(hours=1))
    assert not repo.advances


def test_order_status_enum_round_trips_through_strings() -> None:
    """recover() rebuilds status from a database string."""
    for status in OrderStatus:
        assert OrderStatus(str(status)) is status


# --------------------------------------------------------------------------- guards


def test_new_order_rejects_an_empty_basket() -> None:
    with pytest.raises(ValueError, match="at least one item"):
        NewOrder(
            customer_id=1,
            restaurant_id=1,
            placed_ts=T0,
            promised_ts=T0 + timedelta(minutes=30),
            subtotal_inr=0.0,
            delivery_fee_inr=0.0,
            discount_inr=0.0,
            total_inr=0.0,
            items=(),
            payment_method="UPI",
        )


def test_new_order_rejects_a_negative_total() -> None:
    """The DDL has CHECK (total_inr >= 0); catch it before the database does."""
    with pytest.raises(ValueError, match="total_inr must be >= 0"):
        NewOrder(
            customer_id=1,
            restaurant_id=1,
            placed_ts=T0,
            promised_ts=T0 + timedelta(minutes=30),
            subtotal_inr=100.0,
            delivery_fee_inr=0.0,
            discount_inr=200.0,
            total_inr=-100.0,
            items=(OrderItem(menu_item_id=1, qty=1, unit_price_inr=100.0),),
            payment_method="UPI",
        )


def test_manual_clock_advance_is_an_alias_for_sleep() -> None:
    clock = ManualClock(T0)
    clock.advance(12)
    assert clock.now() == T0 + timedelta(seconds=12)


def test_scheduling_a_terminal_state_is_a_no_op() -> None:
    """A terminal OrderState has no next_due_at, so there is nothing to schedule."""
    gen, _, _ = make_generator()
    before = len(gen._heap)
    terminal = OrderState(
        order_id=999,
        status=OrderStatus.DELIVERED,
        placed_ts=T0,
        promised_ts=T0,
        next_due_at=None,
    )
    gen._schedule(terminal)
    assert len(gen._heap) == before


def test_transitioning_an_order_that_is_already_terminal_is_ignored() -> None:
    gen, repo, _ = make_generator()
    gen.run(max_events=1)
    order_id = next(iter(gen.open_states())).order_id
    gen._states[order_id] = OrderState(
        order_id=order_id,
        status=OrderStatus.CANCELLED,
        placed_ts=T0,
        promised_ts=T0,
        next_due_at=None,
        cancel_reason="CUSTOMER_CANCELLED",
    )
    gen._do_transition(order_id, T0 + timedelta(hours=1))
    assert not repo.advances


def test_a_heap_entry_that_fires_early_is_ignored() -> None:
    """Defensive: a stale heap entry must not force a transition before it is due."""
    gen, repo, _ = make_generator()
    gen.run(max_events=1)
    state = next(iter(gen.open_states()))
    assert state.next_due_at is not None
    gen._do_transition(state.order_id, state.next_due_at - timedelta(seconds=1))
    assert not repo.advances
