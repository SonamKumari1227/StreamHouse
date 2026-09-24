"""Order lifecycle state machine.

    PLACED -> ACCEPTED -> PICKED_UP -> DELIVERED
       |          |            |
       +----------+------------+-------> CANCELLED

Pure logic. No database, no network, no wall clock, no sleeping. Every function takes
the current time and a random source as arguments, which is what lets the whole module
be unit-tested in milliseconds without a container anywhere in sight.

Three design decisions worth knowing:

1. **Table-driven, not if/elif.** Transitions live in `DEFAULT_RULES`. Adding a state means
   adding a row, and the invariants below keep holding.

2. **Due-time driven, not tick-driven.** Each order carries `next_due_at`; the caller polls
   for orders whose time has come. Lifecycles overlap the way real ones do, instead of
   advancing in lockstep batches.

3. **Transitions are stamped at `next_due_at`, not at `now`.** If the daemon polls late, the
   data must not record that lateness — polling granularity is an artifact of the generator,
   not a property of the business. This keeps delay distributions clean regardless of how
   coarsely the caller polls.

This module is the *only* writer of `status` and the `*_ts` columns. Chaos scenarios wrap it
from outside; they never reach in. That is what makes an invalid state attributable to a named
scenario rather than to a bug in here.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from itertools import pairwise

__all__ = [
    "DEFAULT_CONFIG",
    "DEFAULT_RULES",
    "DEFAULT_SPEED",
    "TERMINAL_STATUSES",
    "Delay",
    "MachineConfig",
    "OrderState",
    "OrderStatus",
    "Rule",
    "Transition",
    "advance",
    "apply_transition",
    "invariant_errors",
    "is_terminal",
    "place_order",
    "rule_for",
]


class OrderStatus(StrEnum):
    """Mirrors the CHECK constraint on orders.status in infra/postgres/init/01-schema.sql."""

    PLACED = "PLACED"
    ACCEPTED = "ACCEPTED"
    PICKED_UP = "PICKED_UP"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.DELIVERED, OrderStatus.CANCELLED}
)

#: Wall-clock compression. At 20x an order's full lifecycle takes ~90 seconds instead of
#: ~30 minutes, so a demo shows a realistic status distribution in minutes rather than a day.
#: Use speed=1.0 for an honest overnight run.
DEFAULT_SPEED: float = 20.0


@dataclass(frozen=True, slots=True)
class Delay:
    """A triangular delay distribution, in real-world seconds.

    Triangular rather than uniform because real durations cluster around a typical value
    with a long right tail, and rather than normal because it cannot produce the negative
    or absurd samples a normal distribution occasionally will.
    """

    low_s: float
    high_s: float
    mode_s: float

    def __post_init__(self) -> None:
        if self.low_s < 0:
            raise ValueError(f"low_s must be >= 0, got {self.low_s}")
        if not self.low_s <= self.mode_s <= self.high_s:
            raise ValueError(
                f"require low_s <= mode_s <= high_s, got "
                f"{self.low_s} / {self.mode_s} / {self.high_s}"
            )

    def sample(self, rng: random.Random, speed: float = 1.0) -> timedelta:
        """Draw one delay, compressed by `speed`."""
        if speed <= 0:
            raise ValueError(f"speed must be > 0, got {speed}")
        seconds = rng.triangular(self.low_s, self.high_s, self.mode_s)
        return timedelta(seconds=seconds / speed)


@dataclass(frozen=True, slots=True)
class Rule:
    """One row of the transition table: how to leave `source`."""

    source: OrderStatus
    target: OrderStatus
    stamps: str
    """The orders column stamped when this transition succeeds."""
    delay: Delay
    """How long the order dwells in `source` before the transition fires."""
    cancel_probability: float
    """Chance the order cancels out of `source` instead of reaching `target`."""
    cancel_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not 0.0 <= self.cancel_probability <= 1.0:
            raise ValueError(f"cancel_probability must be in [0, 1], got {self.cancel_probability}")
        if self.cancel_probability > 0 and not self.cancel_reasons:
            raise ValueError(f"rule {self.source}->{self.target} can cancel but has no reasons")


DEFAULT_RULES: tuple[Rule, ...] = (
    Rule(
        source=OrderStatus.PLACED,
        target=OrderStatus.ACCEPTED,
        stamps="accepted_ts",
        # Restaurants usually confirm fast; a slow confirm is the tail.
        delay=Delay(low_s=15, high_s=420, mode_s=75),
        cancel_probability=0.060,
        cancel_reasons=(
            "RESTAURANT_REJECTED",
            "CUSTOMER_CANCELLED",
            "PAYMENT_FAILED",
            "NO_RIDER_AVAILABLE",
        ),
    ),
    Rule(
        source=OrderStatus.ACCEPTED,
        target=OrderStatus.PICKED_UP,
        stamps="picked_up_ts",
        # Prep 8-25 min per the spec, typical ~14.
        delay=Delay(low_s=8 * 60, high_s=25 * 60, mode_s=14 * 60),
        cancel_probability=0.020,
        cancel_reasons=(
            "RESTAURANT_OUT_OF_STOCK",
            "CUSTOMER_CANCELLED",
            "NO_RIDER_AVAILABLE",
        ),
    ),
    Rule(
        source=OrderStatus.PICKED_UP,
        target=OrderStatus.DELIVERED,
        stamps="delivered_ts",
        # Transit 5-40 min per the spec, typical ~16.
        delay=Delay(low_s=5 * 60, high_s=40 * 60, mode_s=16 * 60),
        cancel_probability=0.005,
        cancel_reasons=(
            "CUSTOMER_UNREACHABLE",
            "ADDRESS_NOT_FOUND",
        ),
    ),
)

#: Delivery SLA offered to the customer at placement, used for sla_breach_flag in Gold.
#: Scaled by `speed` like every other duration, or the breach rate would be meaningless.
PROMISE_DELAY: Delay = Delay(low_s=25 * 60, high_s=60 * 60, mode_s=35 * 60)


@dataclass(frozen=True, slots=True)
class MachineConfig:
    speed: float = DEFAULT_SPEED
    rules: tuple[Rule, ...] = DEFAULT_RULES
    promise_delay: Delay = PROMISE_DELAY

    def __post_init__(self) -> None:
        if self.speed <= 0:
            raise ValueError(f"speed must be > 0, got {self.speed}")
        sources = [r.source for r in self.rules]
        if len(sources) != len(set(sources)):
            raise ValueError("more than one rule per source status")


DEFAULT_CONFIG = MachineConfig()


@dataclass(frozen=True, slots=True)
class OrderState:
    """The slice of an order this machine reasons about.

    Deliberately not the whole `orders` row — money, customer and restaurant are none of
    this module's business. Immutable: `apply_transition` returns a new instance.
    """

    order_id: int
    status: OrderStatus
    placed_ts: datetime
    promised_ts: datetime
    next_due_at: datetime | None
    accepted_ts: datetime | None = None
    picked_up_ts: datetime | None = None
    delivered_ts: datetime | None = None
    cancel_reason: str | None = None


@dataclass(frozen=True, slots=True)
class Transition:
    """A decided, not-yet-applied state change.

    Returned rather than applied so the caller can write it to Postgres and update its own
    state in one unit, and so chaos scenarios can duplicate, delay or reorder it.
    """

    order_id: int
    from_status: OrderStatus
    to_status: OrderStatus
    occurred_at: datetime
    stamps: str | None
    cancel_reason: str | None
    next_due_at: datetime | None


def is_terminal(status: OrderStatus) -> bool:
    return status in TERMINAL_STATUSES


def rule_for(status: OrderStatus, config: MachineConfig = DEFAULT_CONFIG) -> Rule:
    """The rule governing how an order leaves `status`."""
    for rule in config.rules:
        if rule.source == status:
            return rule
    raise KeyError(f"no transition rule for status {status!r}")


def place_order(
    order_id: int,
    now: datetime,
    rng: random.Random,
    config: MachineConfig = DEFAULT_CONFIG,
) -> OrderState:
    """Create a new order in PLACED, with its first transition already scheduled."""
    first = rule_for(OrderStatus.PLACED, config)
    return OrderState(
        order_id=order_id,
        status=OrderStatus.PLACED,
        placed_ts=now,
        promised_ts=now + config.promise_delay.sample(rng, config.speed),
        next_due_at=now + first.delay.sample(rng, config.speed),
    )


def advance(
    state: OrderState,
    now: datetime,
    rng: random.Random,
    config: MachineConfig = DEFAULT_CONFIG,
) -> Transition | None:
    """Decide the next transition, or None if the order is terminal or not yet due.

    Does not mutate `state`. Pass the result to `apply_transition`.
    """
    if is_terminal(state.status):
        return None
    if state.next_due_at is None:
        raise ValueError(
            f"order {state.order_id} is in non-terminal state {state.status} "
            f"with no next_due_at; it would never advance"
        )
    if now < state.next_due_at:
        return None

    rule = rule_for(state.status, config)
    # Stamped at the due time, not at `now` — see the module docstring.
    occurred_at = state.next_due_at

    if rng.random() < rule.cancel_probability:
        return Transition(
            order_id=state.order_id,
            from_status=state.status,
            to_status=OrderStatus.CANCELLED,
            occurred_at=occurred_at,
            stamps=None,
            cancel_reason=rng.choice(rule.cancel_reasons),
            next_due_at=None,
        )

    if is_terminal(rule.target):
        next_due_at = None
    else:
        next_due_at = occurred_at + rule_for(rule.target, config).delay.sample(rng, config.speed)

    return Transition(
        order_id=state.order_id,
        from_status=state.status,
        to_status=rule.target,
        occurred_at=occurred_at,
        stamps=rule.stamps,
        cancel_reason=None,
        next_due_at=next_due_at,
    )


def apply_transition(state: OrderState, transition: Transition) -> OrderState:
    """Apply a transition, returning a new OrderState.

    Rejects a transition that does not match the order's current state. A stale transition
    applied blindly is exactly how out-of-order updates corrupt a state machine, so it is
    refused here rather than silently absorbed.
    """
    if transition.order_id != state.order_id:
        raise ValueError(
            f"transition is for order {transition.order_id}, state is order {state.order_id}"
        )
    if transition.from_status != state.status:
        raise ValueError(
            f"stale transition for order {state.order_id}: transition leaves "
            f"{transition.from_status}, but order is in {state.status}"
        )

    changes: dict[str, object] = {
        "status": transition.to_status,
        "next_due_at": transition.next_due_at,
    }
    if transition.stamps is not None:
        changes[transition.stamps] = transition.occurred_at
    if transition.cancel_reason is not None:
        changes["cancel_reason"] = transition.cancel_reason

    return replace(state, **changes)  # type: ignore[arg-type]


def invariant_errors(state: OrderState) -> list[str]:
    """Every way an OrderState can be wrong. Empty list means valid.

    Used by tests, and later by the chaos harness to assert that a scenario broke exactly
    what it claimed to break and nothing else.
    """
    errors: list[str] = []

    if is_terminal(state.status):
        if state.next_due_at is not None:
            errors.append(f"terminal status {state.status} still has next_due_at")
    elif state.next_due_at is None:
        errors.append(f"non-terminal status {state.status} has no next_due_at")

    # Timestamps must be monotonic in lifecycle order.
    stamped = [
        ("placed_ts", state.placed_ts),
        ("accepted_ts", state.accepted_ts),
        ("picked_up_ts", state.picked_up_ts),
        ("delivered_ts", state.delivered_ts),
    ]
    present = [(name, ts) for name, ts in stamped if ts is not None]
    for (prev_name, prev_ts), (next_name, next_ts) in pairwise(present):
        if next_ts < prev_ts:
            errors.append(f"{next_name} ({next_ts}) is before {prev_name} ({prev_ts})")

    # Which timestamps must exist, given how far the order got.
    required: dict[OrderStatus, tuple[str, ...]] = {
        OrderStatus.PLACED: (),
        OrderStatus.ACCEPTED: ("accepted_ts",),
        OrderStatus.PICKED_UP: ("accepted_ts", "picked_up_ts"),
        OrderStatus.DELIVERED: ("accepted_ts", "picked_up_ts", "delivered_ts"),
        OrderStatus.CANCELLED: (),
    }
    for field in required[state.status]:
        if getattr(state, field) is None:
            errors.append(f"status {state.status} requires {field}, which is None")

    # A timestamp the order cannot yet have earned.
    reached: dict[OrderStatus, int] = {
        OrderStatus.PLACED: 0,
        OrderStatus.ACCEPTED: 1,
        OrderStatus.PICKED_UP: 2,
        OrderStatus.DELIVERED: 3,
    }
    if state.status is not OrderStatus.CANCELLED:
        depth = reached[state.status]
        for name, index in (("accepted_ts", 1), ("picked_up_ts", 2), ("delivered_ts", 3)):
            if index > depth and getattr(state, name) is not None:
                errors.append(f"status {state.status} should not have {name}")

    if state.status is OrderStatus.CANCELLED and state.cancel_reason is None:
        errors.append("CANCELLED order has no cancel_reason")
    if state.status is not OrderStatus.CANCELLED and state.cancel_reason is not None:
        errors.append(f"status {state.status} should not have a cancel_reason")

    return errors
