"""Unit tests for the order lifecycle state machine.

No containers, no database, no sleeping. A fake clock and a seeded RNG make every run
deterministic, so a failure here is always reproducible.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from generator.state_machine import (
    DEFAULT_CONFIG,
    DEFAULT_SPEED,
    Delay,
    MachineConfig,
    OrderState,
    OrderStatus,
    Rule,
    Transition,
    advance,
    apply_transition,
    invariant_errors,
    is_terminal,
    place_order,
    rule_for,
)

T0 = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)


def seeded(seed: int = 1234) -> random.Random:
    return random.Random(seed)


def run_to_completion(
    order_id: int,
    rng: random.Random,
    config: MachineConfig = DEFAULT_CONFIG,
    max_steps: int = 20,
) -> tuple[OrderState, list[Transition]]:
    """Drive one order to a terminal state, returning (final_state, transitions)."""
    state = place_order(order_id, T0, rng, config)
    transitions: list[Transition] = []
    for _ in range(max_steps):
        if is_terminal(state.status):
            break
        assert state.next_due_at is not None
        # Jump the clock straight to the due time: no sleeping, no polling loop.
        transition = advance(state, state.next_due_at, rng, config)
        assert transition is not None
        transitions.append(transition)
        state = apply_transition(state, transition)
    else:
        pytest.fail(f"order {order_id} did not terminate within {max_steps} steps")
    return state, transitions


# --------------------------------------------------------------------------- placement


def test_place_order_starts_in_placed_with_a_scheduled_transition() -> None:
    state = place_order(1, T0, seeded())
    assert state.status is OrderStatus.PLACED
    assert state.placed_ts == T0
    assert state.next_due_at is not None and state.next_due_at > T0
    assert state.accepted_ts is None
    assert state.picked_up_ts is None
    assert state.delivered_ts is None
    assert state.cancel_reason is None
    assert invariant_errors(state) == []


def test_promised_ts_is_after_placement() -> None:
    state = place_order(1, T0, seeded())
    assert state.promised_ts > state.placed_ts


# --------------------------------------------------------------------------- due-time gating


def test_advance_returns_none_before_the_due_time() -> None:
    state = place_order(1, T0, seeded())
    assert state.next_due_at is not None
    assert advance(state, state.next_due_at - timedelta(seconds=1), seeded()) is None


def test_advance_fires_exactly_at_the_due_time() -> None:
    state = place_order(1, T0, seeded())
    assert state.next_due_at is not None
    assert advance(state, state.next_due_at, seeded()) is not None


def test_transition_is_stamped_at_due_time_not_at_now() -> None:
    """Polling lateness must not leak into the data."""
    state = place_order(1, T0, seeded())
    assert state.next_due_at is not None
    very_late = state.next_due_at + timedelta(hours=3)
    transition = advance(state, very_late, seeded())
    assert transition is not None
    assert transition.occurred_at == state.next_due_at


# --------------------------------------------------------------------------- terminal states


@pytest.mark.parametrize("status", [OrderStatus.DELIVERED, OrderStatus.CANCELLED])
def test_terminal_orders_never_advance(status: OrderStatus) -> None:
    state = place_order(1, T0, seeded())
    terminal = state.__class__(
        order_id=state.order_id,
        status=status,
        placed_ts=state.placed_ts,
        promised_ts=state.promised_ts,
        next_due_at=None,
    )
    assert advance(terminal, T0 + timedelta(days=365), seeded()) is None


def test_non_terminal_without_due_time_is_rejected() -> None:
    """An order that could never advance is a bug, not a silent no-op."""
    state = place_order(1, T0, seeded())
    stuck = state.__class__(
        order_id=1,
        status=OrderStatus.PLACED,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=None,
    )
    with pytest.raises(ValueError, match="no next_due_at"):
        advance(stuck, T0, seeded())


# --------------------------------------------------------------------------- stale transitions


def test_applying_a_stale_transition_is_rejected() -> None:
    """Out-of-order updates are exactly how a state machine gets corrupted."""
    rng = seeded()
    state = place_order(1, T0, rng)
    assert state.next_due_at is not None
    transition = advance(state, state.next_due_at, rng)
    assert transition is not None
    moved = apply_transition(state, transition)

    with pytest.raises(ValueError, match="stale transition"):
        apply_transition(moved, transition)  # replaying the same transition


def test_applying_a_transition_for_another_order_is_rejected() -> None:
    rng = seeded()
    a = place_order(1, T0, rng)
    b = place_order(2, T0, rng)
    assert a.next_due_at is not None
    transition = advance(a, a.next_due_at, rng)
    assert transition is not None
    with pytest.raises(ValueError, match="transition is for order 1"):
        apply_transition(b, transition)


# --------------------------------------------------------------------------- full lifecycles


def test_every_order_terminates_and_stays_valid() -> None:
    """2000 orders, every intermediate state checked against every invariant."""
    rng = seeded(7)
    outcomes: dict[OrderStatus, int] = {OrderStatus.DELIVERED: 0, OrderStatus.CANCELLED: 0}

    for order_id in range(2000):
        state = place_order(order_id, T0, rng)
        assert invariant_errors(state) == [], invariant_errors(state)
        for _ in range(20):
            if is_terminal(state.status):
                break
            assert state.next_due_at is not None
            transition = advance(state, state.next_due_at, rng)
            assert transition is not None
            state = apply_transition(state, transition)
            errors = invariant_errors(state)
            assert errors == [], f"order {order_id} in {state.status}: {errors}"
        assert is_terminal(state.status)
        outcomes[state.status] += 1

    # Both outcomes must actually occur, or the test is not exercising cancellation.
    assert outcomes[OrderStatus.DELIVERED] > 0
    assert outcomes[OrderStatus.CANCELLED] > 0
    # Compounded cancel rates: ~6% + ~2% + ~0.5% => roughly 8%.
    cancel_rate = outcomes[OrderStatus.CANCELLED] / 2000
    assert 0.03 < cancel_rate < 0.15, f"implausible cancel rate {cancel_rate:.3f}"


def test_delivered_orders_have_the_full_timestamp_chain() -> None:
    """Encodes the dbt singular test: no order reaches DELIVERED without an accepted_ts."""
    rng = seeded(11)
    checked = 0
    for order_id in range(500):
        state, _ = run_to_completion(order_id, rng)
        if state.status is OrderStatus.DELIVERED:
            assert state.accepted_ts is not None
            assert state.picked_up_ts is not None
            assert state.delivered_ts is not None
            assert state.placed_ts <= state.accepted_ts <= state.picked_up_ts <= state.delivered_ts
            assert state.cancel_reason is None
            checked += 1
    assert checked > 400, f"only {checked} delivered orders; sample too small to mean anything"


def test_cancelled_orders_carry_a_reason_and_stop() -> None:
    rng = seeded(13)
    seen_sources: set[OrderStatus] = set()
    for order_id in range(2000):
        state, transitions = run_to_completion(order_id, rng)
        if state.status is OrderStatus.CANCELLED:
            assert state.cancel_reason is not None
            assert state.next_due_at is None
            assert state.delivered_ts is None
            cancelling = transitions[-1]
            seen_sources.add(cancelling.from_status)
            assert cancelling.cancel_reason in rule_for(cancelling.from_status).cancel_reasons
    # Cancellation must be reachable from every non-terminal state, not just the first.
    assert seen_sources == {OrderStatus.PLACED, OrderStatus.ACCEPTED, OrderStatus.PICKED_UP}


def test_transitions_follow_the_declared_rule_table() -> None:
    rng = seeded(17)
    allowed = {(r.source, r.target) for r in DEFAULT_CONFIG.rules}
    allowed |= {(r.source, OrderStatus.CANCELLED) for r in DEFAULT_CONFIG.rules}
    for order_id in range(500):
        _, transitions = run_to_completion(order_id, rng)
        for t in transitions:
            pair = (t.from_status, t.to_status)
            assert pair in allowed, f"illegal transition {t.from_status}->{t.to_status}"


# --------------------------------------------------------------------------- speed


def test_speed_compresses_durations_proportionally() -> None:
    slow = MachineConfig(speed=1.0)
    fast = MachineConfig(speed=20.0)
    # Same seed => same underlying triangular draw, so the only difference is the divisor.
    # Not exact: timedelta quantises to microseconds, so the compressed delay is rounded
    # and the ratio lands within ~1e-7 rather than on the nose.
    slow_state = place_order(1, T0, seeded(99), slow)
    fast_state = place_order(1, T0, seeded(99), fast)
    assert slow_state.next_due_at is not None and fast_state.next_due_at is not None
    slow_delay = (slow_state.next_due_at - T0).total_seconds()
    fast_delay = (fast_state.next_due_at - T0).total_seconds()
    assert slow_delay / fast_delay == pytest.approx(20.0, rel=1e-5)


def test_default_speed_is_20x() -> None:
    assert DEFAULT_SPEED == 20.0
    assert DEFAULT_CONFIG.speed == 20.0


def test_speed_must_be_positive() -> None:
    with pytest.raises(ValueError, match="speed must be > 0"):
        MachineConfig(speed=0)


# --------------------------------------------------------------------------- determinism


def test_same_seed_produces_identical_lifecycles() -> None:
    a_state, a_transitions = run_to_completion(1, seeded(4242))
    b_state, b_transitions = run_to_completion(1, seeded(4242))
    assert a_state == b_state
    assert a_transitions == b_transitions


def test_different_seeds_diverge() -> None:
    a_state, _ = run_to_completion(1, seeded(1))
    b_state, _ = run_to_completion(1, seeded(2))
    assert a_state != b_state


# --------------------------------------------------------------------------- config validation


def test_delay_rejects_an_impossible_distribution() -> None:
    with pytest.raises(ValueError, match="low_s <= mode_s <= high_s"):
        Delay(low_s=100, high_s=10, mode_s=50)


def test_delay_rejects_negative_low() -> None:
    with pytest.raises(ValueError, match="low_s must be >= 0"):
        Delay(low_s=-1, high_s=10, mode_s=5)


def test_rule_that_can_cancel_must_offer_reasons() -> None:
    with pytest.raises(ValueError, match="has no reasons"):
        Rule(
            source=OrderStatus.PLACED,
            target=OrderStatus.ACCEPTED,
            stamps="accepted_ts",
            delay=Delay(1, 2, 1.5),
            cancel_probability=0.1,
            cancel_reasons=(),
        )


def test_duplicate_rules_for_one_source_are_rejected() -> None:
    rule = rule_for(OrderStatus.PLACED)
    with pytest.raises(ValueError, match="more than one rule per source"):
        MachineConfig(rules=(rule, rule))


def test_rule_for_unknown_status_raises() -> None:
    with pytest.raises(KeyError):
        rule_for(OrderStatus.DELIVERED)


# --------------------------------------------------------------------------- invariants


def test_invariant_errors_catches_out_of_order_timestamps() -> None:
    """Chaos scenario 2 in the making: picked_up_ts before accepted_ts."""
    state = place_order(1, T0, seeded())
    broken = state.__class__(
        order_id=1,
        status=OrderStatus.PICKED_UP,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=T0 + timedelta(minutes=30),
        accepted_ts=T0 + timedelta(minutes=10),
        picked_up_ts=T0 + timedelta(minutes=5),  # before accepted
    )
    errors = invariant_errors(broken)
    assert any("picked_up_ts" in e and "before" in e for e in errors), errors


def test_invariant_errors_catches_a_timestamp_the_order_has_not_earned() -> None:
    state = place_order(1, T0, seeded())
    broken = state.__class__(
        order_id=1,
        status=OrderStatus.PLACED,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=T0 + timedelta(minutes=1),
        delivered_ts=T0 + timedelta(minutes=30),
    )
    assert any("should not have delivered_ts" in e for e in invariant_errors(broken))


def test_invariant_errors_catches_a_cancelled_order_without_a_reason() -> None:
    state = place_order(1, T0, seeded())
    broken = state.__class__(
        order_id=1,
        status=OrderStatus.CANCELLED,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=None,
    )
    assert "CANCELLED order has no cancel_reason" in invariant_errors(broken)


def test_invariant_errors_catches_a_terminal_order_still_scheduled() -> None:
    state = place_order(1, T0, seeded())
    broken = state.__class__(
        order_id=1,
        status=OrderStatus.DELIVERED,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=T0 + timedelta(minutes=5),
        accepted_ts=T0 + timedelta(minutes=1),
        picked_up_ts=T0 + timedelta(minutes=2),
        delivered_ts=T0 + timedelta(minutes=3),
    )
    assert any("still has next_due_at" in e for e in invariant_errors(broken))


def test_invariant_errors_catches_a_non_terminal_order_with_no_due_time() -> None:
    state = place_order(1, T0, seeded())
    broken = state.__class__(
        order_id=1,
        status=OrderStatus.ACCEPTED,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=None,
        accepted_ts=T0 + timedelta(minutes=1),
    )
    assert any("has no next_due_at" in e for e in invariant_errors(broken))


def test_invariant_errors_catches_a_missing_required_timestamp() -> None:
    """ACCEPTED without an accepted_ts: the order claims progress it cannot evidence."""
    state = place_order(1, T0, seeded())
    broken = state.__class__(
        order_id=1,
        status=OrderStatus.ACCEPTED,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=T0 + timedelta(minutes=10),
        accepted_ts=None,
    )
    assert any("requires accepted_ts" in e for e in invariant_errors(broken))


def test_invariant_errors_catches_a_cancel_reason_on_a_live_order() -> None:
    state = place_order(1, T0, seeded())
    broken = state.__class__(
        order_id=1,
        status=OrderStatus.PLACED,
        placed_ts=T0,
        promised_ts=state.promised_ts,
        next_due_at=T0 + timedelta(minutes=1),
        cancel_reason="CUSTOMER_CANCELLED",
    )
    assert any("should not have a cancel_reason" in e for e in invariant_errors(broken))


def test_delay_sample_rejects_non_positive_speed() -> None:
    with pytest.raises(ValueError, match="speed must be > 0"):
        Delay(1, 10, 5).sample(seeded(), speed=0)


def test_rule_rejects_an_out_of_range_cancel_probability() -> None:
    with pytest.raises(ValueError, match=r"cancel_probability must be in \[0, 1\]"):
        Rule(
            source=OrderStatus.PLACED,
            target=OrderStatus.ACCEPTED,
            stamps="accepted_ts",
            delay=Delay(1, 2, 1.5),
            cancel_probability=1.5,
            cancel_reasons=("X",),
        )


def test_state_is_immutable() -> None:
    state = place_order(1, T0, seeded())
    with pytest.raises(AttributeError):
        state.status = OrderStatus.DELIVERED  # type: ignore[misc]
