"""Unit tests for failure injection.

The dedup key is proved against real WAL positions in
tests/integration/test_chaos_duplicates.py. These tests cover the parsing, the injectors and
the boundaries, with no database.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from generator.chaos import (
    IMPLEMENTED,
    SCENARIO_PHASE,
    ChangeKey,
    ChaosConfig,
    ChaosScenario,
    DedupLedger,
    DuplicateInjector,
    OutOfOrderInjector,
    parse_scenarios,
)
from generator.state_machine import OrderStatus, Transition

T0 = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)


def key(pk: int = 1, lsn: str = "0/AF8AFE8", table: str = "orders") -> ChangeKey:
    return ChangeKey(table=table, pk=pk, lsn=lsn)


def transition(stamps: str | None = "accepted_ts") -> Transition:
    return Transition(
        order_id=1,
        from_status=OrderStatus.PLACED,
        to_status=OrderStatus.ACCEPTED if stamps else OrderStatus.CANCELLED,
        occurred_at=T0,
        stamps=stamps,
        cancel_reason=None if stamps else "PAYMENT_FAILED",
        next_due_at=T0 + timedelta(minutes=10),
    )


# --------------------------------------------------------------------------- the catalogue


def test_all_seven_scenarios_from_the_spec_are_named() -> None:
    assert len(ChaosScenario) == 7


def test_every_unimplemented_scenario_declares_its_phase() -> None:
    """A scenario must be either built or scheduled — never silently absent."""
    unimplemented = set(ChaosScenario) - IMPLEMENTED
    assert set(SCENARIO_PHASE) == unimplemented
    assert all(phase >= 2 for phase in SCENARIO_PHASE.values())


def test_duplicates_and_out_of_order_are_the_implemented_pair() -> None:
    assert {ChaosScenario.DUPLICATES, ChaosScenario.OUT_OF_ORDER} == IMPLEMENTED


# --------------------------------------------------------------------------- parsing


def test_parsing_a_single_scenario() -> None:
    assert parse_scenarios("duplicates") == {ChaosScenario.DUPLICATES}


def test_parsing_a_comma_separated_list() -> None:
    assert parse_scenarios("duplicates,out-of-order") == {
        ChaosScenario.DUPLICATES,
        ChaosScenario.OUT_OF_ORDER,
    }


@pytest.mark.parametrize("text", ["", "   ", ",", " , "])
def test_parsing_nothing_yields_nothing(text: str) -> None:
    assert parse_scenarios(text) == frozenset()


def test_parsing_tolerates_whitespace_and_case() -> None:
    assert parse_scenarios("  DUPLICATES , Out-Of-Order ") == {
        ChaosScenario.DUPLICATES,
        ChaosScenario.OUT_OF_ORDER,
    }


def test_an_unknown_scenario_is_rejected_with_the_valid_list() -> None:
    with pytest.raises(ValueError, match="unknown chaos scenario 'nonsense'"):
        parse_scenarios("nonsense")


def test_a_real_but_unbuilt_scenario_names_its_phase() -> None:
    """A different mistake from a typo, so a different message."""
    with pytest.raises(ValueError, match="not implemented yet; it arrives in Phase 3"):
        parse_scenarios("late-events")


def test_one_bad_name_rejects_the_whole_list() -> None:
    with pytest.raises(ValueError):
        parse_scenarios("duplicates,nonsense")


# --------------------------------------------------------------------------- ChangeKey


def test_change_keys_with_the_same_parts_are_equal() -> None:
    assert key() == key()
    assert len({key(), key()}) == 1


@pytest.mark.parametrize(
    "other",
    [key(pk=2), key(lsn="0/AF8B2A0"), key(table="payments")],
    ids=["different pk", "different lsn", "different table"],
)
def test_change_keys_differ_on_any_part(other: ChangeKey) -> None:
    assert key() != other


def test_change_key_rejects_empty_parts() -> None:
    with pytest.raises(ValueError, match="table must not be empty"):
        ChangeKey(table="", pk=1, lsn="0/1")
    with pytest.raises(ValueError, match="lsn must not be empty"):
        ChangeKey(table="orders", pk=1, lsn="")


def test_the_same_pk_in_different_tables_is_not_a_duplicate() -> None:
    """Primary keys are only unique within a table; the table is part of the identity."""
    ledger = DedupLedger()
    assert ledger.accept(ChangeKey("orders", 1, "0/A"))
    assert ledger.accept(ChangeKey("payments", 1, "0/A"))
    assert ledger.rejected == 0


# --------------------------------------------------------------------------- DedupLedger


def test_a_change_is_accepted_once_and_rejected_thereafter() -> None:
    ledger = DedupLedger()
    assert ledger.accept(key()) is True
    assert ledger.accept(key()) is False
    assert ledger.accept(key()) is False
    assert ledger.accepted == 1
    assert ledger.rejected == 2


def test_accept_all_filters_the_stream() -> None:
    ledger = DedupLedger()
    stream = [key(1), key(2), key(1), key(3), key(2)]
    assert ledger.accept_all(stream) == [key(1), key(2), key(3)]
    assert ledger.distinct == 3
    assert ledger.rejected == 2


def test_the_same_row_at_a_different_lsn_is_a_new_change() -> None:
    """A second write to the same row is real history, not a duplicate."""
    ledger = DedupLedger()
    assert ledger.accept(ChangeKey("orders", 5, "0/A1"))
    assert ledger.accept(ChangeKey("orders", 5, "0/A2"))
    assert ledger.rejected == 0


def test_rejected_keys_are_recorded_for_inspection() -> None:
    ledger = DedupLedger()
    ledger.accept_all([key(1), key(1), key(2), key(2)])
    assert ledger.rejected_keys == [key(1), key(2)]


def test_reset_clears_everything() -> None:
    ledger = DedupLedger()
    ledger.accept_all([key(1), key(1)])
    ledger.reset()
    assert (ledger.accepted, ledger.rejected, ledger.distinct) == (0, 0, 0)
    assert ledger.rejected_keys == []
    assert ledger.accept(key(1)) is True


# --------------------------------------------------------------------------- duplicates


def test_a_duplicate_is_byte_identical_to_its_original() -> None:
    injector = DuplicateInjector(rng=random.Random(1), probability=1.0)
    out = list(injector.stream([key(1), key(2)]))
    assert out == [key(1), key(1), key(2), key(2)]
    assert injector.duplicated == 2


def test_probability_zero_duplicates_nothing() -> None:
    injector = DuplicateInjector(rng=random.Random(1), probability=0.0)
    source = [key(n) for n in range(20)]
    assert list(injector.stream(source)) == source
    assert injector.duplicated == 0


def test_duplicate_rate_tracks_the_configured_probability() -> None:
    injector = DuplicateInjector(rng=random.Random(4), probability=0.25)
    source = [key(n) for n in range(4_000)]
    out = list(injector.stream(source))
    assert len(out) == 4_000 + injector.duplicated
    assert 0.2 < injector.duplicated / 4_000 < 0.3


def test_every_injected_duplicate_is_rejected() -> None:
    injector = DuplicateInjector(rng=random.Random(7), probability=0.4)
    source = [key(n) for n in range(500)]
    ledger = DedupLedger()
    survivors = ledger.accept_all(injector.stream(source))
    assert survivors == source
    assert ledger.rejected == injector.duplicated


@pytest.mark.parametrize("probability", [-0.1, 1.1])
def test_duplicate_injector_rejects_an_impossible_probability(probability: float) -> None:
    with pytest.raises(ValueError, match=r"probability must be in \[0, 1\]"):
        DuplicateInjector(rng=random.Random(1), probability=probability)


# --------------------------------------------------------------------------- out of order


def test_a_corrupted_transition_moves_backwards_in_time() -> None:
    injector = OutOfOrderInjector(rng=random.Random(1), probability=1.0)
    corrupted = injector.maybe_corrupt(transition())
    assert corrupted.occurred_at < T0
    assert injector.corrupted == 1


def test_corruption_preserves_everything_except_the_timestamp() -> None:
    injector = OutOfOrderInjector(rng=random.Random(2), probability=1.0)
    original = transition()
    corrupted = injector.maybe_corrupt(original)
    assert corrupted.order_id == original.order_id
    assert corrupted.from_status == original.from_status
    assert corrupted.to_status == original.to_status
    assert corrupted.stamps == original.stamps
    assert corrupted.next_due_at == original.next_due_at, "scheduling must not be rewound"


def test_probability_zero_corrupts_nothing() -> None:
    injector = OutOfOrderInjector(rng=random.Random(3), probability=0.0)
    for _ in range(200):
        assert injector.maybe_corrupt(transition()) == transition()
    assert injector.corrupted == 0


def test_a_cancellation_has_no_timestamp_to_corrupt() -> None:
    injector = OutOfOrderInjector(rng=random.Random(4), probability=1.0)
    cancel = transition(stamps=None)
    assert injector.maybe_corrupt(cancel) is cancel
    assert injector.corrupted == 0


def test_the_rewind_stays_within_the_configured_window() -> None:
    injector = OutOfOrderInjector(
        rng=random.Random(5), probability=1.0, rewind_seconds=(10.0, 20.0)
    )
    for _ in range(300):
        rewound = T0 - injector.maybe_corrupt(transition()).occurred_at
        assert timedelta(seconds=10) <= rewound <= timedelta(seconds=20)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"probability": 1.5}, r"probability must be in \[0, 1\]"),
        ({"rewind_seconds": (0.0, 10.0)}, "positive range"),
        ({"rewind_seconds": (20.0, 10.0)}, "positive range"),
    ],
    ids=["bad probability", "zero floor", "inverted range"],
)
def test_out_of_order_injector_validates_its_config(kwargs: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        OutOfOrderInjector(rng=random.Random(1), **kwargs)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- ChaosConfig


def test_an_empty_config_is_inactive_and_passes_transitions_through() -> None:
    chaos = ChaosConfig.build(frozenset(), random.Random(1))
    assert not chaos.active
    assert chaos.duplicates is None
    assert chaos.out_of_order is None
    original = transition()
    assert chaos.transition_hook(original) is original
    assert chaos.summary() == {}


def test_build_creates_only_the_requested_injectors() -> None:
    chaos = ChaosConfig.build(frozenset({ChaosScenario.DUPLICATES}), random.Random(1))
    assert chaos.active
    assert chaos.duplicates is not None
    assert chaos.out_of_order is None
    # Without out-of-order the hook must not touch anything.
    original = transition()
    assert chaos.transition_hook(original) is original


def test_the_hook_corrupts_when_out_of_order_is_enabled() -> None:
    chaos = ChaosConfig.build(frozenset({ChaosScenario.OUT_OF_ORDER}), random.Random(1))
    assert chaos.out_of_order is not None
    chaos.out_of_order.probability = 1.0
    assert chaos.transition_hook(transition()).occurred_at < T0


def test_summary_reports_only_active_injectors() -> None:
    both = ChaosConfig.build(IMPLEMENTED, random.Random(1))
    assert set(both.summary()) == {"duplicates_injected", "timestamps_corrupted"}
    only_dupes = ChaosConfig.build(frozenset({ChaosScenario.DUPLICATES}), random.Random(1))
    assert set(only_dupes.summary()) == {"duplicates_injected"}
