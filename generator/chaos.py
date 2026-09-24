"""Failure injection.

Seven scenarios are specified in `docs/architecture.md` §5.1. Two are implemented here,
because they are the two that need no infrastructure beyond Phase 1; the rest raise a clear
error naming the phase that brings them, rather than silently doing nothing.

**Chaos wraps the generator from outside. It never reaches into `state_machine`.** That is
what makes a bad row attributable to a named scenario instead of a bug in the machine.

---

## On "duplicate CDC records", and what a duplicate actually is

Re-running the same `UPDATE` does **not** produce a duplicate. It produces a second, genuinely
different change with its own WAL position, and any dedup that rejected it would be wrong.

A real duplicate is the *same WAL record delivered twice* — what Debezium does after a
connector restart that replays from an un-advanced slot. The identity of a change is therefore
`(table, primary key, LSN)`, and `DedupLedger` keys on exactly that.

PostgreSQL reproduces this faithfully: `pg_logical_slot_peek_changes` returns the same changes,
with the same LSNs, every time it is called until the slot is advanced with `get_changes`.
Peeking twice is redelivery. `tests/integration/test_chaos_duplicates.py` does that against a
real slot.

**`pg_current_wal_lsn()` is not a substitute.** It reports the WAL insert pointer, not the
position of a specific change: six writes inside one transaction returned two distinct values
in testing. Keying on it would reject legitimate changes.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import TYPE_CHECKING

from generator.state_machine import Transition

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

__all__ = [
    "IMPLEMENTED",
    "SCENARIO_PHASE",
    "ChangeKey",
    "ChaosConfig",
    "ChaosScenario",
    "DedupLedger",
    "DuplicateInjector",
    "OutOfOrderInjector",
    "parse_scenarios",
]


class ChaosScenario(StrEnum):
    """The seven scenarios from docs/architecture.md §5.1."""

    LATE_EVENTS = "late-events"
    OUT_OF_ORDER = "out-of-order"
    DUPLICATES = "duplicates"
    SCHEMA_DRIFT = "schema-drift"
    NULL_FLOOD = "null-flood"
    REFERENTIAL_BREAK = "referential-break"
    TRAFFIC_BURST = "traffic-burst"


#: Implemented today. The rest are rejected by name with the phase that brings them, so a
#: typo and a not-yet-built scenario give different errors.
IMPLEMENTED: frozenset[ChaosScenario] = frozenset(
    {ChaosScenario.DUPLICATES, ChaosScenario.OUT_OF_ORDER}
)

#: Where each unimplemented scenario belongs. Needing the watermark, the registry, the
#: quality gate or the dbt relationship test respectively is why they are not here yet.
SCENARIO_PHASE: dict[ChaosScenario, int] = {
    ChaosScenario.LATE_EVENTS: 3,
    ChaosScenario.SCHEMA_DRIFT: 2,
    ChaosScenario.NULL_FLOOD: 3,
    ChaosScenario.REFERENTIAL_BREAK: 4,
    ChaosScenario.TRAFFIC_BURST: 3,
}


def parse_scenarios(text: str) -> frozenset[ChaosScenario]:
    """Parse `--chaos duplicates,out-of-order` into scenarios.

    Rejects unknown names and names that exist but are not built yet, with different
    messages, because those are different mistakes.
    """
    if not text or not text.strip():
        return frozenset()

    chosen: set[ChaosScenario] = set()
    valid = {s.value for s in ChaosScenario}
    for raw in text.split(","):
        name = raw.strip().lower()
        if not name:
            continue
        if name not in valid:
            raise ValueError(
                f"unknown chaos scenario {name!r}; choose from {', '.join(sorted(valid))}"
            )
        scenario = ChaosScenario(name)
        if scenario not in IMPLEMENTED:
            phase = SCENARIO_PHASE[scenario]
            raise ValueError(
                f"chaos scenario {name!r} is specified but not implemented yet; "
                f"it arrives in Phase {phase}"
            )
        chosen.add(scenario)
    return frozenset(chosen)


@dataclass(frozen=True, slots=True)
class ChangeKey:
    """The identity of one change: table, primary key, and WAL position.

    This is the Phase 3 dedup key in its simplest form. Silver will express the same idea as
    a Delta `MERGE` predicate over `(pk, lsn)`.
    """

    table: str
    pk: int
    lsn: str

    def __post_init__(self) -> None:
        if not self.table:
            raise ValueError("table must not be empty")
        if not self.lsn:
            raise ValueError("lsn must not be empty")


@dataclass
class DedupLedger:
    """Accepts each `(table, pk, lsn)` once and rejects every repeat.

    Deliberately the dumbest thing that can work. The point of Phase 1 is to prove the *key*
    is right; Phase 3 replaces the mechanism with a `MERGE`, not the key.
    """

    _seen: set[ChangeKey] = field(default_factory=set)
    accepted: int = 0
    rejected: int = 0
    rejected_keys: list[ChangeKey] = field(default_factory=list)

    def accept(self, key: ChangeKey) -> bool:
        """True if this change is new. False if it has been seen before."""
        if key in self._seen:
            self.rejected += 1
            self.rejected_keys.append(key)
            return False
        self._seen.add(key)
        self.accepted += 1
        return True

    def accept_all(self, keys: Iterable[ChangeKey]) -> list[ChangeKey]:
        """Filter a stream down to the changes that have not been seen."""
        return [key for key in keys if self.accept(key)]

    @property
    def distinct(self) -> int:
        return len(self._seen)

    def reset(self) -> None:
        self._seen.clear()
        self.accepted = 0
        self.rejected = 0
        self.rejected_keys.clear()


@dataclass
class DuplicateInjector:
    """Scenario 3. Re-emits changes verbatim, as a restarted connector does.

    The duplicate carries the *same* LSN as the original — that is what makes it a duplicate
    rather than a new change, and what the ledger must key on to catch it.
    """

    rng: random.Random
    probability: float = 0.15
    duplicated: int = 0

    def __post_init__(self) -> None:
        if not 0.0 <= self.probability <= 1.0:
            raise ValueError(f"probability must be in [0, 1], got {self.probability}")

    def stream(self, keys: Iterable[ChangeKey]) -> Iterator[ChangeKey]:
        for key in keys:
            yield key
            if self.rng.random() < self.probability:
                self.duplicated += 1
                yield key  # identical: same table, same pk, same LSN


@dataclass
class OutOfOrderInjector:
    """Scenario 2. Rewinds a transition's timestamp behind the one before it.

    Produces `picked_up_ts` earlier than `accepted_ts` — a device whose clock is wrong, or an
    event that overtook its predecessor in flight. The state machine itself is untouched; only
    the timestamp on the way out is corrupted, so Silver sees exactly what a real out-of-order
    arrival looks like.
    """

    rng: random.Random
    probability: float = 0.05
    rewind_seconds: tuple[float, float] = (30.0, 900.0)
    corrupted: int = 0

    def __post_init__(self) -> None:
        if not 0.0 <= self.probability <= 1.0:
            raise ValueError(f"probability must be in [0, 1], got {self.probability}")
        low, high = self.rewind_seconds
        if low <= 0 or high < low:
            raise ValueError(f"rewind_seconds must be a positive range, got {self.rewind_seconds}")

    def maybe_corrupt(self, transition: Transition) -> Transition:
        """Return the transition, possibly with its timestamp moved backwards."""
        if transition.stamps is None:
            # A cancellation stamps no timestamp column; there is nothing to corrupt.
            return transition
        if self.rng.random() >= self.probability:
            return transition

        self.corrupted += 1
        rewind = timedelta(seconds=self.rng.uniform(*self.rewind_seconds))
        return Transition(
            order_id=transition.order_id,
            from_status=transition.from_status,
            to_status=transition.to_status,
            occurred_at=transition.occurred_at - rewind,
            stamps=transition.stamps,
            cancel_reason=transition.cancel_reason,
            # next_due_at is scheduling, not data: rewinding it would stall the order
            # rather than corrupt its timestamps, which is a different scenario.
            next_due_at=transition.next_due_at,
        )


@dataclass
class ChaosConfig:
    """Which scenarios are active, and the injectors that implement them."""

    scenarios: frozenset[ChaosScenario] = frozenset()
    duplicates: DuplicateInjector | None = None
    out_of_order: OutOfOrderInjector | None = None

    @classmethod
    def build(cls, scenarios: frozenset[ChaosScenario], rng: random.Random) -> ChaosConfig:
        return cls(
            scenarios=scenarios,
            duplicates=(
                DuplicateInjector(rng=rng) if ChaosScenario.DUPLICATES in scenarios else None
            ),
            out_of_order=(
                OutOfOrderInjector(rng=rng) if ChaosScenario.OUT_OF_ORDER in scenarios else None
            ),
        )

    @property
    def active(self) -> bool:
        return bool(self.scenarios)

    def transition_hook(self, transition: Transition) -> Transition:
        """The seam `OltpGenerator` calls. A no-op when out-of-order is not enabled."""
        if self.out_of_order is None:
            return transition
        return self.out_of_order.maybe_corrupt(transition)

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        if self.duplicates is not None:
            counts["duplicates_injected"] = self.duplicates.duplicated
        if self.out_of_order is not None:
            counts["timestamps_corrupted"] = self.out_of_order.corrupted
        return counts
