"""Chaos scenario 3 — duplicate CDC records — against real PostgreSQL logical decoding.

There is no mock here and no invented LSN. A real logical replication slot decodes the
generator's real writes, and `pg_logical_slot_peek_changes` is called twice. Peeking does not
advance the slot, so the second call returns the *same changes with the same LSNs* — which is
exactly what a Debezium connector does when it restarts without having advanced its slot.

The claim under test: dedup on `(table, pk, lsn)` accepts every change once and rejects every
redelivered copy, and a naive count that skips dedup is wrong by precisely the duplicate count.

Every test drops its slot in a `finally`. An inactive slot retains WAL indefinitely and will
fill the source disk — the hazard named in ADR-0001.

**These tests run against a dedicated `streamhouse_test` database, never the dev one.**
Logical decoding only surfaces *committed* changes, so unlike every other integration fixture
here this one cannot roll back — it must commit, and it must clear tables to get a known
state. Pointing it at the dev database would silently destroy whatever was in it, which is
exactly what happened once before this fixture was moved.

Run with:  pytest -m integration
Needs the core stack up:  make up
"""

from __future__ import annotations

import os
import pathlib
import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest

from generator.chaos import (
    ChangeKey,
    ChaosConfig,
    ChaosScenario,
    DedupLedger,
    DuplicateInjector,
)
from generator.config import LoadConfig, Range, SeedVolumes
from generator.oltp_generator import ManualClock, OltpGenerator
from generator.repository import LogicalSlot, PostgresRepository
from generator.seed import build_reference_data, seed_database
from generator.state_machine import MachineConfig

psycopg = pytest.importorskip("psycopg", reason="psycopg is not installed")

pytestmark = pytest.mark.integration

T0 = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)

SMALL = SeedVolumes(
    customers=25,
    restaurants=8,
    riders=10,
    menu_items_per_restaurant=Range(low=3.0, high=6.0, mode=4.0),
)

ALL_TABLES = (
    "order_items",
    "payments",
    "orders",
    "menu_items",
    "restaurants",
    "customers",
    "riders",
)


#: A database of this module's own. Never the dev database — see the module docstring.
TEST_DB = os.environ.get("SH_CHAOS_TEST_DB", "streamhouse_test")

SCHEMA_SQL = pathlib.Path(__file__).parents[2] / "infra" / "postgres" / "init" / "01-schema.sql"


def dsn(dbname: str) -> str:
    return (
        f"host={os.environ.get('SH_POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('SH_POSTGRES_PORT', '5432')} "
        f"user={os.environ.get('SH_POSTGRES_USER', 'streamhouse')} "
        f"password={os.environ.get('SH_POSTGRES_PASSWORD', 'streamhouse_local_dev')} "
        f"dbname={dbname}"
    )


@pytest.fixture(scope="module")
def test_database() -> str:
    """Create `streamhouse_test` and apply the schema. Left in place between runs."""
    try:
        admin = psycopg.connect(dsn("postgres"), connect_timeout=5, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL is not reachable ({exc.__class__.__name__}); run `make up`")

    with admin, admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,))
        if cur.fetchone() is None:
            cur.execute(f'CREATE DATABASE "{TEST_DB}"')
    admin.close()

    with psycopg.connect(dsn(TEST_DB), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    return TEST_DB


@pytest.fixture
def committed_cursor(test_database: str) -> Iterator[Any]:
    """An autocommit cursor on a seeded copy of the schema, in the TEST database.

    Autocommit, not a rolled-back transaction: logical decoding only ever surfaces
    **committed** changes, so a rolled-back fixture would decode precisely nothing. Because
    it cannot roll back, it must not point at the dev database.
    """
    conn = psycopg.connect(dsn(test_database), connect_timeout=5, autocommit=True)

    with conn, conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DELETE FROM {table}")
        seed_database(cur, build_reference_data(SMALL, seed=7, now=T0))
        try:
            yield cur
        finally:
            for table in ALL_TABLES:
                cur.execute(f"DELETE FROM {table}")
    conn.close()


def slot_name() -> str:
    return f"sh_test_{uuid.uuid4().hex[:10]}"


def run_generator(cur: Any, orders: int, seed: int = 4242) -> Any:
    return OltpGenerator(
        repository=PostgresRepository(cur),
        load=LoadConfig(orders_per_day=50_000, speed=20.0),
        machine=MachineConfig(speed=20.0),
        rng=random.Random(seed),
        clock=ManualClock(T0),
    ).run(max_orders=orders)


# --------------------------------------------------------------------------- the mechanism


def test_logical_decoding_yields_a_distinct_lsn_per_change(committed_cursor: Any) -> None:
    """The premise everything else rests on.

    `pg_current_wal_lsn()` does NOT give this: six writes in one transaction returned two
    distinct values. Per-change LSNs come from decoding, which is why this test exists.
    """
    slot = LogicalSlot(committed_cursor, slot_name())
    try:
        slot.__enter__()
        run_generator(committed_cursor, orders=8)
        changes = slot.peek()

        assert len(changes) > 8, "expected several changes per order"
        by_key = {(table, pk): lsn for table, pk, lsn in changes}
        assert len(by_key) > 0
        # No two changes to the same row may share an LSN, or dedup would eat real changes.
        seen: dict[tuple[str, int, str], int] = {}
        for table, pk, lsn in changes:
            seen[(table, pk, lsn)] = seen.get((table, pk, lsn), 0) + 1
        collisions = {k: n for k, n in seen.items() if n > 1}
        assert not collisions, f"distinct changes shared an LSN: {list(collisions)[:3]}"
    finally:
        slot.drop()


def test_peeking_twice_returns_the_identical_changes(committed_cursor: Any) -> None:
    """This is the duplicate-delivery scenario, reproduced rather than simulated."""
    slot = LogicalSlot(committed_cursor, slot_name())
    try:
        slot.__enter__()
        run_generator(committed_cursor, orders=6)

        first = slot.peek()
        second = slot.peek()

        assert first, "no changes were decoded"
        assert first == second, "peek is supposed to be non-destructive"
    finally:
        slot.drop()


# --------------------------------------------------------------------------- the claim


def test_dedup_on_pk_and_lsn_rejects_every_redelivered_change(committed_cursor: Any) -> None:
    """The headline: replay the whole stream and not one duplicate gets through."""
    slot = LogicalSlot(committed_cursor, slot_name())
    try:
        slot.__enter__()
        run_generator(committed_cursor, orders=12)

        delivery_one = [ChangeKey(t, pk, lsn) for t, pk, lsn in slot.peek()]
        delivery_two = [ChangeKey(t, pk, lsn) for t, pk, lsn in slot.peek()]
        assert delivery_one and delivery_one == delivery_two

        ledger = DedupLedger()
        ledger.accept_all(delivery_one)
        accepted_after_first = ledger.accepted

        ledger.accept_all(delivery_two)

        assert ledger.accepted == accepted_after_first, "a redelivered change was accepted"
        assert ledger.rejected == len(delivery_two)
        assert ledger.distinct == len(set(delivery_one))
        # Without dedup the naive total is wrong by exactly the replayed count.
        assert len(delivery_one) + len(delivery_two) == ledger.accepted + ledger.rejected
    finally:
        slot.drop()


def test_an_injected_duplicate_stream_is_fully_rejected(committed_cursor: Any) -> None:
    """The DuplicateInjector against real changes, not synthetic ones."""
    slot = LogicalSlot(committed_cursor, slot_name())
    try:
        slot.__enter__()
        run_generator(committed_cursor, orders=15)
        real = [ChangeKey(t, pk, lsn) for t, pk, lsn in slot.peek()]
        assert real

        injector = DuplicateInjector(rng=random.Random(11), probability=0.35)
        polluted = list(injector.stream(real))

        assert injector.duplicated > 0, "injector produced no duplicates to reject"
        assert len(polluted) == len(real) + injector.duplicated

        ledger = DedupLedger()
        survivors = ledger.accept_all(polluted)

        assert len(survivors) == len(set(real))
        assert ledger.rejected == injector.duplicated
        assert all(key in set(real) for key in ledger.rejected_keys)
    finally:
        slot.drop()


def test_a_duplicate_carries_the_same_lsn_as_its_original(committed_cursor: Any) -> None:
    """If the LSN differed it would be a new change, and rejecting it would be a bug."""
    slot = LogicalSlot(committed_cursor, slot_name())
    try:
        slot.__enter__()
        run_generator(committed_cursor, orders=5)
        real = [ChangeKey(t, pk, lsn) for t, pk, lsn in slot.peek()]
        assert real

        injector = DuplicateInjector(rng=random.Random(3), probability=1.0)
        polluted = list(injector.stream(real))

        assert polluted == [key for key in real for _ in range(2)]
        for original, duplicate in zip(polluted[::2], polluted[1::2], strict=True):
            assert original == duplicate
            assert original.lsn == duplicate.lsn
    finally:
        slot.drop()


def test_re_running_the_same_update_is_not_a_duplicate(committed_cursor: Any) -> None:
    """The distinction the whole design rests on.

    Writing the same values again is a *new* change with a new WAL position. Dedup must let
    it through; treating it as a duplicate would silently drop real history.
    """
    slot = LogicalSlot(committed_cursor, slot_name())
    try:
        slot.__enter__()
        committed_cursor.execute("SELECT rider_id FROM riders ORDER BY rider_id LIMIT 1")
        rider_id = int(committed_cursor.fetchone()[0])

        for _ in range(3):
            committed_cursor.execute(
                "UPDATE riders SET is_online = NOT is_online, updated_at = now() "
                "WHERE rider_id = %s",
                (rider_id,),
            )

        changes = [ChangeKey(t, pk, lsn) for t, pk, lsn in slot.peek() if t == "riders"]
        assert len(changes) == 3, f"expected 3 decoded changes, got {len(changes)}"
        assert len({c.lsn for c in changes}) == 3, "identical writes must get distinct LSNs"

        ledger = DedupLedger()
        assert len(ledger.accept_all(changes)) == 3
        assert ledger.rejected == 0
    finally:
        slot.drop()


# --------------------------------------------------------------------------- hygiene


def test_the_slot_is_dropped_even_when_the_body_raises(committed_cursor: Any) -> None:
    """ADR-0001: an inactive slot retains WAL forever and fills the source disk."""
    name = slot_name()
    with pytest.raises(RuntimeError, match="boom"), LogicalSlot(committed_cursor, name):
        committed_cursor.execute("SELECT 1")
        raise RuntimeError("boom")

    committed_cursor.execute(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (name,)
    )
    assert committed_cursor.fetchone()[0] == 0


def test_no_slots_are_left_behind_by_this_module(committed_cursor: Any) -> None:
    committed_cursor.execute(
        "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'sh_test_%'"
    )
    leftovers = [r[0] for r in committed_cursor.fetchall()]
    assert leftovers == [], f"leaked replication slots: {leftovers}"


def test_consuming_advances_the_slot(committed_cursor: Any) -> None:
    slot = LogicalSlot(committed_cursor, slot_name())
    try:
        slot.__enter__()
        run_generator(committed_cursor, orders=4)
        assert slot.peek(), "expected changes before consuming"
        slot.consume()
        assert slot.peek() == (), "consume() should have advanced past every change"
    finally:
        slot.drop()


# --------------------------------------------------------------------------- wiring


def test_chaos_config_drives_the_generator_hook(committed_cursor: Any) -> None:
    """out-of-order corrupts timestamps in the database, not just in memory."""
    chaos = ChaosConfig.build(frozenset({ChaosScenario.OUT_OF_ORDER}), random.Random(5))
    assert chaos.out_of_order is not None
    chaos.out_of_order.probability = 1.0  # every stamped transition, so the test is decisive

    OltpGenerator(
        repository=PostgresRepository(committed_cursor),
        load=LoadConfig(orders_per_day=50_000, speed=20.0),
        machine=MachineConfig(speed=20.0),
        rng=random.Random(9),
        clock=ManualClock(T0),
        transition_hook=chaos.transition_hook,
    ).run(max_orders=25)

    assert chaos.out_of_order.corrupted > 0
    committed_cursor.execute(
        """
        SELECT count(*) FROM orders
        WHERE (accepted_ts  IS NOT NULL AND accepted_ts  < placed_ts)
           OR (picked_up_ts IS NOT NULL AND picked_up_ts < accepted_ts)
           OR (delivered_ts IS NOT NULL AND delivered_ts < picked_up_ts)
        """
    )
    broken = int(committed_cursor.fetchone()[0])
    assert broken > 0, "out-of-order chaos produced no out-of-sequence rows"
