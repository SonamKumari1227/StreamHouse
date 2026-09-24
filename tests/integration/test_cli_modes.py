"""The CLI driven end to end against the real stack.

`tests/unit/test_cli.py` covers parsing and validation without touching anything. This file
runs the modes for real — seeding, generating and producing — because an entry point that
parses correctly and then fails on first contact is still broken.

Runs against the dedicated `streamhouse_test` database and a throwaway topic, never the dev
database or `gps.pings`.

Run with:  pytest -m integration
Needs the core stack up:  make up
"""

from __future__ import annotations

import contextlib
import os
import pathlib
import uuid
from collections.abc import Iterator

import pytest

from generator.__main__ import main
from generator.repository import LogicalSlot

psycopg = pytest.importorskip("psycopg", reason="psycopg is not installed")

pytestmark = pytest.mark.integration

TEST_DB = os.environ.get("SH_CHAOS_TEST_DB", "streamhouse_test")
SCHEMA_SQL = pathlib.Path(__file__).parents[2] / "infra" / "postgres" / "init" / "01-schema.sql"
BOOTSTRAP = os.environ.get("SH_TEST_BOOTSTRAP", "localhost:9092")

TABLES = ("order_items", "payments", "orders", "menu_items", "restaurants", "customers", "riders")


def dsn(dbname: str = TEST_DB) -> str:
    return (
        f"host={os.environ.get('SH_POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('SH_POSTGRES_PORT', '5432')} "
        f"user={os.environ.get('SH_POSTGRES_USER', 'streamhouse')} "
        f"password={os.environ.get('SH_POSTGRES_PASSWORD', 'streamhouse_local_dev')} "
        f"dbname={dbname}"
    )


@pytest.fixture
def clean_db() -> Iterator[str]:
    """An empty test database with the schema applied."""
    try:
        admin = psycopg.connect(dsn("postgres"), connect_timeout=5, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL is not reachable ({exc.__class__.__name__}); run `make up`")
    with admin, admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,))
        if cur.fetchone() is None:
            cur.execute(f'CREATE DATABASE "{TEST_DB}"')
    admin.close()

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
        for table in TABLES:
            cur.execute(f"DELETE FROM {table}")
    yield dsn()
    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        for table in TABLES:
            cur.execute(f"DELETE FROM {table}")


def count(table: str) -> int:
    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return int(cur.fetchone()[0])


# --------------------------------------------------------------------------- seed mode


def test_seed_mode_populates_the_reference_tables(clean_db: str) -> None:
    status = main(
        [
            "--mode",
            "seed",
            "--dsn",
            clean_db,
            "--customers",
            "20",
            "--restaurants",
            "5",
            "--riders",
            "6",
        ]
    )
    assert status == 0
    assert count("customers") == 20
    assert count("restaurants") == 5
    assert count("riders") == 6
    assert count("menu_items") > 0


def test_seed_mode_is_idempotent(clean_db: str, capsys: pytest.CaptureFixture[str]) -> None:
    args = [
        "--mode",
        "seed",
        "--dsn",
        clean_db,
        "--customers",
        "15",
        "--restaurants",
        "4",
        "--riders",
        "5",
    ]
    assert main(args) == 0
    capsys.readouterr()
    assert main(args) == 0
    assert "nothing to do" in capsys.readouterr().out
    assert count("customers") == 15


def test_reset_clears_before_seeding(clean_db: str, capsys: pytest.CaptureFixture[str]) -> None:
    base = [
        "--mode",
        "seed",
        "--dsn",
        clean_db,
        "--customers",
        "12",
        "--restaurants",
        "3",
        "--riders",
        "4",
    ]
    assert main(base) == 0
    assert main([*base, "--reset"]) == 0
    assert "--reset: deleting every row" in capsys.readouterr().out
    assert count("customers") == 12


# --------------------------------------------------------------------------- oltp mode


def test_oltp_mode_writes_orders(clean_db: str, capsys: pytest.CaptureFixture[str]) -> None:
    main(
        [
            "--mode",
            "seed",
            "--dsn",
            clean_db,
            "--customers",
            "20",
            "--restaurants",
            "5",
            "--riders",
            "6",
        ]
    )
    capsys.readouterr()

    # --speed 500, not the 20x default: `--orders N` drains every in-flight order before
    # returning, and RealClock genuinely sleeps. At 20x that is ~90s of lifecycle per order.
    assert main(["--mode", "oltp", "--dsn", clean_db, "--orders", "15", "--speed", "500"]) == 0
    out = capsys.readouterr().out
    assert "placed=15" in out
    assert count("orders") == 15
    assert count("payments") == 15
    assert count("order_items") >= 15


def test_oltp_mode_recovers_orders_from_a_previous_run(
    clean_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """The recovery path, through the CLI rather than the class."""
    main(
        [
            "--mode",
            "seed",
            "--dsn",
            clean_db,
            "--customers",
            "20",
            "--restaurants",
            "5",
            "--riders",
            "6",
        ]
    )
    main(["--mode", "oltp", "--dsn", clean_db, "--duration", "1", "--orders-per-day", "60000"])
    capsys.readouterr()

    in_flight = count("orders") - _terminal_orders()
    assert in_flight > 0, "test needs orders in flight to be meaningful"

    # Bounded by --duration, not --orders: `run(max_orders=...)` drains everything still in
    # flight, and on a real clock that means waiting out ~90s of order lifecycle per batch.
    # The claim under test is the recovery count, not the drain.
    assert main(["--mode", "oltp", "--dsn", clean_db, "--duration", "2"]) == 0
    assert f"recovered {in_flight} orders" in capsys.readouterr().out


def _terminal_orders() -> int:
    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM orders WHERE status IN ('DELIVERED','CANCELLED')")
        return int(cur.fetchone()[0])


def test_oltp_mode_with_chaos_corrupts_timestamps(
    clean_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    main(
        [
            "--mode",
            "seed",
            "--dsn",
            clean_db,
            "--customers",
            "20",
            "--restaurants",
            "5",
            "--riders",
            "6",
        ]
    )
    capsys.readouterr()

    assert (
        main(
            [
                "--mode",
                "oltp",
                "--dsn",
                clean_db,
                "--orders",
                "40",
                "--speed",
                "500",
                "--chaos",
                "duplicates,out-of-order",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "chaos    : duplicates, out-of-order" in captured.out
    assert "acts on CDC delivery" in captured.err, "the zero-duplicates note must be explained"

    with psycopg.connect(dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM orders
            WHERE (accepted_ts  IS NOT NULL AND accepted_ts  < placed_ts)
               OR (picked_up_ts IS NOT NULL AND picked_up_ts < accepted_ts)
               OR (delivered_ts IS NOT NULL AND delivered_ts < picked_up_ts)
            """
        )
        assert int(cur.fetchone()[0]) > 0, "out-of-order chaos changed nothing in the database"


def test_a_default_duration_is_applied_when_no_limit_is_given(
    clean_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Covers the branch that stops a bare `--mode oltp` running forever."""
    main(
        [
            "--mode",
            "seed",
            "--dsn",
            clean_db,
            "--customers",
            "10",
            "--restaurants",
            "3",
            "--riders",
            "4",
        ]
    )
    capsys.readouterr()
    from generator.__main__ import Stopper, build_parser, run_oltp
    from generator.chaos import ChaosConfig

    args = build_parser().parse_args(["--mode", "oltp", "--dsn", clean_db])
    assert args.duration is None
    args.duration = 1.0  # keep the test quick; the branch under test is the message
    assert run_oltp(args, ChaosConfig(), Stopper()) == 0


# --------------------------------------------------------------------------- gps mode


def test_gps_mode_produces_to_the_topic(clean_db: str, capsys: pytest.CaptureFixture[str]) -> None:
    main(
        [
            "--mode",
            "seed",
            "--dsn",
            clean_db,
            "--customers",
            "10",
            "--restaurants",
            "3",
            "--riders",
            "8",
        ]
    )
    capsys.readouterr()

    topic = f"test.cli.{uuid.uuid4().hex[:8]}"
    try:
        status = main(
            [
                "--mode",
                "gps",
                "--dsn",
                clean_db,
                "--bootstrap",
                BOOTSTRAP,
                "--topic",
                topic,
                "--duration",
                "2",
            ]
        )
        assert status == 0
        out = capsys.readouterr().out
        assert "pings=" in out
        assert topic in out
    finally:
        _delete_topic(topic)


def test_gps_mode_refuses_to_run_without_riders(
    clean_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """An empty database is a setup mistake, not a silent no-op."""
    status = main(["--mode", "gps", "--dsn", clean_db, "--duration", "1"])
    assert status == 2
    assert "run --mode seed first" in capsys.readouterr().err


def _delete_topic(name: str) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for future in admin.delete_topics([name]).values():
        # Cleanup must never mask the failure actually being reported.
        with contextlib.suppress(Exception):
            future.result(timeout=10)


# --------------------------------------------------------------------------- both mode


def test_both_mode_runs_the_oltp_and_gps_paths(
    clean_db: str, capsys: pytest.CaptureFixture[str]
) -> None:
    main(
        [
            "--mode",
            "seed",
            "--dsn",
            clean_db,
            "--customers",
            "15",
            "--restaurants",
            "4",
            "--riders",
            "6",
        ]
    )
    capsys.readouterr()

    topic = f"test.cli.{uuid.uuid4().hex[:8]}"
    try:
        status = main(
            [
                "--mode",
                "both",
                "--dsn",
                clean_db,
                "--bootstrap",
                BOOTSTRAP,
                "--topic",
                topic,
                "--orders",
                "10",
                "--duration",
                "2",
            ]
        )
        assert status == 0
        out = capsys.readouterr().out
        assert "oltp     :" in out
        assert "gps      :" in out
        assert count("orders") == 10
    finally:
        _delete_topic(topic)


# --------------------------------------------------------------------------- slot hygiene


def test_dropping_a_slot_that_was_never_created_is_a_no_op(clean_db: str) -> None:
    with psycopg.connect(clean_db, autocommit=True) as conn, conn.cursor() as cur:
        slot = LogicalSlot(cur, "sh_never_created")
        assert slot.created is False
        slot.drop()  # must not raise, must not query
        cur.execute(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", ("sh_never_created",)
        )
        assert int(cur.fetchone()[0]) == 0


def test_untracked_tables_are_skipped_by_the_decoder(clean_db: str) -> None:
    """A change to a table outside _PK_BY_TABLE must not become a ChangeKey."""
    with psycopg.connect(clean_db, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS untracked (id bigint PRIMARY KEY, note text)")
        with LogicalSlot(cur, f"sh_test_{uuid.uuid4().hex[:8]}") as slot:
            cur.execute("INSERT INTO untracked VALUES (1, 'ignore me')")
            assert slot.peek() == (), "an untracked table leaked into the change stream"
        cur.execute("DROP TABLE untracked")
