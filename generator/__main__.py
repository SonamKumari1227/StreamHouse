"""Command-line entry point for the generators.

    python -m generator --mode oltp  --speed 20 --orders 500
    python -m generator --mode gps   --duration 60
    python -m generator --mode both  --chaos duplicates,out-of-order
    python -m generator --mode seed  --reset

Every knob has a working default, so `python -m generator` alone does something sensible:
seeds if the database is empty, then runs both producers until interrupted.

Ctrl-C is a clean stop, not a stack trace — the run report still prints, because knowing what
a run did is the point of running it.
"""

from __future__ import annotations

import argparse
import random
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import FrameType
from typing import TYPE_CHECKING

from generator.chaos import IMPLEMENTED, ChaosConfig, ChaosScenario, parse_scenarios
from generator.config import DEFAULT_RNG_SEED, LoadConfig, SeedVolumes
from generator.gps_producer import GPS_TOPIC, GpsProducer, KafkaSink
from generator.oltp_generator import OltpGenerator, RealClock
from generator.repository import PostgresRepository
from generator.seed import build_reference_data, seed_database
from generator.state_machine import DEFAULT_SPEED, MachineConfig

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["build_parser", "main"]

DEFAULT_DSN = (
    "host=localhost port=5432 user=streamhouse password=streamhouse_local_dev dbname=streamhouse"
)
DEFAULT_BOOTSTRAP = "localhost:9092"

_TABLES = (
    "order_items",
    "payments",
    "orders",
    "menu_items",
    "restaurants",
    "customers",
    "riders",
)


@dataclass
class Stopper:
    """Turns SIGINT into a flag the loops can honour, instead of a traceback."""

    event: threading.Event

    def __init__(self) -> None:
        self.event = threading.Event()

    def install(self) -> None:
        def handler(_signum: int, _frame: FrameType | None) -> None:
            if self.event.is_set():
                # A second Ctrl-C means the user is not asking politely any more.
                raise KeyboardInterrupt
            print("\nstopping after the current event; Ctrl-C again to force", file=sys.stderr)
            self.event.set()

        signal.signal(signal.SIGINT, handler)

    @property
    def stopped(self) -> bool:
        return self.event.is_set()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m generator",
        description="StreamHouse synthetic source generators.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python -m generator --mode seed --reset\n"
            "  python -m generator --mode oltp --orders 500 --speed 20\n"
            "  python -m generator --mode gps --duration 60\n"
            "  python -m generator --mode both --chaos duplicates,out-of-order\n"
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("seed", "oltp", "gps", "both"),
        default="both",
        help="what to run (default: both)",
    )
    parser.add_argument(
        "--chaos",
        default="",
        metavar="LIST",
        help=(
            "comma-separated failure scenarios. "
            f"implemented: {', '.join(sorted(s.value for s in IMPLEMENTED))}. "
            f"specified but not built yet: "
            f"{', '.join(sorted(s.value for s in ChaosScenario if s not in IMPLEMENTED))}"
        ),
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=DEFAULT_SPEED,
        help=f"wall-clock compression (default: {DEFAULT_SPEED}; use 1 for real time)",
    )
    parser.add_argument("--orders", type=int, default=None, help="stop after N orders")
    parser.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    parser.add_argument(
        "--orders-per-day", type=int, default=50_000, help="arrival rate (default: 50000)"
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_RNG_SEED, help="RNG seed")
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Redpanda bootstrap servers")
    parser.add_argument("--topic", default=GPS_TOPIC, help=f"GPS topic (default: {GPS_TOPIC})")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="DELETE every row before seeding. Destructive; seed mode only.",
    )
    parser.add_argument(
        "--customers", type=int, default=SeedVolumes().customers, help="seed: customer count"
    )
    parser.add_argument(
        "--restaurants", type=int, default=SeedVolumes().restaurants, help="seed: restaurants"
    )
    parser.add_argument("--riders", type=int, default=SeedVolumes().riders, help="seed: riders")
    return parser


def _deadline(duration: float | None) -> datetime | None:
    return datetime.now(UTC) + timedelta(seconds=duration) if duration else None


def run_seed(args: argparse.Namespace) -> int:
    import psycopg

    volumes = SeedVolumes(
        customers=args.customers, restaurants=args.restaurants, riders=args.riders
    )
    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        if args.reset:
            print("--reset: deleting every row")
            for table in _TABLES:
                cur.execute(f"DELETE FROM {table}")
        report = seed_database(cur, build_reference_data(volumes, seed=args.seed))
        conn.commit()
    print(f"seeded   : {report.inserted}")
    print(f"skipped  : {report.skipped}")
    if report.was_noop:
        print("nothing to do — the reference data was already present")
    return 0


def run_oltp(args: argparse.Namespace, chaos: ChaosConfig, stopper: Stopper) -> int:
    import psycopg

    if args.orders is None and args.duration is None:
        args.duration = 60.0
        print("no --orders or --duration given; running for 60s")

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        generator = OltpGenerator(
            repository=PostgresRepository(cur),
            load=LoadConfig(orders_per_day=args.orders_per_day, speed=args.speed),
            machine=MachineConfig(speed=args.speed),
            rng=random.Random(args.seed),
            clock=RealClock(),
            transition_hook=chaos.transition_hook if chaos.active else None,
        )
        recovered = generator.recover()
        if recovered:
            print(f"recovered {recovered} orders left in flight by a previous run")
        conn.commit()

        report = generator.run(max_orders=args.orders, until=_deadline(args.duration))
        conn.commit()

    print(
        f"oltp     : placed={report.orders_placed} transitions={report.transitions} "
        f"delivered={report.delivered} cancelled={report.cancelled} "
        f"in_flight={generator.in_flight}"
    )
    if report.mutations:
        print(f"mutations: {report.mutations}")
    if stopper.stopped:
        print("stopped early on request")
    return 0


def run_gps(args: argparse.Namespace, stopper: Stopper) -> int:
    import psycopg

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT rider_id FROM riders ORDER BY rider_id")
        rider_ids = tuple(int(row[0]) for row in cur.fetchall())

    if not rider_ids:
        print("no riders in the database; run --mode seed first", file=sys.stderr)
        return 2

    sink = KafkaSink(bootstrap_servers=args.bootstrap)
    producer = GpsProducer(
        sink=sink,
        rider_ids=rider_ids,
        load=LoadConfig(orders_per_day=args.orders_per_day, speed=args.speed),
        rng=random.Random(args.seed),
        clock=RealClock(),
        topic=args.topic,
    )
    report = producer.run(until=_deadline(args.duration or 60.0))

    print(
        f"gps      : pings={report.pings} trips_started={report.trips_started} "
        f"avg_msg={report.average_message_bytes:.1f}B -> {args.topic}"
    )
    if sink.failures:
        print(
            f"delivery failures: {len(sink.failures)} (first: {sink.failures[0]})", file=sys.stderr
        )
        return 1
    if stopper.stopped:
        print("stopped early on request")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        scenarios = parse_scenarios(args.chaos)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.speed <= 0:
        print("error: --speed must be > 0", file=sys.stderr)
        return 2
    if args.reset and args.mode != "seed":
        print("error: --reset is destructive and only valid with --mode seed", file=sys.stderr)
        return 2

    chaos = ChaosConfig.build(scenarios, random.Random(args.seed))
    if chaos.active:
        print(f"chaos    : {', '.join(sorted(s.value for s in scenarios))}")
    if ChaosScenario.DUPLICATES in scenarios:
        # Being explicit rather than reporting a silent zero. A duplicate is the same WAL
        # record delivered twice, which only exists once something consumes the CDC stream.
        # The generator writes; it does not deliver. Phase 2 wires this to the real
        # connector; until then the proof lives in tests/integration/test_chaos_duplicates.py.
        print(
            "note     : 'duplicates' acts on CDC delivery, not on writes. The generator "
            "has no delivery\n           path until Phase 2, so it reports 0 here. See "
            "tests/integration/test_chaos_duplicates.py.",
            file=sys.stderr,
        )

    stopper = Stopper()
    stopper.install()

    try:
        if args.mode == "seed":
            return run_seed(args)
        if args.mode == "oltp":
            status = run_oltp(args, chaos, stopper)
        elif args.mode == "gps":
            status = run_gps(args, stopper)
        else:
            status = run_oltp(args, chaos, stopper)
            if status == 0:
                status = run_gps(args, stopper)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130

    if chaos.active:
        print(f"chaos    : {chaos.summary()}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
