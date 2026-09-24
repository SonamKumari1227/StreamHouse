"""The GPS producer against a real Redpanda broker.

The unit tests prove the geometry, the throttling and the Avro round trip in memory. They
cannot prove that a message reaches a broker, survives partitioning, keeps its timestamp, or
comes back byte-identical. That is what this file does: produce to a real topic, consume the
messages back, and decode them against the contract.

Each test uses its own throwaway topic, so nothing collides with the topics Phase 2 will
create and nothing is left behind.

Run with:  pytest -m integration
Needs the core stack up:  make up
"""

from __future__ import annotations

import contextlib
import io
import os
import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from fastavro import schemaless_reader

from generator.config import LoadConfig
from generator.gps_producer import GpsProducer, KafkaSink, load_schema
from generator.oltp_generator import ManualClock

confluent_kafka = pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

pytestmark = pytest.mark.integration

T0 = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)
SCHEMA = load_schema()


def bootstrap() -> str:
    return os.environ.get("SH_TEST_BOOTSTRAP", "localhost:9092")


def decode(payload: bytes) -> dict[str, Any]:
    """fastavro returns Any; the contract guarantees the shape."""
    return cast("dict[str, Any]", schemaless_reader(io.BytesIO(payload), SCHEMA))


@pytest.fixture
def topic() -> Iterator[str]:
    """A throwaway topic, deleted afterwards."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap(), "socket.timeout.ms": 5000})
    try:
        metadata = admin.list_topics(timeout=5)
    except Exception as exc:
        pytest.skip(f"Redpanda is not reachable ({exc}); run `make up`")
    assert metadata is not None

    name = f"test.gps.{uuid.uuid4().hex[:8]}"
    for future in admin.create_topics(
        [NewTopic(name, num_partitions=3, replication_factor=1)]
    ).values():
        future.result(timeout=10)
    try:
        yield name
    finally:
        for future in admin.delete_topics([name]).values():
            # Cleanup must never mask the failure actually being reported.
            with contextlib.suppress(Exception):
                future.result(timeout=10)


def consume_all(topic_name: str, expected: int, timeout_s: float = 30.0) -> list[Any]:
    """Read `expected` messages from the beginning of the topic."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap(),
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic_name])
    collected: list[Any] = []
    deadline = datetime.now(UTC) + timedelta(seconds=timeout_s)
    try:
        while len(collected) < expected and datetime.now(UTC) < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                continue
            collected.append(message)
    finally:
        consumer.close()
    return collected


def produce(topic_name: str, riders: int, batches: int, seed: int = 5) -> tuple[Any, KafkaSink]:
    sink = KafkaSink(bootstrap_servers=bootstrap())
    producer = GpsProducer(
        sink=sink,
        rider_ids=tuple(range(1, riders + 1)),
        load=LoadConfig(),
        rng=random.Random(seed),
        clock=ManualClock(T0),
        topic=topic_name,
    )
    report = producer.run(max_batches=batches)
    return report, sink


# --------------------------------------------------------------------------- round trip


def test_messages_reach_the_broker_and_come_back(topic: str) -> None:
    report, sink = produce(topic, riders=10, batches=5)
    assert report.pings == 50
    assert sink.failures == [], f"delivery failures: {sink.failures}"

    messages = consume_all(topic, expected=50)
    assert len(messages) == 50


def test_consumed_payloads_decode_against_the_contract(topic: str) -> None:
    produce(topic, riders=8, batches=4)
    messages = consume_all(topic, expected=32)
    assert len(messages) == 32

    for message in messages:
        record = decode(message.value())
        assert 1 <= record["rider_id"] <= 8
        assert record["trip_id"].startswith(f"trip-{record['rider_id']}-")
        assert -90 <= record["lat"] <= 90
        assert -180 <= record["lon"] <= 180
        assert record["speed_kmph"] >= 0
        assert 0 <= record["heading_deg"] < 360
        assert record["accuracy_m"] > 0
        assert record["event_ts"].tzinfo is not None


def test_the_broker_keeps_the_event_timestamp(topic: str) -> None:
    """Phase 3 watermarks on event time; a broker-assigned time would break that."""
    from confluent_kafka import TIMESTAMP_CREATE_TIME

    produce(topic, riders=5, batches=3)
    messages = consume_all(topic, expected=15)
    assert len(messages) == 15

    for message in messages:
        kind, value = message.timestamp()
        assert kind == TIMESTAMP_CREATE_TIME
        assert value == int(decode(message.value())["event_ts"].timestamp() * 1000)


def test_messages_are_keyed_by_rider(topic: str) -> None:
    produce(topic, riders=6, batches=3)
    messages = consume_all(topic, expected=18)
    assert len(messages) == 18
    for message in messages:
        assert int(message.key().decode()) == decode(message.value())["rider_id"]


def test_one_riders_pings_all_land_on_one_partition(topic: str) -> None:
    """Keying exists so per-rider order survives. Without it, sessionization is guesswork."""
    produce(topic, riders=12, batches=6)
    messages = consume_all(topic, expected=72)
    assert len(messages) == 72

    partitions: dict[int, set[int]] = {}
    for message in messages:
        rider_id = int(message.key().decode())
        partitions.setdefault(rider_id, set()).add(message.partition())

    for rider_id, seen in partitions.items():
        assert len(seen) == 1, f"rider {rider_id} was spread across partitions {seen}"
    assert len({next(iter(v)) for v in partitions.values()}) > 1, "all riders on one partition"


def test_per_rider_offsets_increase_with_event_time(topic: str) -> None:
    produce(topic, riders=4, batches=8)
    messages = consume_all(topic, expected=32)
    assert len(messages) == 32

    by_rider: dict[int, list[tuple[int, datetime]]] = {}
    for message in messages:
        rider_id = int(message.key().decode())
        by_rider.setdefault(rider_id, []).append(
            (message.offset(), decode(message.value())["event_ts"])
        )

    for rider_id, rows in by_rider.items():
        ordered = [ts for _offset, ts in sorted(rows)]
        assert ordered == sorted(ordered), f"rider {rider_id} pings are out of order on the log"


# --------------------------------------------------------------------------- throughput


def test_a_larger_burst_is_delivered_without_loss(topic: str) -> None:
    # 40 riders sits under the default ceiling (200 msg/s * 0.25s batch = 50), so every
    # rider reports every batch and the expected count is exact. Throttling is covered
    # separately in the unit tests; conflating the two here just hid an arithmetic slip.
    report, sink = produce(topic, riders=40, batches=20)
    assert report.pings == 800
    assert sink.failures == []

    messages = consume_all(topic, expected=800, timeout_s=60)
    assert len(messages) == 800, f"produced 800, consumed {len(messages)}"
    assert len({int(m.key().decode()) for m in messages}) == 40


def test_payloads_stay_compact_on_the_wire(topic: str) -> None:
    """~5M messages/day: bytes per message is a capacity question, not a detail."""
    report, _ = produce(topic, riders=20, batches=5)
    messages = consume_all(topic, expected=100)
    assert len(messages) == 100
    average = sum(len(m.value()) for m in messages) / len(messages)
    assert average < 80, f"average payload {average:.1f} bytes"
    assert report.average_message_bytes == pytest.approx(average, rel=0.01)


def test_the_topic_is_reported_by_the_broker(topic: str) -> None:
    from confluent_kafka.admin import AdminClient

    produce(topic, riders=3, batches=2)
    admin = AdminClient({"bootstrap.servers": bootstrap()})
    metadata = admin.list_topics(topic=topic, timeout=10)
    assert topic in metadata.topics
    assert len(metadata.topics[topic].partitions) == 3
