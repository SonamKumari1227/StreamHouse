"""Unit tests for the GPS ping producer.

No broker. `RecordingSink` captures what would have been sent, and every message is decoded
back through the Avro contract — a round trip is the only way to know the encoding is real
rather than merely bytes-shaped.
"""

from __future__ import annotations

import io
import json
import math
import random
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from fastavro import schemaless_reader

from generator.config import CITIES, LoadConfig
from generator.gps_producer import (
    GPS_TOPIC,
    SCHEMA_PATH,
    GpsPing,
    GpsProducer,
    RecordingSink,
    RiderTrack,
    Route,
    encode_ping,
    load_schema,
    utc_now,
)
from generator.oltp_generator import ManualClock

T0 = datetime(2026, 9, 24, 12, 0, 0, tzinfo=UTC)
SCHEMA = load_schema()


def decode(payload: bytes) -> dict[str, Any]:
    """fastavro returns Any; the contract guarantees the shape."""
    return cast("dict[str, Any]", schemaless_reader(io.BytesIO(payload), SCHEMA))


def make_producer(
    riders: int = 8,
    seed: int = 5,
    load: LoadConfig | None = None,
) -> tuple[GpsProducer, RecordingSink, ManualClock]:
    sink = RecordingSink()
    clock = ManualClock(T0)
    producer = GpsProducer(
        sink=sink,
        rider_ids=tuple(range(1, riders + 1)),
        load=load or LoadConfig(),
        rng=random.Random(seed),
        clock=clock,
    )
    return producer, sink, clock


# --------------------------------------------------------------------------- the contract


def test_the_avro_contract_file_exists_and_parses() -> None:
    assert SCHEMA_PATH.is_file(), f"contract missing at {SCHEMA_PATH}"
    raw = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert raw["name"] == "GpsPing"
    assert raw["namespace"] == "streamhouse.gps"


def test_the_contract_declares_every_field_the_ping_carries() -> None:
    raw = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    declared = {f["name"] for f in raw["fields"]}
    assert declared == {
        "rider_id",
        "trip_id",
        "lat",
        "lon",
        "speed_kmph",
        "heading_deg",
        "accuracy_m",
        "event_ts",
    }


def test_event_ts_is_a_logical_timestamp() -> None:
    """Phase 3 watermarks on this column; a bare long would lose the semantics."""
    raw = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    event_ts = next(f for f in raw["fields"] if f["name"] == "event_ts")
    assert event_ts["type"]["logicalType"] == "timestamp-millis"


def test_every_contract_field_is_documented() -> None:
    raw = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    for field in raw["fields"]:
        assert field.get("doc"), f"{field['name']} has no doc"


# --------------------------------------------------------------------------- encoding


def test_a_ping_survives_an_avro_round_trip() -> None:
    ping = GpsPing(
        rider_id=7,
        trip_id="trip-7-1",
        lat=12.971600,
        lon=77.594600,
        speed_kmph=18.5,
        heading_deg=91.25,
        accuracy_m=6.5,
        event_ts=T0,
    )
    decoded = decode(encode_ping(ping, SCHEMA))
    assert decoded["rider_id"] == 7
    assert decoded["trip_id"] == "trip-7-1"
    assert decoded["lat"] == pytest.approx(12.9716)
    assert decoded["lon"] == pytest.approx(77.5946)
    assert decoded["speed_kmph"] == pytest.approx(18.5)
    assert decoded["heading_deg"] == pytest.approx(91.25)
    assert decoded["accuracy_m"] == pytest.approx(6.5)
    assert decoded["event_ts"] == T0


def test_encoded_pings_are_compact() -> None:
    """Avro over ~5M messages/day: payload size is not a detail."""
    ping = GpsPing(1, "trip-1-1", 12.9716, 77.5946, 20.0, 180.0, 5.0, T0)
    assert len(encode_ping(ping, SCHEMA)) < 80


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"lat": 91.0}, "lat out of range"),
        ({"lon": -181.0}, "lon out of range"),
        ({"speed_kmph": -1.0}, "speed_kmph must be >= 0"),
        ({"heading_deg": 360.0}, r"heading_deg must be in \[0, 360\)"),
        ({"event_ts": datetime(2026, 9, 24, 12, 0, 0)}, "timezone-aware"),
    ],
    ids=["bad lat", "bad lon", "negative speed", "heading 360", "naive timestamp"],
)
def test_ping_rejects_invalid_values(kwargs: dict[str, Any], match: str) -> None:
    base: dict[str, Any] = {
        "rider_id": 1,
        "trip_id": "trip-1-1",
        "lat": 12.9716,
        "lon": 77.5946,
        "speed_kmph": 20.0,
        "heading_deg": 180.0,
        "accuracy_m": 5.0,
        "event_ts": T0,
    }
    base.update(kwargs)
    with pytest.raises(ValueError, match=match):
        GpsPing(**base)


# --------------------------------------------------------------------------- geometry


def test_route_interpolates_between_its_endpoints() -> None:
    route = Route(start_lat=10.0, start_lon=20.0, end_lat=11.0, end_lon=22.0)
    assert route.at(0.0) == (10.0, 20.0)
    assert route.at(1.0) == (11.0, 22.0)
    mid_lat, mid_lon = route.at(0.5)
    assert mid_lat == pytest.approx(10.5)
    assert mid_lon == pytest.approx(21.0)


def test_route_clamps_fractions_outside_zero_to_one() -> None:
    route = Route(10.0, 20.0, 11.0, 22.0)
    assert route.at(-5.0) == (10.0, 20.0)
    assert route.at(9.0) == (11.0, 22.0)


@pytest.mark.parametrize(
    ("end_lat", "end_lon", "expected"),
    [(11.0, 20.0, 0.0), (10.0, 21.0, 90.0), (9.0, 20.0, 180.0), (10.0, 19.0, 270.0)],
    ids=["north", "east", "south", "west"],
)
def test_route_bearing_points_the_right_way(
    end_lat: float, end_lon: float, expected: float
) -> None:
    bearing = Route(10.0, 20.0, end_lat, end_lon).bearing
    assert bearing == pytest.approx(expected, abs=0.5)


def test_a_track_stays_near_its_city() -> None:
    rng = random.Random(3)
    city = CITIES[0]
    track = RiderTrack(rider_id=1, city=city, rng=rng)
    for _ in range(300):
        ping = track.ping(T0, 5.0)
        # Legs are at most ~6 km; a rider must not wander across India.
        assert abs(ping.lat - city.lat) < 1.5
        assert abs(ping.lon - city.lon) < 1.5


def test_a_track_starts_a_new_trip_when_it_arrives() -> None:
    rng = random.Random(4)
    track = RiderTrack(rider_id=1, city=CITIES[0], rng=rng)
    trips = {track.ping(T0 + timedelta(seconds=5 * n), 5.0).trip_id for n in range(400)}
    assert len(trips) > 1, "a rider that never completes a leg produces one endless trip"


def test_trip_ids_identify_their_rider() -> None:
    track = RiderTrack(rider_id=42, city=CITIES[0], rng=random.Random(1))
    assert track.ping(T0, 5.0).trip_id.startswith("trip-42-")


def test_some_pings_report_a_stationary_rider() -> None:
    """Riders stop at lights and for handovers; a fleet that never stops is not real."""
    track = RiderTrack(rider_id=1, city=CITIES[0], rng=random.Random(9))
    speeds = [track.ping(T0, 5.0).speed_kmph for _ in range(400)]
    assert any(s == 0.0 for s in speeds)
    assert any(s > 0.0 for s in speeds)


def test_headings_are_always_a_valid_compass_bearing() -> None:
    track = RiderTrack(rider_id=1, city=CITIES[0], rng=random.Random(2))
    for _ in range(300):
        assert 0.0 <= track.ping(T0, 5.0).heading_deg < 360.0


def test_coordinates_are_rounded_to_six_decimal_places() -> None:
    track = RiderTrack(rider_id=1, city=CITIES[0], rng=random.Random(6))
    for _ in range(100):
        ping = track.ping(T0, 5.0)
        assert round(ping.lat, 6) == ping.lat
        assert round(ping.lon, 6) == ping.lon


# --------------------------------------------------------------------------- the producer


def test_producer_requires_riders() -> None:
    with pytest.raises(ValueError, match="no riders to track"):
        GpsProducer(sink=RecordingSink(), rider_ids=())


def test_a_batch_emits_one_ping_per_rider() -> None:
    producer, sink, _ = make_producer(riders=8)
    assert producer.emit_batch(T0) == 8
    assert len(sink.messages) == 8
    assert {decode(m[2])["rider_id"] for m in sink.messages} == set(range(1, 9))


def test_messages_go_to_the_gps_topic_keyed_by_rider() -> None:
    producer, sink, _ = make_producer(riders=5)
    producer.emit_batch(T0)
    for topic, key, value, _ts in sink.messages:
        assert topic == GPS_TOPIC
        assert key is not None
        assert int(key.decode()) == decode(value)["rider_id"]


def test_the_kafka_timestamp_matches_event_ts() -> None:
    """Producing must not substitute the send time for the device time."""
    producer, sink, _ = make_producer(riders=4)
    producer.emit_batch(T0)
    for _topic, _key, value, timestamp_ms in sink.messages:
        assert timestamp_ms == int(decode(value)["event_ts"].timestamp() * 1000)


def test_every_produced_message_decodes_against_the_contract() -> None:
    producer, sink, _ = make_producer(riders=10)
    producer.run(max_batches=12)
    assert len(sink.messages) == 120
    for _topic, _key, value, _ts in sink.messages:
        record = decode(value)
        assert -90 <= record["lat"] <= 90
        assert -180 <= record["lon"] <= 180
        assert record["speed_kmph"] >= 0
        assert 0 <= record["heading_deg"] < 360
        assert record["accuracy_m"] > 0


def test_throughput_is_capped_by_the_message_ceiling() -> None:
    """More riders than the ceiling allows must not produce one ping each per tick."""
    load = LoadConfig(gps_max_msgs_per_second=20, gps_ping_interval_s=5.0, speed=20.0)
    producer, sink, _ = make_producer(riders=200, load=load)
    ceiling = int(load.gps_max_msgs_per_second * producer.batch_interval_s)
    assert producer.emit_batch(T0) == ceiling
    assert len(sink.messages) == ceiling


def test_throttling_rotates_so_the_whole_fleet_moves() -> None:
    """Sampling the same prefix every tick would freeze the tail of the fleet."""
    load = LoadConfig(gps_max_msgs_per_second=20, gps_ping_interval_s=5.0, speed=20.0)
    producer, sink, _ = make_producer(riders=200, load=load)
    producer.run(max_batches=60)
    reported = {decode(m[2])["rider_id"] for m in sink.messages}
    assert len(reported) > 100, f"only {len(reported)} of 200 riders ever reported"


def test_batch_interval_is_compressed_by_speed() -> None:
    real_time, _, _ = make_producer(load=LoadConfig(speed=1.0, gps_ping_interval_s=5.0))
    fast, _, _ = make_producer(load=LoadConfig(speed=20.0, gps_ping_interval_s=5.0))
    assert real_time.batch_interval_s == 5.0
    assert fast.batch_interval_s == pytest.approx(0.25)


def test_run_advances_event_time_between_batches() -> None:
    producer, sink, clock = make_producer(riders=3)
    producer.run(max_batches=5)
    timestamps = sorted({m[3] for m in sink.messages})
    assert len(timestamps) == 5, "each batch should carry its own event time"
    assert clock.now() > T0


def test_run_requires_a_stopping_condition() -> None:
    producer, _, _ = make_producer()
    with pytest.raises(ValueError, match="max_batches, max_pings or until"):
        producer.run()


def test_run_respects_max_pings() -> None:
    producer, sink, _ = make_producer(riders=6)
    report = producer.run(max_pings=30)
    assert report.pings >= 30
    assert len(sink.messages) == report.pings


def test_run_respects_a_deadline() -> None:
    producer, _, clock = make_producer(riders=4)
    report = producer.run(until=T0 + timedelta(seconds=3))
    assert clock.now() >= T0 + timedelta(seconds=3)
    assert report.pings > 0


def test_run_flushes_the_sink() -> None:
    producer, sink, _ = make_producer()
    producer.run(max_batches=2)
    assert sink.flushed == 1


def test_the_report_counts_pings_trips_and_bytes() -> None:
    producer, sink, _ = make_producer(riders=6)
    report = producer.run(max_batches=40)
    assert report.pings == len(sink.messages)
    assert report.trips_started >= 0
    assert report.bytes_sent == sum(len(m[2]) for m in sink.messages)
    assert 0 < report.average_message_bytes < 100


def test_an_empty_report_has_no_average() -> None:
    producer, _, _ = make_producer()
    assert producer.report.average_message_bytes == 0.0


def test_the_same_seed_produces_the_same_stream() -> None:
    a_producer, a_sink, _ = make_producer(seed=31)
    b_producer, b_sink, _ = make_producer(seed=31)
    a_producer.run(max_batches=10)
    b_producer.run(max_batches=10)
    assert [m[2] for m in a_sink.messages] == [m[2] for m in b_sink.messages]


def test_riders_are_spread_across_cities() -> None:
    producer, _, _ = make_producer(riders=300, seed=17)
    assert len({t.city.name for t in producer.tracks}) > 3


def test_recording_sink_flush_reports_nothing_outstanding() -> None:
    sink = RecordingSink()
    assert sink.flush() == 0


def test_utc_now_is_timezone_aware() -> None:
    assert utc_now().tzinfo is not None


def test_a_ping_is_roughly_where_the_previous_one_was() -> None:
    """No teleporting: consecutive fixes on one trip must be physically plausible."""
    track = RiderTrack(rider_id=1, city=CITIES[0], rng=random.Random(8))
    previous = track.ping(T0, 5.0)
    for n in range(1, 200):
        current = track.ping(T0 + timedelta(seconds=5 * n), 5.0)
        if current.trip_id != previous.trip_id:
            previous = current
            continue
        km = math.dist((previous.lat, previous.lon), (current.lat, current.lon)) * 111.32
        assert km < 1.0, f"jumped {km:.2f} km between consecutive pings"
        previous = current


def test_a_leg_takes_the_time_its_distance_and_speed_imply() -> None:
    """Regression: the leg length used to be redrawn on every ping, so arrival time was
    unrelated to the distance being covered."""
    rng = random.Random(77)
    track = RiderTrack(rider_id=1, city=CITIES[0], rng=rng)
    leg_km = track._leg_km
    speed = track._speed
    start_trip = track.trip_id

    interval_s = 5.0
    pings = 0
    while track.ping(T0, interval_s).trip_id == start_trip and pings < 5000:
        pings += 1

    # Ignoring the ~12% of ticks where the rider is stationary, distance = speed * time.
    expected = leg_km / (speed * interval_s / 3600.0)
    assert (
        expected * 0.8 < pings < expected * 1.6
    ), f"took {pings} pings to cover {leg_km:.2f} km at {speed:.1f} km/h; expected ~{expected:.0f}"


def test_leg_length_is_stable_within_a_trip() -> None:
    track = RiderTrack(rider_id=1, city=CITIES[0], rng=random.Random(3))
    trip = track.trip_id
    leg_km = track._leg_km
    for _ in range(20):
        if track.ping(T0, 5.0).trip_id != trip:
            break
        assert track._leg_km == leg_km


def test_trips_started_counts_riders_completing_a_leg() -> None:
    """A short demo run shows 0 because a leg takes ~10 simulated minutes. Over a long
    enough run the counter must actually move, or sessionization has nothing to segment."""
    producer, sink, _ = make_producer(riders=2, seed=19)
    report = producer.run(max_batches=600)
    assert report.trips_started > 0
    trip_ids = {decode(m[2])["trip_id"] for m in sink.messages}
    assert len(trip_ids) > 2, "each rider should have produced more than one trip"


def test_kafka_sink_records_delivery_failures_instead_of_raising() -> None:
    """A load generator that dies on one transient delivery error is not a load generator."""
    from generator.gps_producer import KafkaSink

    sink = KafkaSink(bootstrap_servers="localhost:1")  # never connected to
    assert sink.failures == []
    sink._on_delivery("broker transport failure", None)
    sink._on_delivery(None, None)
    assert sink.failures == ["broker transport failure"]
