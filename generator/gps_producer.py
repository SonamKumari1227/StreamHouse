"""Rider GPS ping producer.

Riders walk interpolated routes between points in their own city, emitting a position fix
every `gps_ping_interval_s` simulated seconds. Pings are Avro-encoded against
`contracts/gps_ping.v1.avsc` and produced to Redpanda.

Same split as the rest of the generator: the geometry and the scheduling are pure and
unit-tested with no broker in sight, and everything that touches Kafka sits behind
`MessageSink`.

**Encoding.** Phase 1 writes bare Avro via `fastavro.schemaless_writer`. Phase 2 switches to
the Confluent wire format — a magic byte plus the registry's schema id — once the schema is
actually registered. The `.avsc` file is the contract either way; only the framing changes.
Decoding a Phase 1 message therefore needs the schema out of band, which is exactly why the
registry exists and is worth saying out loud in the ADR.

**event_ts is when the device recorded the fix, not when it was produced.** Chaos scenario 1
will delay delivery long past it. Phase 3's watermark depends on that distinction, so nothing
here may quietly substitute the send time.
"""

from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from fastavro import parse_schema, schemaless_writer

from generator.config import CITIES, City, LoadConfig, pick_city
from generator.oltp_generator import Clock, RealClock

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "GPS_TOPIC",
    "GpsPing",
    "GpsProducer",
    "KafkaSink",
    "MessageSink",
    "RecordingSink",
    "RiderTrack",
    "Route",
    "encode_ping",
    "load_schema",
]

GPS_TOPIC = "gps.pings"

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "contracts" / "gps_ping.v1.avsc"

#: Degrees of latitude per kilometre. Close enough anywhere; longitude is scaled by cos(lat).
_DEG_PER_KM = 1.0 / 111.32

#: A rider covers this much ground in one leg before picking a new destination.
_LEG_KM = (0.8, 6.0)

#: Riding speed in km/h. Urban two-wheeler traffic, not a motorway.
_SPEED_KMPH = (12.0, 34.0)

#: Reported GPS accuracy in metres. Urban canyons are the reason this is not a constant.
_ACCURACY_M = (4.0, 28.0)

#: Chance a rider is stationary at any given ping — a light, a pickup, a handover.
_STOPPED_PROBABILITY = 0.12


def load_schema() -> Any:
    """Parse the Avro contract once. Raises if the file is missing or malformed."""
    with SCHEMA_PATH.open(encoding="utf-8") as handle:
        return parse_schema(json.load(handle))


@dataclass(frozen=True, slots=True)
class GpsPing:
    rider_id: int
    trip_id: str
    lat: float
    lon: float
    speed_kmph: float
    heading_deg: float
    accuracy_m: float
    event_ts: datetime

    def __post_init__(self) -> None:
        if not -90.0 <= self.lat <= 90.0:
            raise ValueError(f"lat out of range: {self.lat}")
        if not -180.0 <= self.lon <= 180.0:
            raise ValueError(f"lon out of range: {self.lon}")
        if self.speed_kmph < 0:
            raise ValueError(f"speed_kmph must be >= 0, got {self.speed_kmph}")
        if not 0.0 <= self.heading_deg < 360.0:
            raise ValueError(f"heading_deg must be in [0, 360), got {self.heading_deg}")
        if self.event_ts.tzinfo is None:
            raise ValueError("event_ts must be timezone-aware")

    def as_record(self) -> dict[str, Any]:
        """The Avro record. `event_ts` stays a datetime; fastavro encodes the logical type."""
        return {
            "rider_id": self.rider_id,
            "trip_id": self.trip_id,
            "lat": self.lat,
            "lon": self.lon,
            "speed_kmph": self.speed_kmph,
            "heading_deg": self.heading_deg,
            "accuracy_m": self.accuracy_m,
            "event_ts": self.event_ts,
        }


def encode_ping(ping: GpsPing, schema: Any) -> bytes:
    buffer = BytesIO()
    schemaless_writer(buffer, schema, ping.as_record())
    return buffer.getvalue()


def _bearing(from_lat: float, from_lon: float, to_lat: float, to_lon: float) -> float:
    """Compass bearing in degrees, 0 = north."""
    d_lon = math.radians(to_lon - from_lon)
    lat1, lat2 = math.radians(from_lat), math.radians(to_lat)
    y = math.sin(d_lon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(d_lon)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


@dataclass(frozen=True, slots=True)
class Route:
    """A straight leg between two points, sampled by fraction travelled."""

    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float

    def at(self, fraction: float) -> tuple[float, float]:
        f = min(1.0, max(0.0, fraction))
        return (
            self.start_lat + (self.end_lat - self.start_lat) * f,
            self.start_lon + (self.end_lon - self.start_lon) * f,
        )

    @property
    def bearing(self) -> float:
        return _bearing(self.start_lat, self.start_lon, self.end_lat, self.end_lon)


class RiderTrack:
    """One rider's movement. Picks a destination, walks to it, picks another."""

    def __init__(self, rider_id: int, city: City, rng: random.Random) -> None:
        self.rider_id = rider_id
        self.city = city
        self._rng = rng
        self._lat, self._lon = city.lat, city.lon
        self.trip_index = 0
        self._leg_km = 0.0
        self.route = self._new_route()
        self._progress = 0.0
        self._speed = rng.uniform(*_SPEED_KMPH)

    def _new_route(self) -> Route:
        # The leg length is stored, not redrawn per ping. Redrawing made `_progress`
        # advance by an inconsistent fraction each tick, so a rider's arrival time bore
        # no relation to the distance it was supposedly covering.
        distance_km = self._rng.uniform(*_LEG_KM)
        self._leg_km = distance_km
        heading = self._rng.uniform(0.0, 2.0 * math.pi)
        d_lat = distance_km * _DEG_PER_KM * math.cos(heading)
        d_lon = distance_km * _DEG_PER_KM * math.sin(heading) / math.cos(math.radians(self._lat))
        self.trip_index += 1
        return Route(self._lat, self._lon, self._lat + d_lat, self._lon + d_lon)

    @property
    def trip_id(self) -> str:
        return f"trip-{self.rider_id}-{self.trip_index}"

    def ping(self, now: datetime, interval_s: float) -> GpsPing:
        """Advance along the route by one interval and report the new position."""
        stopped = self._rng.random() < _STOPPED_PROBABILITY
        speed = 0.0 if stopped else self._speed

        travelled_km = speed * (interval_s / 3600.0)
        self._progress = min(1.0, self._progress + travelled_km / max(self._leg_km, 0.001))

        self._lat, self._lon = self.route.at(self._progress)
        bearing = self.route.bearing

        if self._progress >= 1.0:
            # Arrived: start a fresh leg, which also starts a new trip_id.
            self.route = self._new_route()
            self._progress = 0.0
            self._speed = self._rng.uniform(*_SPEED_KMPH)

        return GpsPing(
            rider_id=self.rider_id,
            trip_id=self.trip_id,
            lat=round(self._lat, 6),
            lon=round(self._lon, 6),
            speed_kmph=round(speed, 2),
            heading_deg=round(bearing, 2),
            accuracy_m=round(self._rng.uniform(*_ACCURACY_M), 2),
            event_ts=now,
        )


class MessageSink(Protocol):
    def send(self, topic: str, key: bytes | None, value: bytes, timestamp_ms: int) -> None: ...
    def flush(self, timeout: float = ...) -> int: ...


@dataclass
class RecordingSink:
    """An in-memory sink. Lets the whole producer be tested without a broker."""

    messages: list[tuple[str, bytes | None, bytes, int]]

    def __init__(self) -> None:
        self.messages = []
        self.flushed = 0

    def send(self, topic: str, key: bytes | None, value: bytes, timestamp_ms: int) -> None:
        self.messages.append((topic, key, value, timestamp_ms))

    def flush(self, timeout: float = 10.0) -> int:
        self.flushed += 1
        return 0


class KafkaSink:
    """`MessageSink` over confluent_kafka.Producer.

    Delivery errors are collected rather than raised per message: a producer that raised on
    every transient error would stop a load generator dead, which is not what a load
    generator is for. `failures` is checked after `flush`.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092", **config: Any) -> None:
        from confluent_kafka import Producer

        self.failures: list[str] = []
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 20,
                "compression.type": "snappy",
                "enable.idempotence": True,
                **config,
            }
        )

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self.failures.append(str(err))

    def send(self, topic: str, key: bytes | None, value: bytes, timestamp_ms: int) -> None:
        self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            timestamp=timestamp_ms,
            on_delivery=self._on_delivery,
        )
        # Serve delivery callbacks without blocking.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return int(self._producer.flush(timeout))


@dataclass
class GpsReport:
    pings: int = 0
    trips_started: int = 0
    bytes_sent: int = 0

    @property
    def average_message_bytes(self) -> float:
        return self.bytes_sent / self.pings if self.pings else 0.0


class GpsProducer:
    """Emits pings for every rider on a fixed interval, throttled to a message ceiling."""

    def __init__(
        self,
        sink: MessageSink,
        rider_ids: tuple[int, ...],
        load: LoadConfig | None = None,
        rng: random.Random | None = None,
        clock: Clock | None = None,
        topic: str = GPS_TOPIC,
        cities: tuple[City, ...] = CITIES,
    ) -> None:
        if not rider_ids:
            raise ValueError("no riders to track; run the seeder first")

        self.sink = sink
        self.load = load or LoadConfig()
        self.rng = rng or random.Random(0)
        self.clock = clock or RealClock()
        self.topic = topic
        self.schema = load_schema()
        self.report = GpsReport()

        self.tracks = tuple(
            RiderTrack(rider_id, pick_city(self.rng, cities), self.rng) for rider_id in rider_ids
        )
        self._seen_trips = {t.trip_id for t in self.tracks}
        self._next_batch_at = self.clock.now()

    @property
    def batch_interval_s(self) -> float:
        """Simulated ping interval, compressed by speed like every other duration."""
        return self.load.gps_ping_interval_s / self.load.speed

    def _throttled_tracks(self) -> Iterator[RiderTrack]:
        """Riders to report on this tick, capped by the message ceiling.

        With more riders than the ceiling allows, a rotating slice is sampled rather than the
        same prefix every time — otherwise the tail of the fleet would never move.
        """
        ceiling = max(1, int(self.load.gps_max_msgs_per_second * self.batch_interval_s))
        if len(self.tracks) <= ceiling:
            return iter(self.tracks)
        return iter(self.rng.sample(self.tracks, ceiling))

    def emit_batch(self, now: datetime) -> int:
        """One ping per eligible rider. Returns how many were sent."""
        sent = 0
        for track in self._throttled_tracks():
            ping = track.ping(now, self.load.gps_ping_interval_s)
            if ping.trip_id not in self._seen_trips:
                self._seen_trips.add(ping.trip_id)
                self.report.trips_started += 1
            payload = encode_ping(ping, self.schema)
            self.sink.send(
                topic=self.topic,
                # Keyed by rider so one rider's pings keep their order within a partition.
                key=str(ping.rider_id).encode(),
                value=payload,
                timestamp_ms=int(ping.event_ts.timestamp() * 1000),
            )
            self.report.pings += 1
            self.report.bytes_sent += len(payload)
            sent += 1
        return sent

    def run(
        self,
        max_batches: int | None = None,
        max_pings: int | None = None,
        until: datetime | None = None,
    ) -> GpsReport:
        if max_batches is None and max_pings is None and until is None:
            raise ValueError("run() needs max_batches, max_pings or until")

        batches = 0
        while True:
            if max_batches is not None and batches >= max_batches:
                break
            if max_pings is not None and self.report.pings >= max_pings:
                break
            if until is not None and self.clock.now() >= until:
                break

            now = self.clock.now()
            if now < self._next_batch_at:
                self.clock.sleep(min((self._next_batch_at - now).total_seconds(), 0.25))
                continue

            self.emit_batch(now)
            batches += 1
            self._next_batch_at = now + timedelta(seconds=self.batch_interval_s)

        self.sink.flush()
        return self.report


def utc_now() -> datetime:
    return datetime.now(UTC)
