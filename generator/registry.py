"""Minimal Confluent Schema Registry client, and the wire format it implies.

Only what StreamHouse needs: register a schema, look its id up, and frame a payload the way
every Confluent-compatible consumer expects. `confluent_kafka` ships a fuller client, but it
pulls in a serializer stack that wants to own encoding - and the GPS producer already encodes
with fastavro.

THE WIRE FORMAT
    byte 0      magic, always 0
    bytes 1-4   schema id, big-endian int32
    bytes 5..   the Avro payload, schemaless

Bare Avro - which Phase 1 wrote - is indistinguishable from a corrupt payload to any consumer
that expects this framing, because the first bytes of the record get read as the magic byte
and id. That is precisely why the registry exists: the payload alone does not say what it is.
"""

from __future__ import annotations

import json
import struct
import urllib.error
import urllib.request
from typing import Any

__all__ = [
    "MAGIC_BYTE",
    "WIRE_HEADER_BYTES",
    "SchemaRegistry",
    "frame",
    "unframe",
]

MAGIC_BYTE = 0
WIRE_HEADER_BYTES = 5

DEFAULT_URL = "http://localhost:8081"


def frame(schema_id: int, payload: bytes) -> bytes:
    """Prefix an Avro payload with the Confluent header."""
    if schema_id < 0:
        raise ValueError(f"schema_id must be >= 0, got {schema_id}")
    return struct.pack(">bI", MAGIC_BYTE, schema_id) + payload


def unframe(message: bytes) -> tuple[int, bytes]:
    """Split a framed message into (schema_id, payload).

    Raises on anything that is not Confluent-framed, rather than returning plausible
    nonsense - a silently mis-decoded payload is far harder to diagnose than a refusal.
    """
    if len(message) < WIRE_HEADER_BYTES:
        raise ValueError(f"message is {len(message)} bytes, too short to be framed")
    magic, schema_id = struct.unpack(">bI", message[:WIRE_HEADER_BYTES])
    if magic != MAGIC_BYTE:
        raise ValueError(
            f"expected magic byte {MAGIC_BYTE}, got {magic} - is this bare Avro?"
        )
    return schema_id, message[WIRE_HEADER_BYTES:]


class SchemaRegistry:
    """Just enough of the REST API to register a subject and resolve ids."""

    def __init__(self, url: str = DEFAULT_URL, timeout: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.url + path,
            data=data,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method=method,
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def set_compatibility(self, subject: str, level: str = "BACKWARD") -> str:
        """Pin compatibility on a subject.

        Call this BEFORE the first schema is registered. Setting it afterwards does not
        retroactively validate version 1, so a schema registered first is never checked
        against anything.
        """
        return self._request("PUT", f"/config/{subject}", {"compatibility": level})[
            "compatibility"
        ]

    def compatibility(self, subject: str) -> str:
        try:
            return self._request("GET", f"/config/{subject}")["compatibilityLevel"]
        except urllib.error.HTTPError:
            return self._request("GET", "/config")["compatibilityLevel"]

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        """Register a schema and return its id. Idempotent: re-registering returns the same id."""
        return int(
            self._request("POST", f"/subjects/{subject}/versions", {"schema": json.dumps(schema)})[
                "id"
            ]
        )

    def schema_by_id(self, schema_id: int) -> dict[str, Any]:
        return json.loads(self._request("GET", f"/schemas/ids/{schema_id}")["schema"])

    def latest(self, subject: str) -> dict[str, Any]:
        return json.loads(self._request("GET", f"/subjects/{subject}/versions/latest")["schema"])

    def versions(self, subject: str) -> list[int]:
        return list(self._request("GET", f"/subjects/{subject}/versions"))
