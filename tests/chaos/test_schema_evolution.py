"""Chaos scenario 4 - schema evolution: additive changes pass, breaking ones are refused.

From docs/architecture.md 5.1: "a new nullable column, then a breaking type change that the
contract must reject".

These run against the live schema registry rather than a mock, because the thing under test
IS the registry's compatibility engine. A mock would only assert that we understand Avro
compatibility rules, which is not the claim being made.

The first test is the one that matters most in the long run: it fails if the committed
contract and the registered schema ever diverge, which is the drift a CI gate exists to
catch.

Run with:  pytest -m chaos
Needs the core stack up and the connector registered:  make up && make connector-register
"""

from __future__ import annotations

import json
import pathlib
import urllib.error
import urllib.request
from typing import Any

import pytest

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

REGISTRY = "http://localhost:8081"
SUBJECT = "cdc.public.orders-value"
CONTRACTS = pathlib.Path(__file__).parents[2] / "contracts"


def _get(path: str) -> Any:
    with urllib.request.urlopen(REGISTRY + path, timeout=10) as response:
        return json.load(response)


def _post(path: str, body: dict[str, Any]) -> tuple[int, Any]:
    request = urllib.request.Request(
        REGISTRY + path,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


@pytest.fixture(scope="module")
def registry() -> str:
    try:
        subjects = _get("/subjects")
    except Exception as exc:
        pytest.skip(f"schema registry unreachable ({exc}); run `make up`")
    if SUBJECT not in subjects:
        pytest.skip(f"{SUBJECT} is not registered; run `make connector-register`")
    return SUBJECT


def row_record(schema: dict[str, Any]) -> dict[str, Any]:
    """The row record, which Avro defines under `before` and only references under `after`."""
    before = next(f for f in schema["fields"] if f["name"] == "before")
    return next(t for t in before["type"] if isinstance(t, dict))


def check_compatible(schema: dict[str, Any]) -> tuple[int, Any]:
    return _post(
        f"/compatibility/subjects/{SUBJECT}/versions/latest", {"schema": json.dumps(schema)}
    )


# --------------------------------------------------------------------------- drift


def test_the_committed_contract_still_matches_what_is_registered(registry: str) -> None:
    """The CI gate in miniature: contracts/orders.v1.avsc must equal the live schema.

    If this fails, either the source table changed without the contract being re-captured, or
    the contract was edited by hand. Both are exactly what the gate exists to stop.
    """
    committed = json.loads((CONTRACTS / "orders.v1.avsc").read_text())
    registered = json.loads(_get(f"/subjects/{SUBJECT}/versions/1")["schema"])
    assert json.dumps(committed, sort_keys=True) == json.dumps(registered, sort_keys=True)


def test_backward_compatibility_is_pinned_on_the_subject(registry: str) -> None:
    """Set before version 1 was registered, so v1 is a contract rather than a description."""
    assert _get(f"/config/{SUBJECT}")["compatibilityLevel"] == "BACKWARD"


# --------------------------------------------------------------------------- additive


def test_an_optional_field_with_a_default_is_accepted(registry: str) -> None:
    """The additive half of scenario 4: adding a nullable column must not break consumers."""
    schema = json.loads((CONTRACTS / "orders.v1.avsc").read_text())
    row_record(schema)["fields"].append(
        {"name": "loyalty_tier", "type": ["null", "string"], "default": None}
    )
    status, body = check_compatible(schema)
    assert status == 200
    assert body["is_compatible"] is True


# --------------------------------------------------------------------------- breaking


def test_the_committed_breaking_schema_is_refused(registry: str) -> None:
    """contracts/orders.v2.avsc exists to be rejected: status changes string -> int."""
    breaking = json.loads((CONTRACTS / "orders.v2.avsc").read_text())
    status, body = check_compatible(breaking)
    assert status == 200, f"expected a compatibility verdict, got {status}: {body}"
    assert body["is_compatible"] is False


def test_registering_the_breaking_schema_returns_409(registry: str) -> None:
    """A verdict is advice; the gate has to actually refuse the write."""
    breaking = json.loads((CONTRACTS / "orders.v2.avsc").read_text())
    before = _get(f"/subjects/{SUBJECT}/versions")

    status, body = _post(f"/subjects/{SUBJECT}/versions", {"schema": json.dumps(breaking)})

    assert status == 409, f"expected 409 Conflict, got {status}: {body}"
    assert "incompatible" in json.dumps(body).lower()
    assert _get(f"/subjects/{SUBJECT}/versions") == before, "a rejected schema was registered"


def test_a_required_field_without_a_default_is_refused(registry: str) -> None:
    """Adding a column is only safe when the field is optional or carries a default."""
    schema = json.loads((CONTRACTS / "orders.v1.avsc").read_text())
    row_record(schema)["fields"].append({"name": "mandatory_thing", "type": "string"})
    _, body = check_compatible(schema)
    assert body["is_compatible"] is False


def test_removing_an_optional_field_is_accepted(registry: str) -> None:
    """Removing a field IS backward compatible - a reader simply ignores what it lacks.

    Worth an explicit test because the opposite was assumed during the scenario-4 run. When a
    column was dropped from `orders`, the connector failed with MISSING_UNION_BRANCH, and the
    obvious inference - "dropping a column breaks compatibility" - is wrong, as this proves.
    The real cause was a *stale registered version* still describing the dropped column; see
    runbook 4.10. Keeping this test stops that wrong inference being made again.
    """
    schema = json.loads((CONTRACTS / "orders.v1.avsc").read_text())
    record = row_record(schema)
    record["fields"] = [f for f in record["fields"] if f["name"] != "cancel_reason"]
    _, body = check_compatible(schema)
    assert body["is_compatible"] is True
