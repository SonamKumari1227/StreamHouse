# contracts/

Avro schemas — the source of truth for the shape of every topic. Producers and consumers agree here,
not in code.

Registered with the Redpanda built-in schema registry (`localhost:8081`) under `BACKWARD`
compatibility, so a new schema must remain readable by consumers built against the previous one.

| File | Purpose | Status |
| --- | --- | --- |
| `orders.v1.avsc` | CDC envelope for the `orders` table. | Phase 2 |
| `orders.v2.avsc` | A deliberately breaking change, used to prove the registry rejects it. | Phase 2 |
| `gps_ping.v1.avsc` | Rider GPS ping payload. | Phase 1 |

A CI job checks every changed `.avsc` for backward compatibility against `main` and fails the pull
request on a breaking change. That gate is the point of this directory.
