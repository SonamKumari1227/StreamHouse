# ADR-0001: Log-based CDC over query-based CDC

- **Status:** Accepted
- **Date:** 2026-09-24
- **Phase:** 0

## Context

StreamHouse ingests changes from a PostgreSQL OLTP database whose central entity, `orders`, is
mutated many times over its lifetime as it moves through
`PLACED → ACCEPTED → PICKED_UP → DELIVERED | CANCELLED`. Dimension rows — menu prices, restaurant
open/closed status, rider tier — also change in place, and those changes must be preserved as SCD
Type 2 history downstream.

The requirement is therefore not "get the current state periodically" but "observe every state
transition, in order, including deletions." How changes leave Postgres determines whether that is
achievable at all, so this decision is made before any ingestion code exists.

## Options considered

| Option | Pros | Cons |
| --- | --- | --- |
| **A — Log-based CDC via Debezium** (chosen) | Reads the write-ahead log, so it sees every committed change including `DELETE`s and intermediate states. No load on the source from polling. Emits before and after images. Ordered by LSN, which gives a natural dedup and merge key. | Requires `wal_level=logical` and a restart. An inactive replication slot retains WAL forever and can fill the source disk. More moving parts: Kafka Connect plus a connector to operate. |
| B — Query-based CDC (`WHERE updated_at > :watermark`) | Trivial to implement. No source configuration. No extra infrastructure. | **Cannot see deletes at all** — a deleted row simply stops appearing. **Misses intra-interval updates**: two status changes between polls collapse into one, destroying the state transitions we exist to capture. Depends on the application maintaining `updated_at` correctly. Adds repeated scan load to the OLTP database. |
| C — Application-level event publishing (dual write) | Events carry business meaning rather than row diffs. | Dual-write consistency problem: the database commit and the publish can diverge. Requires changing the source application, which is out of scope here. Silently loses any change made outside the application path. |

## Decision

Use **Debezium reading the PostgreSQL WAL** via logical replication, publishing to Redpanda.

Two properties decided it, and neither is available from option B at any level of effort. First,
**deletes are invisible to query-based CDC** — a row that vanishes produces no row to select, so a
watermark query cannot distinguish a delete from a row that simply did not change. Second,
**query-based CDC samples state rather than observing transitions**: if an order moves `ACCEPTED →
PICKED_UP → DELIVERED` between two polls, only `DELIVERED` is ever seen. The intermediate
transitions are precisely the data this project is built to model.

`orders` is additionally set to `REPLICA IDENTITY FULL` so that `UPDATE` events carry a complete
before image, making before/after deltas computable rather than inferred.

## Consequences

**Easier:** Deletes, intermediate states, and out-of-order arrivals are all observable. The LSN gives
a monotonic ordering key, which becomes the dedup key `(pk, lsn)` in Silver and the idempotency
boundary for the `MERGE`. Source database load is near zero — Debezium tails the log rather than
scanning tables.

**Harder:** Postgres must run with `wal_level=logical`, `max_replication_slots` and
`max_wal_senders` raised, and be restarted for those to take effect. Kafka Connect becomes a
component that must be monitored and has its own failure modes. `REPLICA IDENTITY FULL` increases
WAL volume on every `orders` update — a real cost, accepted deliberately for the before images.

**Commits us to:** operating a replication slot, and to the discipline around it. An inactive slot
retains WAL indefinitely and will fill the source disk; `pg_replication_slots.active` must be
monitored from the moment the connector is registered, and the slot must never be dropped without a
planned re-snapshot, because the retained WAL is gone the instant it is. This becomes a mandatory
entry in the Phase 6 alert runbook.

## Notes

Revisit if the source ever becomes a system where logical replication is unavailable. In that case
the honest fallback is option C with an outbox table, not option B — an outbox at least preserves
transitions and is transactional with the write, whereas polling structurally cannot recover either
property.
