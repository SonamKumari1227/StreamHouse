# ADR-0008: CDC topic naming, snapshot mode, and capturing the schema from the producer

- **Status:** Accepted
- **Date:** 2026-09-28
- **Phase:** 2

## Context

Debezium had to be pointed at seven tables and its output made durable and contract-checked.
Four choices had no obvious default, and one of them inverts how the `.avsc` files in
`contracts/` are meant to come into existence.

## Options considered

| Decision | Chosen | Rejected alternative |
| --- | --- | --- |
| Topic naming | `topic.prefix=cdc`, giving `cdc.public.<table>` | A bare table-name prefix. Keeping `cdc.` makes the change stream obviously distinct from `gps.pings` and from any future non-CDC topic, and keeping `public` leaves room for a second schema later. |
| Snapshot | `snapshot.mode=initial` | `never`. The reference tables hold 1270 rows, so the snapshot is trivial, and Bronze would otherwise have dimensions with no baseline - Phase 3's SCD2 needs a starting row per key. Spec ?12 warns that `initial` is a trap on large tables; at this size it is not. |
| Money | `decimal.handling.mode=precise` | `double`. Money in floating point is indefensible. `precise` yields Avro `bytes` with a decimal logical type, which arrived as `Decimal('485.00')` intact. |
| Schema authorship | **Capture** what Debezium registers into `contracts/orders.v1.avsc` | Hand-author the `.avsc` and force Debezium to match it. |

## Decision

The last one is the substantive one. The Debezium envelope is derived from the live table
definition - every column's type, nullability and default - across 17 columns. Hand-writing it
and setting `auto.register.schemas=false` means guessing Debezium's exact type mapping; a
mismatch is a connector that will not start.

So the producer registers the schema, and the registered schema is then exported verbatim
into `contracts/orders.v1.avsc` and committed. `BACKWARD` compatibility is pinned on the
subject **before** the connector runs, so the captured file is a contract from the first
version onward rather than a description written after the fact.

This is what makes the Phase 8 CI gate meaningful: a pull request that changes the table can
be diffed against a committed schema that provably matched reality.

## Consequences

**Easier:** The contract cannot drift from what the producer actually emits, because it *is*
what the producer emitted. No guessing at type mappings.

**Harder:** The contract is only as good as the moment it was captured. Re-capturing after a
schema change has to be a deliberate, reviewed step, not an automatic refresh - otherwise the
gate rubber-stamps whatever happened.

**Commits us to:** treating a captured `.avsc` as a reviewed artifact. `BACKWARD` must be set
on a subject *before* its first schema is registered, since setting it afterwards does not
retroactively validate version 1.

## Notes

`time.precision.mode=connect` was set, but note what it does **not** cover: `TIMESTAMPTZ`
columns map to `io.debezium.time.ZonedTimestamp`, which is an **ISO-8601 string**, not an Avro
timestamp logical type. Observed: `'2026-09-28T15:52:14.333969Z'`. Microsecond precision
survives, but Bronze will have to parse these rather than receiving a native timestamp. Worth
revisiting if Spark parsing proves costly at volume.
