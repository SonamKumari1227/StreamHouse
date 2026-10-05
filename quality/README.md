# quality/

The gate on the Bronze → Silver boundary.

| Path | Purpose | Status |
| --- | --- | --- |
| `expectations.py` | Suites — one per Silver table, plus a generic SCD2 suite. | **Done 2026-10-06** |
| `run_gate.py` | Applies a suite, quarantines what fails, exits non-zero if anything did. | **Done 2026-10-06** |

```bash
make quality-gate QUALITY_TABLE=fact_order_state
make quality-gate QUALITY_TABLE=dim_menu_item_scd2
make quality-gate QUALITY_TABLE=gps_trips_sessionized
```

**This is not Great Expectations** — see [ADR-0010](../docs/decisions/0010-quality-gate-without-great-expectations.md)
for why, and for what it would take to swap it in. The shape is GE's deliberately: a suite is
a tuple of named expectations, validation returns `passed` and `failed`, failures carry a
`violations` array. Replacing the engine changes `validate()` and leaves the suites alone.

## How it behaves

**Failures quarantine, they do not crash.** A job that dies on one bad row stops the pipeline
for everyone; one that drops bad rows silently lies about its own completeness. Failing rows
go to `s3a://quarantine/silver_<table>` with the names of every expectation they broke, and
the exit status is 1 — which is the hook Phase 5 orchestration reads and the number Phase 6
alerts on.

**A row carries every rule it broke**, not just the first. One row violating four expectations
is a different problem from one violating a single rule.

**A null predicate counts as a violation.** SQL's three-valued logic would otherwise let a row
with a null in the wrong place pass a check it never satisfied — which is exactly the row the
gate exists to catch. `Expectation.violated()` coalesces to false before negating, and a test
asserts it directly.

## What it caught on first contact

Run against the real `fact_order_state`, 2134 rows, it quarantined 2 — both orders whose
status was set by hand during earlier testing, bypassing the generator's state machine and
leaving `rider_id` null on a DELIVERED order. The rule was right and the rows really were
invalid.

dbt tests (Phase 4) run on modelled data and catch business-rule violations; this layer runs
on conformed data and catches structural ones. Two layers, because they detect different
classes of problem — see ADR-0006.
