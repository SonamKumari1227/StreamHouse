# transform/silver/

Conformed, deduplicated, history-preserving tables. This is the hard layer.

| Model | Grain | Notes | Status |
| --- | --- | --- | --- |
| `fact_order_state` | one row per order, latest state | Dedup CDC on `(order_id, lsn)`. Rank by LSN within the micro-batch before merging, or two updates to the same key in one batch will corrupt the result. | **Done 2026-10-05** |
| `dim_restaurant_scd2` | one row per (restaurant, validity window) | `valid_from` / `valid_to` / `is_current` via Delta `MERGE`. | **Done 2026-10-06** |
| `dim_rider_scd2` | one row per (rider, validity window) | As above. | **Done 2026-10-06** |
| `dim_menu_item_scd2` | one row per (menu_item, validity window) | Captures price history. | **Done 2026-10-06** |
| `gps_trips_sessionized` | one row per trip | Watermarked on event time, keyed on (rider, trip). | **Done 2026-10-06** |

All three dimensions are one module, `dim_scd2.py`, parameterised by a `DimensionSpec`. They
differ only in key, columns and target, and three near-identical files would be three places
for the window logic to drift apart.

```bash
make silver-orders ONCE=1                 # fact_order_state
make silver-dim DIM=menu_items ONCE=1     # one dimension
make silver-dims                          # all three
make silver-trips ONCE=1                  # gps_trips_sessionized
make silver-maintain DRY_RUN=1            # file counts, change nothing
```

Each reads its Bronze table as a Delta stream, so a run resumes from its checkpoint rather
than rebuilding from the beginning.

The `quality/` gate runs on these tables and quarantines failing rows rather than crashing the
job. It is not Great Expectations — see ADR-0010.

**Two rules every model here follows**, learned building `fact_order_state`:

1. **Collapse to one row per key before the MERGE.** A micro-batch routinely carries several
   changes to the same entity, and Delta refuses a MERGE whose source matches a target row
   more than once. Bronze's `(pk, lsn)` dedup does not help - those are different LSNs and all
   of them are real. Rank by LSN and take the newest.
2. **Guard the update with `s.lsn > t.lsn`.** `foreachBatch` is at-least-once and batches are
   not ordered across a restart. Without the guard, replaying an older batch silently moves an
   entity backwards; with it, a replay is a no-op.

## Maintenance

`make silver-maintain` runs `OPTIMIZE` with `ZORDER BY` the column each table is queried on
(the business key for the dimensions, `order_id` for the fact, `trip_id` for trips), then
`VACUUM`.

The small-file problem is real and immediate, not theoretical: one pass of the pipeline left
**200 files in `fact_order_state` and 188 in `dim_menu_item_scd2`**, because every micro-batch
writes at least one file per partition.

**VACUUM retention is 168 hours (7 days), stated rather than defaulted.** It matches the Kafka
topic retention on purpose: inside that window both the raw events and the table's history are
available, so a bad batch can be diagnosed from either end and reprocessed. Promising longer
Delta history than the upstream topic can supply would be false comfort. `maintain.py` refuses
a shorter retention and will not disable Delta's safety check — the flag can only widen it.

**Correctness bar:** a test asserts that SCD2 validity windows never overlap for any key.
