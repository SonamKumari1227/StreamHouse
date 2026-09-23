# transform/silver/

Conformed, deduplicated, history-preserving tables. This is the hard layer.

| Model | Grain | Notes | Status |
| --- | --- | --- | --- |
| `fact_order_state` | one row per order, latest state | Dedup CDC on `(order_id, lsn)`. Rank by LSN within the micro-batch before merging, or two updates to the same key in one batch will corrupt the result. | Phase 3 |
| `dim_restaurant_scd2` | one row per (restaurant, validity window) | `valid_from` / `valid_to` / `is_current` via Delta `MERGE`. | Phase 3 |
| `dim_rider_scd2` | one row per (rider, validity window) | As above. | Phase 3 |
| `dim_menu_item_scd2` | one row per (menu_item, validity window) | Captures price history. | Phase 3 |
| `gps_trips_sessionized` | one row per trip | Watermarked sessionization with late-arrival handling. | Phase 3 |

A Great Expectations suite gates Bronze → Silver. Failures quarantine the offending rows rather than
crashing the job.

Maintenance: scheduled `OPTIMIZE` with `ZORDER BY (order_id)`, and a documented `VACUUM` retention
policy. The small-file problem from streaming micro-batches is not optional to solve.

**Correctness bar:** a test asserts that SCD2 validity windows never overlap for any key.
