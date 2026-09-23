# tests/chaos/

One test per failure scenario injected by the generator `--chaos` flag. Each proves the pipeline
survives a specific, named failure, rather than proving it works on the happy path.

| # | Scenario | Asserts | Status |
| --- | --- | --- | --- |
| 1 | Late events — `DELIVERED` arriving 45 min after the watermark | Late-arrival reconciliation catches it; no silent loss | Phase 3 |
| 2 | Out-of-order timestamps — `picked_up_ts` before `accepted_ts` | Flagged as invalid and quarantined, not silently modelled | Phase 3 |
| 3 | Duplicate CDC records — Debezium redelivery after restart | Dedup on `(pk, lsn)` holds; row counts unchanged | Phase 2 |
| 4 | Schema evolution — new nullable column, then a breaking type change | Additive change passes; breaking change is rejected by the registry | Phase 2 |
| 5 | NULL floods — 30% of `rider_id` suddenly NULL | GE null-rate expectation fires; rows quarantine | Phase 3 |
| 6 | Referential integrity break — `order_items` pointing at a deleted `menu_item` | dbt `relationships` test fails the build | Phase 4 |
| 7 | Traffic burst — 3× volume for 10 minutes | Backpressure holds; no OOM; lag recovers | Phase 3 |

Scenario definitions live in `docs/architecture.md` §5.1. Injection lives in `generator/chaos.py`.
