# orchestration/

Airflow 3 DAGs. Runs in the `orchestration` Compose profile, backed by `postgres-meta`.

| DAG | Purpose | Status |
| --- | --- | --- |
| `streaming_health` | Sensor on stream freshness and consumer lag; gates the batch path. | Phase 5 |
| `silver_to_gold` | GE checkpoint → Silver batch → `dbt build` → GE on Gold. | Phase 5 |
| `api_extracts` | Daily Open-Meteo and Nager.Date pulls, dynamic task mapping over cities. | Phase 5 |
| `backfill` | Genuinely idempotent — re-running a past date produces bit-identical output. | Phase 5 |

SLA misses and `on_failure_callback` route to a local webhook receiver. No hosted alerting service.

**The correctness bar:** delete a day of Gold, trigger the backfill, get bit-identical results. That
property is what separates a real pipeline from a demo.
