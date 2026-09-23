# generator/

Synthetic source data. The OLTP generator is synthetic on purpose: no public dataset lets you inject
a specific failure mode on demand and then assert that the pipeline survived it.

| File | Purpose | Status |
| --- | --- | --- |
| `oltp_generator.py` | Faker-seeded customers, restaurants, menus and riders, plus an order state machine advancing orders on a realistic timeline (prep 8–25 min, transit 5–40 min). Also mutates dimension rows, which is what later feeds SCD2. | Phase 1 |
| `gps_producer.py` | GPS pings along interpolated routes, Avro-serialised to Redpanda at ~200 msg/s. | Phase 1 |
| `chaos.py` | The seven failure scenarios behind the `--chaos` flag. See `docs/architecture.md` §5.1. | Phase 1 |

Writes to Postgres and Redpanda only. Never writes to Delta directly.
