# StreamHouse

**A real-time CDC lakehouse with enforced data contracts, automated quality gates, and column-level lineage.**

StreamHouse captures change-data-capture events from a PostgreSQL OLTP database via Debezium, streams them
through Redpanda into a Delta Lake medallion architecture, and serves a dbt-modeled star schema — with Avro
contracts enforced at the schema registry, a native expectation suite and dbt tests gating every layer
boundary, and Airflow 3 orchestrating the lot behind a backfill that proves itself bit-identical.
Observability (OpenLineage into Marquez, Prometheus and Grafana) is designed and scheduled for Phase 6.

The domain is a quick-commerce delivery marketplace: orders mutate through a real state machine
(`PLACED → ACCEPTED → PICKED_UP → DELIVERED | CANCELLED`), riders emit GPS pings at ~200 msg/s, and menu
prices change over time. That combination forces every hard streaming problem — out-of-order events,
SCD Type 2 over CDC, watermarking, exactly-once sinks — to appear naturally rather than as a contrived demo.

Everything runs locally on Docker. There is no cloud account, subscription, or credential anywhere in
the project — see [Runs entirely on your machine](#runs-entirely-on-your-machine).

> **Project status: Phases 0–5 complete, last verified 2026-10-06.** The pipeline runs end to end — a
> row mutated in Postgres is captured from the WAL by Debezium, lands in Bronze Delta in seconds, flows
> through SCD2 Silver into a dbt star schema, and is scheduled by Airflow 3 with a backfill that proves
> itself bit-identical. Phase 6 (observability — OpenLineage, Prometheus, Grafana) is next and has not
> started. Phase-by-phase state and the evidence behind each tick is in [Build status](#build-status).

---

## Architecture

```
+------------------------------ SOURCES ---------------------------------+
|                                                                        |
|  (A) PostgreSQL OLTP        (B) Rider GPS         (C) REST APIs        |
|      orders, order_items        producer              Open-Meteo       |
|      customers, riders          (Python)              Nager.Date       |
|      restaurants, menu_items    ~200 msg/s            (daily pull)     |
|      payments                                                          |
|      ^ synthetic load generator                                        |
|      | (Faker + order state machine + --chaos)                         |
+------|--------------------------|--------------------|----------------+
       | WAL (logical replication)|                    |
       v                          v                    v
+--------------+          +----------------+   +--------------------+
|   Debezium   |--------->|                |   | Python extractor   |
|  connector   |          |    REDPANDA    |<--| (idempotent,       |
| (Kafka Conn) |          |  (Kafka API)   |   |  watermarked)      |
+--------------+          |                |   +--------------------+
                          |  topics:       |
  +-----------------------|  cdc.*         |
  |  SCHEMA REGISTRY      |  gps.pings     |
  |  (Avro contracts,     |  dlq.*         |
  |   BACKWARD compat)    |                |
  +-----------------------+-------+--------+
                                  |
                 =================v==================================
                  SPARK STRUCTURED STREAMING   ->  BRONZE
                  - exactly-once: checkpoints + idempotent Delta sink
                  - schema enforced from Registry; violations -> DLQ
                  - append-only; full payload + Kafka offset metadata
                 =================+==================================
                                  v
     +===============================================================+
     ||            DELTA LAKE  on  MinIO  (s3a://)                  ||
     ||                                                             ||
     ||  BRONZE   raw_orders_cdc, raw_gps, raw_weather              ||
     ||     |     immutable, partitioned by ingest_date             ||
     ||     v     --- Great Expectations gate ---> quarantine/      ||
     ||  SILVER   dim_restaurant_scd2, dim_rider_scd2,              ||
     ||     |     fact_order_state, gps_trips_sessionized           ||
     ||     |     MERGE INTO, dedup on (pk, lsn), conformed types   ||
     ||     v     --- dbt tests ---> failure blocks downstream      ||
     ||  GOLD     dim_date, dim_customer, dim_restaurant, dim_rider ||
     ||           fact_delivery, fact_order_item,                   ||
     ||           agg_sla_daily, agg_rider_utilisation              ||
     +===============================================================+
                                  |
       +--------------------------+---------------------------+
       v                          v                           v
+--------------+          +----------------+        +------------------+
|  Trino /     |          |    FastAPI     |        |    Streamlit     |
|  DuckDB      |          |  metrics API   |        |    dashboard     |
|  ad-hoc SQL  |          |  (+ API tests) |        |                  |
+--------------+          +----------------+        +------------------+

+--- CROSS-CUTTING ------------------------------------------------------+
| ORCHESTRATION  Airflow 3 - streaming health sensors, batch DAGs,       |
|                backfills, SLA misses, dynamic task mapping             |
| LINEAGE        OpenLineage events from Spark + dbt + Airflow           |
|                -> Marquez UI (column-level where supported)            |
| METRICS        Prometheus <- Spark / Kafka / custom exporters          |
|                -> Grafana: freshness, consumer lag, DQ %, row deltas   |
| ALERTING       Alertmanager -> local webhook receiver                  |
| CI/CD          GitHub Actions: ruff + mypy, pytest + chispa,           |
|                testcontainers integration test, dbt build              |
| IaC            Terraform - validate-only portability exercise (Phase 7) |
+------------------------------------------------------------------------+
```

<!-- PHASE 2 DELIVERABLE: replace this comment with the demo GIF at docs/img/cdc-demo.gif —
     UPDATE in Postgres on the left, the row landing in Bronze Delta on the right,
     within seconds. It belongs above the fold. -->

---

## Demo

| | | |
| --- | --- | --- |
| **CDC end-to-end** | `UPDATE orders SET status='DELIVERED'` in Postgres → Bronze Delta in seconds | ✅ 8384 Kafka messages reconciled to 8383 Bronze rows, 0 duplicates, p50 19s |
| **Silver SCD2** | Three dimensions with non-overlapping validity windows; GPS sessionized on event time | ✅ 54 transform tests, incl. the MERGE-replay and overlapping-window guards |
| **Gold star schema** | dbt-spark: 6 staging views, 9 marts, generic + singular tests | ✅ `dbt build` PASS=74 WARN=0 ERROR=0; point-in-time joins matched 2133/2133 |
| **Idempotent backfill** | Destroy a day of Gold, rebuild it, compare a content checksum | ✅ Asserted by the `backfill` DAG, not by hand — hash 153839121457 both sides |
| **Airflow** | 4 DAGs: `streaming_health`, `silver_to_gold`, `api_extracts`, `backfill` | ✅ All four run to completion, 0 import errors |
| **SLO dashboard** | Grafana: freshness, volume delta vs 7-day baseline, DQ pass rate, consumer lag | ⬜ Phase 6 |
| **Lineage graph** | Marquez, column-level where the integration supports it | ⬜ Phase 6 |
| **dbt docs** | `make dbt-docs` generates the catalog locally | ✅ Generated; not yet published (no CI — see Engineering practices) |

---

## Run it in 3 commands

Requires Docker 24+, Docker Compose v2, and ~8 GB of RAM allocated to Docker. No Python needed for
this part — every service runs in a container.

```bash
git clone https://github.com/SonamKumari1227/StreamHouse.git && cd StreamHouse
cp .env.example .env
make up && make db-init
```

Then verify the stack, including the one condition the whole project depends on:

```bash
make health        # every service, plus: wal_level must print `logical`
```

`make down` stops everything and keeps your data; `make clean` also drops the volumes.

> **What works today:** Phases 0–5. `make up && make db-init` brings up the core stack; from there
> `make connector-register` starts CDC, `make stream-bronze` and `make stream-gps` fill Bronze,
> `make silver-orders` / `silver-dims` / `silver-trips` build Silver behind `make quality-gate`,
> `make dbt-build` builds Gold, and `make airflow-up` runs all of it on a schedule. Observability
> (Phase 6) is not built yet. `make help` always lists what actually exists.

<details>
<summary><b>Step-by-step</b> — what <code>make up</code> does, and running Compose directly</summary>

Every Make target is a thin wrapper, so you can drive Compose yourself:

```bash
docker compose -f infra/docker-compose.yml --profile core up -d
docker compose -f infra/docker-compose.yml ps        # all services should report "healthy"
docker compose -f infra/docker-compose.yml logs -f postgres
```

The **core** profile is postgres, redpanda, redpanda-console, connect, minio, spark-master and
spark-worker — roughly 6 GB. The `orchestration` and `observability` profiles are not used until
Phases 5 and 6.

Kafka Connect and Spark come up **idle**. No connector is registered and no job is submitted until
Phase 2; they run so the stack is complete and `make health` means something.

The Python virtualenv first matters in Phase 1, when the data generator runs on the host:

```bash
python3.11 -m venv .venv            # 3.11 explicitly — PySpark 3.5.x does not support 3.13+
source .venv/bin/activate           # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**PySpark and Delta are deliberately absent from the venv.** Everything under `ingestion/` and
`transform/silver/` runs inside the Spark image via `spark-submit` (ADR-0009), so the tested runtime is
the shipped one. That splits the suite in two, and both halves have to pass: `make test` runs 275 tests
on the host in about a second, `make test-spark` runs the transform tests inside the image.

**Windows note:** keep the repo *and* all Docker volumes inside the WSL2 filesystem. Docker Desktop
file I/O across the Windows/WSL boundary is roughly an order of magnitude slower, and it will make
Spark look broken when it isn't.

Full operational detail — per-service verification, common failures, teardown — is in
[docs/runbook.md](docs/runbook.md).

</details>

### Service endpoints

| UI | URL | Login |
| --- | --- | --- |
| Redpanda Console | http://localhost:8080 | none |
| MinIO Console | http://localhost:9001 | from `.env` |
| Spark Master | http://localhost:8090 | none |
| Airflow | http://localhost:8082 | `admin` / `admin` |
| Marquez | http://localhost:3000 | none |
| Grafana | http://localhost:3001 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | none |

The full stack is ~10.5 GB at full tilt, so it is split across Compose profiles — `core` (~6 GB),
`orchestration`, and `observability`. You never need all of it at once.

---

## Data model

### Source OLTP — the CDC source

```sql
customers     (customer_id PK, name, phone, city, signup_ts, tier, is_active, updated_at)
restaurants   (restaurant_id PK, name, city, lat, lon, cuisine, rating,
               commission_pct, is_open, updated_at)          -- SCD2 target
menu_items    (menu_item_id PK, restaurant_id FK, name, price_inr,
               is_available, updated_at)                     -- SCD2 target (price changes)
riders        (rider_id PK, name, city, vehicle_type, tier,
               shift_start, is_online, updated_at)           -- SCD2 target
orders        (order_id PK, customer_id FK, restaurant_id FK, rider_id FK NULL,
               status, placed_ts, accepted_ts, picked_up_ts, delivered_ts,
               promised_ts, subtotal_inr, delivery_fee_inr, discount_inr,
               total_inr, payment_id FK, updated_at)         -- heavily mutated
order_items   (order_item_id PK, order_id FK, menu_item_id FK,
               qty, unit_price_inr, updated_at)
payments      (payment_id PK, order_id FK, method, status,
               amount_inr, gateway_ref, updated_at)
```

`orders` runs with `REPLICA IDENTITY FULL` so Debezium emits full *before* images on `UPDATE`, which is what
makes before/after deltas computable. It costs extra WAL volume — a deliberate trade, recorded in
[ADR-0001](docs/decisions/0001-why-debezium.md).

### Gold star schema

```
                        dim_date
                           |
   dim_customer -------+   |   +------- dim_restaurant (SCD2, surrogate key)
                       v   v   v
                    fact_delivery  <------- dim_rider (SCD2)
                    ---------------
                    order_sk (PK)
                    date_sk, customer_sk, restaurant_sk, rider_sk, weather_sk
                    placed_ts, delivered_ts, promised_ts
                    prep_minutes, transit_minutes, total_minutes
                    sla_breach_flag, sla_breach_minutes
                    distance_km
                    gross_revenue_inr, discount_inr, commission_inr,
                    rider_payout_inr, contribution_margin_inr
                    cancelled_flag, cancel_reason
                           |
                           v
                    fact_order_item   (grain: one row per item per order)
```

**Grain statements**

- `fact_delivery` — one row per **order**, at its terminal state.
- `fact_order_item` — one row per **(order, menu_item)**.
- `agg_sla_daily` — one row per **(date, city, restaurant)**.

Aggregates: SLA breach rate by city / restaurant / hour-of-day, unit economics split across prep vs transit
cost, rider utilisation and idle-gap distribution, weather impact on median transit time, and customer
cohort retention by signup month.

---

## Data sources

| # | Source | Type | Access | Volume |
| --- | --- | --- | --- | --- |
| A | Synthetic OLTP generator (Faker + order state machine) | Mutating relational | Local Postgres | ~50k orders/day, ~200k updates/day |
| B | Rider GPS ping producer | High-volume append stream | Local Kafka producer | ~200 msg/s (~5M/day) |
| C | [Open-Meteo](https://open-meteo.com) forecast + archive | Real REST API | Free, no key | ~5k rows/day |
| D | [Nager.Date](https://date.nager.at) public holidays | Real REST API | Free, no key | ~30 rows/year |
| E | India city/geo reference (static GeoJSON) | Static reference | Free download | ~100 rows |

### On synthetic data

The OLTP generator is synthetic **on purpose**. No public dataset lets you inject a specific failure mode on
demand and then assert that the pipeline survived it. The `--chaos` flag injects seven scenarios, each with
a matching test in [`tests/chaos/`](tests/chaos/):

1. **Late events** — a `DELIVERED` update arriving 45 minutes after the watermark
2. **Out-of-order timestamps** — `picked_up_ts` before `accepted_ts`
3. **Duplicate CDC records** — Debezium redelivery after a connector restart
4. **Schema evolution** — a new nullable column, then a breaking type change the contract must reject
5. **NULL floods** — 30% of `rider_id` suddenly NULL
6. **Referential integrity breaks** — an `order_items` row pointing at a deleted `menu_item`
7. **Traffic burst** — 3× volume for 10 minutes, to exercise backpressure

Sources C, D and E are real public APIs with real rate limits and real outages. Real APIs plus a
controllable chaos generator exercise far more failure surface than any static public dataset would.

---

## Key engineering decisions

Each links to a full ADR — context, options, decision, consequences.

Six are written up in full — context, options, decision, consequences. The rest are recorded here only;
their ADRs are not written yet and this table says so rather than linking to a file that does not exist.

| Decision | Short rationale | ADR |
| --- | --- | --- |
| **Debezium** (log-based CDC) over query-based | Reads the Postgres WAL, so it captures `DELETE`s and never polls the source. `WHERE updated_at > x` silently misses deletes and any intra-interval update. | [ADR-0001](docs/decisions/0001-why-debezium.md) |
| **Health checks probe the work path, not the port** | Connect crash-looped 39 times while its REST layer kept answering on 8083. The herder was dead; the port was not. A TCP check on `spark-worker` has the same blind spot. | [ADR-0007](docs/decisions/0007-healthcheck-what-to-probe.md) |
| **Schemas captured from the producer, not hand-written** | Debezium's envelope shape is the contract, so `contracts/*.avsc` is derived from what the connector actually emits rather than from what the DDL suggests it should. | [ADR-0008](docs/decisions/0008-cdc-topics-and-schema-capture.md) |
| **Spark runtime pinned in an image** | `apache/spark:3.5.3` ships no Delta, no `hadoop-aws`, no `spark-sql-kafka`. Pinning all three in the image means the tested runtime is the shipped one. | [ADR-0009](docs/decisions/0009-spark-runtime-and-the-s3a-sdk.md) |
| **A native expectation suite over Great Expectations** | The gate has to run inside the Spark image on Python 3.8, and has to quarantine failing rows rather than crash the job. GE buys little there and costs a dependency tree. | [ADR-0010](docs/decisions/0010-quality-gate-without-great-expectations.md) |
| **Airflow drives Spark through the Docker socket** | Airflow has no Spark, no Delta jars, no S3A client and no dbt. Rather than duplicate that stack into the scheduler image, it starts containers that already have it. | [ADR-0011](docs/decisions/0011-airflow-drives-spark-through-the-docker-socket.md) |
| **Delta Lake** as table format | Mature `MERGE INTO`, time travel, and `OPTIMIZE`/`ZORDER` — all of which the Silver layer depends on. | *not written* |
| **Airflow 3** as orchestrator | Mature backfills and dynamic task mapping. Dagster's asset model is conceptually cleaner; a port of one pipeline would show the contrast. | *not written* |
| **SCD Type 2** over snapshot-per-day | Preserves exact validity windows for price and status history at a fraction of the storage. | *not written* |
| **Redpanda** over Kafka | Kafka API-compatible, single binary, no ZooKeeper/KRaft tuning, ~1 GB RAM vs 4 GB+. Ships a built-in schema registry, so no separate container. | *not written* |
| **MinIO** as object store | Self-hosted and S3A-compatible, so the storage layer is swappable by configuration alone. Phase 7 is where that gets proven. | — |
| **dbt-spark** for Gold | Version-controlled SQL with tests, docs and lineage included. Hand-written PySpark for Gold gives none of those. | — |

---

## Engineering practices

**Testing pyramid**

383 test functions across 20 files, split by where they can run:

| Suite | Count | Where it runs |
| --- | --- | --- |
| **Unit** — state machine, generator, config, chaos, CLI | 198 | host venv, no containers, ~1s |
| **Spark** — Bronze transforms, SCD2, sessionization, expectations, orchestration checks | 89 | inside the Spark image, `make test-spark` |
| **Integration** — real Postgres and Redpanda round trips | 55 | host, needs `make up` |
| **Chaos** — schema evolution against the live registry | 7 | host, needs `make up` |

`make test` runs the host half: **275 passed, 61 deselected in 1.06s** (verified 2026-10-09). The spark
and integration markers are deselected there because the host venv has no PySpark by design —
`tests/spark/conftest.py` sets `collect_ignore_glob` so collection does not even import it.

Quality gates are run before committing, not after: `ruff check`, `ruff format --check`, and `mypy
--strict`, all three green across 29 source files as of the Phase 2 repair.

**CI — designed, not built.** There is no `.github/` in this repo yet. The intended pipeline is:

```
lint        -> ruff check + ruff format --check + mypy
unit        -> pytest tests/unit --cov --cov-fail-under=80
contracts   -> Avro BACKWARD compatibility check against master  # fails the PR on a breaking change
integration -> pytest tests/integration
dbt         -> dbt deps && dbt build --target ci (on a seeded sample)
docs        -> dbt docs generate && deploy to GitHub Pages
```

The `contracts` job is the one that matters, and it is the reason this is listed as pending rather than
quietly dropped: a pull request that breaks an Avro schema for downstream consumers should fail before
it can merge. Until that job exists, the registry check in `tests/chaos/test_schema_evolution.py` proves
the gate works but nothing enforces it on a merge.

**Operations** — [`docs/runbook.md`](docs/runbook.md) carries one entry per configured alert: symptom,
likely causes in order of frequency, the first three commands to run, and the recovery procedure — including
which steps are safe to repeat. Restarting the Spark streaming job always is, because checkpoints guarantee
exactly-once. Dropping a replication slot never is, because the WAL is gone the moment you do.

---

## Repository layout

```
streamhouse/
├── docs/              architecture, ADRs, data contracts, runbook
├── infra/             docker-compose, connector configs, prometheus/grafana, terraform
├── contracts/         Avro schemas — the source of truth for shape
├── generator/         synthetic OLTP + GPS producers, chaos injection
├── ingestion/         Spark Structured Streaming jobs, API extractors
├── transform/
│   ├── silver/        PySpark: SCD2 MERGE, dedup, sessionization
│   └── gold_dbt/      dbt project: staging, marts, tests, macros, seeds
├── quality/           native expectation suite, Bronze→Silver gate, freshness and backfill checks
├── orchestration/     Airflow DAGs
├── serving/           FastAPI metrics service, Streamlit dashboard
└── tests/             unit, integration, chaos
```

---

## Build status

Built in eight phases, each ending in something demoable and committed.

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Foundation — repo skeleton, `core` Compose profile, Postgres DDL + logical replication, ADR-0001, runbook | ✅ Done |
| 1 | Source simulation — OLTP generator, order state machine, GPS producer, `--chaos` | ✅ Done |
| 2 | CDC ingestion + contracts — Debezium, Avro registry, Bronze streaming, DLQ, dedup on `(table, pk, lsn)` | ✅ Done |
| 3 | Silver — SCD2 `MERGE`, dedup on `(pk, lsn)`, GPS sessionization, quality gate, `OPTIMIZE`/`ZORDER` | ✅ Done |
| 4 | Gold — dbt star schema, `dim_date` from holidays, weather join, generic + singular tests, docs | ✅ Done |
| 5 | Orchestration — Airflow 3 DAGs, dynamic task mapping, idempotent backfills | ✅ Done |
| 6 | Observability — OpenLineage → Marquez, Grafana SLO dashboard, alert rules, runbook | 🔨 Next |
| 7 | Portability & IaC (validate-only, no cloud spend) — Terraform as a documented design exercise, storage-layer swap proven locally, ADR on the mapping | ⬜ Optional |
| 8 | Stretch — Iceberg comparison, Dagster port, streaming Gold, contract CI gate, cost model | ⬜ Optional |

**What each tick above rests on.** Phases are marked done only against output from a running stack, not
against code that looks finished:

```
Phase 2   8384 Kafka messages -> 8383 Bronze rows, 0 duplicate (pk, lsn), p50 latency 19s
          registry refuses an incompatible schema          (runbook 4.9, 4.10)
Phase 3   non-overlapping SCD2 validity windows asserted; 8387 rows, 8387 distinct (pk, lsn)
Phase 4   dbt build  PASS=74 WARN=0 ERROR=0 SKIP=0 TOTAL=74
          fact_delivery 2133 rows; date/customer/restaurant/weather joins 2133/2133 (100%)
          rider_sk 1898/1898 of the orders that have a rider; 0 fan-out
Phase 5   backfill  6/6 tasks   checksum 153839121457 before and after a destructive rebuild
          silver_to_gold 14/14  api_extracts 10/10  streaming_health 3/3
```

**Still outstanding for the project as a whole:** CI does not exist yet (see Engineering practices), dbt
docs are generated locally rather than published, and there is no `LICENSE` file despite the section
below. A stranger can clone this and reach a working pipeline from this README alone — that part holds.

---

## Runs entirely on your machine

No cloud account, no subscription, no credentials, no spend. Every component above runs in Docker
locally, and there is no managed service anywhere in the pipeline.

The only external calls in the whole project are to two free public APIs — [Open-Meteo](https://open-meteo.com)
for weather and [Nager.Date](https://date.nager.at) for public holidays. Neither needs an account or a
key, and neither is used before Phase 4.

That constraint is deliberate rather than a limitation. It means the repo is reproducible by anyone
who clones it, it cannot rot when a free tier changes, and it never bills you for a portfolio project.
Portability to managed infrastructure is proven in Phase 7 as a **design exercise** — Terraform that is
validated but never applied, plus a storage-layer swap performed locally by configuration alone.

---

## What I'd do differently at 100× scale

At ~5M events/day the single-node local stack is honest about its limits. At 500M/day these are the parts
that would have to change:

- **Bronze layout.** `ingest_date` partitioning produces too many small files under streaming micro-batches.
  Hour-level partitions with auto-compaction and a tuned `maxFilesPerTrigger`, with `OPTIMIZE` as a
  scheduled and monitored job rather than an afterthought.
- **SCD2 merge cost.** A full-dimension `MERGE` per micro-batch stops scaling once the dimension exceeds
  memory. Narrow the merge to the changed key range with a pushdown predicate, and Z-order on the merge key.
- **Skew.** A handful of restaurants would carry a disproportionate share of orders. AQE absorbs moderate
  skew; past that, salting the hot keys explicitly is the fix.
- **One Spark job per stream.** Fine at this size, wasteful at scale — consolidate CDC and GPS into a shared
  streaming application with separate sinks, or move GPS to a dedicated stateful runtime where the state
  store, not the shuffle, is the bottleneck.
- **Replication slot risk.** A single Debezium slot is a single point of WAL-retention failure. Shard
  connectors per table group, alert on slot lag in bytes rather than time, and rehearse the re-snapshot.
- **Quality gate placement.** Great Expectations running synchronously in the write path adds latency that
  is affordable at 5M/day and not at 500M. Sample for inline checks, and run the full suite asynchronously
  against the committed Delta version.

---

## License

MIT. **The `LICENSE` file is not in the repo yet** — add one before treating this as licensed.
