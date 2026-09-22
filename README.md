# StreamHouse

**A real-time CDC lakehouse with enforced data contracts, automated quality gates, and column-level lineage.**

StreamHouse captures change-data-capture events from a PostgreSQL OLTP database via Debezium, streams them
through Redpanda into a Delta Lake medallion architecture, and serves a dbt-modeled star schema — with Avro
contracts enforced at the schema registry, Great Expectations and dbt tests gating every layer boundary,
OpenLineage lineage into Marquez, and freshness/SLO alerting through Prometheus and Grafana.

The domain is a quick-commerce delivery marketplace: orders mutate through a real state machine
(`PLACED → ACCEPTED → PICKED_UP → DELIVERED | CANCELLED`), riders emit GPS pings at ~200 msg/s, and menu
prices change over time. That combination forces every hard streaming problem — out-of-order events,
SCD Type 2 over CDC, watermarking, exactly-once sinks — to appear naturally rather than as a contrived demo.

Everything runs locally on Docker. A Terraform module deploys the equivalent Azure slice
(ADLS Gen2 + Event Hubs + Databricks + Unity Catalog) and tears it back down to zero.

> **Project status: Phase 0 — foundation in progress.** The architecture, data model, and build plan are
> settled and documented below. Phase-by-phase state is tracked in [Build status](#build-status).
> Screenshots and the demo GIF land as each phase completes.

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
| ALERTING       Alertmanager -> Slack webhook                           |
| CI/CD          GitHub Actions: ruff + mypy, pytest + chispa,           |
|                testcontainers integration test, dbt build              |
| IaC            Terraform - Azure slice                                 |
+------------------------------------------------------------------------+
```

<!-- PHASE 2 DELIVERABLE: replace this comment with the demo GIF at docs/img/cdc-demo.gif —
     UPDATE in Postgres on the left, the row landing in Bronze Delta on the right,
     within seconds. It belongs above the fold. -->

---

## Demo

| | |
| --- | --- |
| **CDC end-to-end** | `UPDATE orders SET status='DELIVERED'` in Postgres → visible in Bronze Delta in seconds *(GIF — Phase 2)* |
| **SLO dashboard** | Grafana: freshness, volume delta vs 7-day baseline, DQ pass rate, Kafka consumer lag *(Phase 6)* |
| **Lineage graph** | Marquez, column-level where the integration supports it *(Phase 6)* |
| **dbt docs** | Published to GitHub Pages *(Phase 4)* |

---

## Run it in 3 commands

Requires Docker 24+, Docker Compose v2, and ~8 GB of RAM allocated to Docker.

```bash
git clone https://github.com/SonamKumari1227/streamhouse.git && cd streamhouse
cp .env.example .env
make demo          # core stack up, Postgres seeded, contracts + Debezium registered,
                   # Bronze stream running
```

Then mutate a row and watch it arrive:

```bash
docker compose exec postgres psql -U streamhouse \
  -c "UPDATE orders SET status='DELIVERED' WHERE order_id=1;"
make query-bronze  # the CDC event appears within ~10s
```

`make down` stops everything; `make clean` also drops volumes.

<details>
<summary><b>Step-by-step setup</b> — what <code>make demo</code> does, and how to add the other stacks</summary>

```bash
# Python 3.11 venv — NOT 3.13. PySpark 3.5.x supports 3.8–3.11 only.
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1        # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env                # MinIO keys, Postgres password, optional SLACK_WEBHOOK_URL

docker compose --profile core up -d # ~6 GB: postgres, redpanda, connect, minio, spark
docker compose ps                   # every service should report "healthy"

make db-init                        # Postgres DDL + logical replication
docker compose exec postgres psql -U streamhouse -c "SHOW wal_level;"   # expect: logical

make minio-init                     # buckets: bronze/ silver/ gold/ quarantine/ checkpoints/
make contracts-register             # Avro schemas -> schema registry (BACKWARD compat)
make debezium-register
curl -s localhost:8083/connectors/streamhouse-pg/status | jq            # expect RUNNING

make generate                       # synthetic OLTP + GPS load; add CHAOS=1 to inject failures
make stream-bronze                  # Spark Structured Streaming: Kafka -> Bronze Delta

# Layer on the rest as you need them
docker compose --profile orchestration up -d   # Airflow
docker compose --profile observability up -d   # Marquez, Prometheus, Grafana
```

**Windows note:** keep the repo *and* all Docker volumes inside the WSL2 filesystem. Docker Desktop file
I/O across the Windows/WSL boundary is roughly an order of magnitude slower, and it will make Spark look
broken when it isn't.

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

| Decision | Short rationale | ADR |
| --- | --- | --- |
| **Debezium** (log-based CDC) over query-based | Reads the Postgres WAL, so it captures `DELETE`s and never polls the source. `WHERE updated_at > x` silently misses deletes and any intra-interval update. | [ADR-0001](docs/decisions/0001-why-debezium.md) |
| **Delta Lake** as table format | Maps directly onto Databricks / Unity Catalog. An Iceberg branch exists to compare the two on measured numbers rather than blog claims. | [ADR-0002](docs/decisions/0002-delta-vs-iceberg.md) |
| **Airflow 3** as orchestrator | Dominates enterprise Azure job postings. Dagster's asset model is conceptually cleaner; a port of one pipeline shows the contrast. | [ADR-0003](docs/decisions/0003-airflow-vs-dagster.md) |
| **SCD Type 2** over snapshot-per-day | Preserves exact validity windows for price and status history at a fraction of the storage. | [ADR-0004](docs/decisions/0004-scd2-vs-snapshots.md) |
| **Redpanda** over Kafka | Kafka API-compatible, single binary, no ZooKeeper/KRaft tuning, ~1 GB RAM vs 4 GB+. Swaps to Azure Event Hubs by config alone. | [ADR-0005](docs/decisions/0005-redpanda-vs-kafka.md) |
| **Quality enforced twice** — GE at ingestion, dbt at transform | GE catches structural and statistical drift on raw data; dbt tests catch business-rule violations where the business logic already lives. | [ADR-0006](docs/decisions/0006-where-quality-lives.md) |
| **MinIO** as object store | S3A-compatible, so identical Spark code runs against ADLS Gen2 with only a credential swap. That portability is the point. | — |
| **dbt-spark** for Gold | Version-controlled SQL with tests, docs and lineage included. Hand-written PySpark for Gold gives none of those. | — |

---

## Engineering practices

**Testing pyramid**

- **Unit** — pure transform functions, DataFrame equality via `chispa`. Target 80% coverage on `transform/`.
- **Integration** — `testcontainers` spins a real Postgres + Kafka and runs CDC end-to-end.
- **Chaos** — one test per scenario listed above.
- **Data tests** — Great Expectations and dbt, run in CI against a seeded miniature dataset.

**CI** (`.github/workflows/ci.yml`)

```
lint        -> ruff check + ruff format --check + mypy
unit        -> pytest tests/unit --cov --cov-fail-under=80
contracts   -> Avro BACKWARD compatibility check against main   # fails the PR on a breaking change
integration -> pytest tests/integration (testcontainers)
dbt         -> dbt deps && dbt build --target ci (on a seeded sample)
docs        -> dbt docs generate && deploy to GitHub Pages (main only)
```

The `contracts` job is the one that matters: a pull request that breaks an Avro schema for downstream
consumers fails before it can merge. Data contracts enforced as code, not as policy.

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
├── quality/           Great Expectations suites and checkpoints
├── orchestration/     Airflow DAGs
├── serving/           FastAPI metrics service, Streamlit dashboard
└── tests/             unit, integration, chaos
```

---

## Build status

Built in eight phases, each ending in something demoable and committed.

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Foundation — repo, pre-commit, `core` Compose profile, Postgres DDL + logical replication, ADR-0001 | 🔨 In progress |
| 1 | Source simulation — OLTP generator, order state machine, GPS producer, `--chaos` | ⬜ Not started |
| 2 | CDC ingestion + contracts — Debezium, Avro registry, Bronze streaming, DLQ, exactly-once | ⬜ Not started |
| 3 | Silver — SCD2 `MERGE`, dedup on `(pk, lsn)`, GPS sessionization, GE gate, `OPTIMIZE`/`ZORDER` | ⬜ Not started |
| 4 | Gold — dbt star schema, `dim_date` from holidays, weather join, generic + singular tests, docs | ⬜ Not started |
| 5 | Orchestration — Airflow DAGs, dynamic task mapping, idempotent backfills, SLA callbacks | ⬜ Not started |
| 6 | Observability — OpenLineage → Marquez, Grafana SLO dashboard, alert rules, runbook | ⬜ Not started |
| 7 | Azure slice — Terraform: ADLS Gen2, Event Hubs, Databricks, Unity Catalog, rehearsed teardown | ⬜ Optional |
| 8 | Stretch — Iceberg comparison, Dagster port, streaming Gold, contract CI gate, cost model | ⬜ Optional |

**Done, for the project as a whole:** a stranger can clone this and reach a working pipeline in under 15
minutes from this README alone; CI is green on `main`; dbt docs are live; every chaos scenario has a passing
test; and every component choice has a written ADR behind it.

---

## Azure path

The local stack is free and complete. The Terraform slice exists to prove the same design runs in the cloud.

| Local component | Azure equivalent |
| --- | --- |
| MinIO | ADLS Gen2 |
| Redpanda | Event Hubs (Kafka endpoint — no code change) |
| Spark on Docker | Azure Databricks |
| Delta on MinIO | Delta on ADLS Gen2 + Unity Catalog |
| Airflow | Databricks Workflows, or Azure Managed Airflow |
| Marquez | Unity Catalog lineage / Microsoft Purview |
| Grafana | Azure Monitor + Log Analytics |
| `.env` secrets | Azure Key Vault |

The Spark code is identical across both — only the storage URI (`s3a://` → `abfss://`) and the broker
endpoint change. Deploy with `make azure-up`, tear down with `make azure-down`; a budget alert is part of
the Terraform module, not an afterthought.

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

MIT — see [LICENSE](LICENSE).
