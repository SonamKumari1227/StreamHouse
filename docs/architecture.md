# StreamHouse — Architecture

The design spec for this project. Trimmed from the original planning document to the parts that
describe the system rather than the reasons for building it.

**Constraint that shapes everything below:** the entire stack is cloud-free. Every component runs
locally in Docker. No Azure, AWS, or GCP resources, credentials, or SDKs — see `../CLAUDE.md` for the
full rule and its single narrow Phase 7 exception.

---

## 1. The domain

A quick-commerce delivery marketplace, chosen because it forces every hard data-engineering problem
to appear naturally rather than as a contrived exercise:

- **Mutable entities** — an order moves `PLACED → ACCEPTED → PICKED_UP → DELIVERED | CANCELLED`.
  This cannot be modelled correctly with append-only batch. It is what justifies CDC.
- **High-volume append stream** — rider GPS pings, ~1 ping / 5s / rider. Justifies Structured
  Streaming, watermarking, and state stores.
- **Slowly changing dimensions** — menu prices, restaurant open/closed status, and rider tier change
  over time. Justifies SCD Type 2.
- **Late and out-of-order data** — a delivery confirmation arriving after a rider's phone reconnects.
  Justifies watermarks plus a late-arrival reconciliation job.
- **External enrichment** — weather and public holidays genuinely change delivery times. Justifies
  incremental API ingestion with idempotent backfill.
- **Real business questions** — SLA breach rate, unit economics, rider utilisation, cohort retention.
  Justifies a proper star schema rather than one wide flat table.

---

## 2. High-level flow

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
+------------------------------------------------------------------------+
```

---

## 3. Component decisions

Each of these is a real fork in the road, and each gets an ADR in `decisions/`. The short form:

| Component | Chosen | Why not the alternative |
| --- | --- | --- |
| Message bus | **Redpanda** | Kafka API-compatible, single binary, no ZooKeeper/KRaft tuning, ~1 GB RAM vs Kafka's 4 GB+. Ships a built-in schema registry, so no separate container. |
| CDC | **Debezium** | Log-based — reads the Postgres WAL, so it captures `DELETE`s and never polls the source. Query-based CDC (`WHERE updated_at > x`) silently misses deletes and any intra-interval update. |
| Table format | **Delta Lake** | Mature `MERGE INTO`, time travel, and `OPTIMIZE`/`ZORDER` — all of which the Silver layer depends on. Iceberg is a Phase 8 comparison. |
| Object store | **MinIO** | Self-hosted and S3A-compatible, so the storage layer is swappable by configuration alone. That portability is the point; it is also what Phase 7 proves. |
| Gold transforms | **dbt-spark** | Version-controlled SQL with tests, docs, and lineage included. Hand-written PySpark for Gold provides none of those. |
| Orchestrator | **Airflow 3** | Mature sensors, backfills, SLA handling, and dynamic task mapping. Dagster's asset model is conceptually cleaner — a port of one pipeline is a Phase 8 stretch. |
| Quality | **Great Expectations** (Bronze→Silver) + **dbt tests** (Silver→Gold) | GE catches structural and statistical drift on raw data; dbt tests catch business-rule violations where the business logic already lives. Two layers, two distinct jobs. |
| Lineage | **OpenLineage + Marquez** | Vendor-neutral standard, self-hosted. The same emitted events would later feed any catalog without rework. |

---

## 4. Data model

### 4.1 Source OLTP schema — the CDC source

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

Logical replication must be enabled for Debezium to read the WAL:

```sql
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
-- restart Postgres, then:
ALTER TABLE orders REPLICA IDENTITY FULL;   -- gives "before" images on UPDATE
```

`REPLICA IDENTITY FULL` on `orders` is what makes before/after deltas computable. Without it, Debezium
emits only the primary key in the `before` block on updates. It costs extra WAL volume — a deliberate
trade, recorded in ADR-0001.

### 4.2 Gold star schema

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

**Grain statements** — these go verbatim into the dbt model docs:

- `fact_delivery` — one row per **order**, at its terminal state.
- `fact_order_item` — one row per **(order, menu_item)**.
- `agg_sla_daily` — one row per **(date, city, restaurant)**.

**Aggregates to build in Gold**

- SLA breach rate by city / restaurant / hour-of-day
- Unit economics — contribution margin per order, split across prep vs transit cost
- Rider utilisation — active minutes ÷ shift minutes, plus idle-gap distribution
- Weather impact — median transit time in rain vs clear
- Customer cohort retention by signup month

---

## 5. Data sources

| # | Source | Type | Access | Volume | What it exercises |
| --- | --- | --- | --- | --- | --- |
| A | Synthetic OLTP generator (Python + Faker, order state machine) | Mutating relational | Local Postgres | ~50k orders/day, ~200k updates/day | CDC, SCD2, `MERGE`, deletes, replaying history |
| B | Rider GPS ping producer | High-volume append stream | Local Kafka producer | ~200 msg/s (~5M/day) | Watermarking, sessionization, state stores, backpressure |
| C | **Open-Meteo** — `api.open-meteo.com/v1/forecast` and `/v1/archive` | Real REST API | Free, no key | ~5k rows/day | Incremental pulls, idempotent backfill, rate limits, retry/backoff |
| D | **Nager.Date** — `date.nager.at/api/v3/PublicHolidays/{year}/IN` | Real REST API | Free, no key | ~30 rows/year | Building a proper `dim_date` with holiday flags |
| E | India city/geo reference (static GeoJSON) | Static reference | Free download | ~100 rows | Seed / reference-data management in dbt |

Sources C and D are public HTTPS APIs with no account and no key. They are not cloud-platform
services and are permitted under the cloud-free rule.

### 5.1 Why the OLTP source is synthetic

Deliberately so: no public dataset lets you inject a specific failure mode on demand and then assert
that the pipeline survived it. The generator's `--chaos` flag injects seven scenarios, each with a
matching test in `tests/chaos/`:

1. **Late events** — a `DELIVERED` update arriving 45 minutes after the watermark
2. **Out-of-order timestamps** — `picked_up_ts` before `accepted_ts`
3. **Duplicate CDC records** — Debezium redelivery after a connector restart
4. **Schema evolution** — a new nullable column, then a *breaking* type change the contract must reject
5. **NULL floods** — 30% of `rider_id` suddenly NULL
6. **Referential integrity breaks** — an `order_items` row pointing at a deleted `menu_item`
7. **Traffic burst** — 3× volume for 10 minutes, to exercise backpressure

---

## 6. Build phases

Each phase ends in something demoable and committed. The repo never sits in a broken state.

| Phase | Scope | Done when |
| --- | --- | --- |
| **0** | Foundation — repo skeleton, `core` Compose profile, Postgres DDL + logical replication, ADR-0001 | `make up` gives a healthy stack; `SELECT * FROM pg_replication_slots;` works |
| **1** | Source simulation — OLTP generator with order state machine, dimension mutations, GPS producer, `--chaos` | The generator runs an hour unattended and produces a plausible order distribution |
| **2** | CDC ingestion + contracts — Debezium connector, Avro schemas registered `BACKWARD`, Bronze streaming with checkpoints, DLQ for violations | `UPDATE orders SET status='DELIVERED'` lands in Bronze Delta within seconds |
| **3** | Silver — dedup on `(order_id, lsn)`, SCD2 via Delta `MERGE` with `valid_from`/`valid_to`/`is_current`, GPS sessionization, GE gate, `OPTIMIZE`/`ZORDER` | Time travel returns full price history with non-overlapping validity windows, asserted by test |
| **4** | Gold — dbt-spark star schema, `dim_date` from Nager.Date, Open-Meteo join, generic + singular tests | `dbt build` is green and docs generate |
| **5** | Orchestration — Airflow 3 DAGs, streaming-health sensors, dynamic task mapping over cities, idempotent backfills, SLA callbacks | Deleting a day of Gold and re-running the backfill produces bit-identical output |
| **6** | Observability — OpenLineage → Marquez, Grafana SLO dashboard, Prometheus alert rules, runbook | Killing the CDC connector produces an actionable alert within 15 minutes |
| **7** | Portability & IaC (validate-only, no cloud spend) — Terraform as a documented design exercise, storage-layer portability proven by config swap, ADR on the local→cloud mapping | `terraform validate` passes and the storage swap runs with zero code changes. **`apply` is never run.** |
| **8** | Stretch — Iceberg comparison, Dagster port, streaming Gold, contract CI gate, cost model | Pick one or two; do not attempt all |

Phase 3 is the hard one. SCD2 via `MERGE` over streaming CDC is genuinely difficult — idempotency,
out-of-order updates, and the "two updates in the same micro-batch" problem. Budget accordingly.

---

## 7. Engineering practices

### 7.1 Testing pyramid

- **Unit** (fast, no containers) — pure transform functions tested with `chispa` DataFrame equality.
  Target 80% coverage on `transform/`.
- **Integration** (`testcontainers`) — a real Postgres + Kafka, CDC run end-to-end, assertions on Bronze.
- **Chaos** — one test per scenario in §5.1.
- **Data tests** — Great Expectations and dbt, run in CI against a seeded miniature dataset.

### 7.2 CI

```
lint        -> ruff check + ruff format --check + mypy
unit        -> pytest tests/unit --cov --cov-fail-under=80
contracts   -> Avro BACKWARD compatibility check against main   # fails the PR on a breaking change
integration -> pytest tests/integration (testcontainers)
dbt         -> dbt deps && dbt build --target ci (on a seeded sample)
docs        -> dbt docs generate
```

The `contracts` job is the substantive one: a pull request that breaks an Avro schema for downstream
consumers fails before it can merge. Data contracts enforced as code, not as policy.

### 7.3 Runbook

`docs/runbook.md` is the living operations doc — how to run the stack, verify each service, and fix
common failures. From Phase 6 it also carries one entry per configured alert, in this shape:

```markdown
## ALERT: bronze_freshness_breach (> 15 min)

**Symptom:** max(ingest_ts) on bronze.raw_orders_cdc is older than 15 minutes.

**Likely causes, most common first**
1. Debezium connector has FAILED    -> curl localhost:8083/connectors/streamhouse-pg/status
2. Replication slot is full/stalled -> SELECT * FROM pg_replication_slots;
3. Spark streaming job died         -> check Spark UI, then the checkpoint dir
4. MinIO is out of disk             -> docker compose exec minio df -h

**First three commands**
    make health
    docker compose logs --tail=100 connect
    make stream-status

**Recovery:** restarting the Spark job is always safe — checkpoints guarantee exactly-once.
Restarting Debezium is safe as long as the replication slot still exists. Never drop the
replication slot without a planned re-snapshot; the WAL is gone the moment you do.
```

### 7.4 ADRs

One Architecture Decision Record per real fork in the road, ~200 words: context, options, decision,
consequences. ADRs are immutable once accepted — supersede, never edit. Minimum set:

1. Log-based vs query-based CDC
2. Delta vs Iceberg
3. Airflow vs Dagster
4. SCD2 vs snapshot-per-day on dimensions
5. Redpanda vs Kafka
6. Where quality is enforced — ingestion vs transform
7. Local→cloud mapping (Phase 7)

---

## 8. Runtime footprint

The full stack is ~10.5 GB at peak, split across Compose profiles so it is never all needed at once.

| Service | Image | Ports | RAM | Profile |
| --- | --- | --- | --- | --- |
| postgres (OLTP source) | `postgres:16` | 5432 | 512 MB | core |
| redpanda | `redpandadata/redpanda` | 9092, 8081 (registry) | 1 GB | core |
| redpanda-console | `redpandadata/console` | 8080 | 256 MB | core |
| connect + Debezium | `debezium/connect:2.7` | 8083 | 1 GB | core |
| minio | `minio/minio` | 9000, 9001 (UI) | 512 MB | core |
| spark-master / spark-worker | `bitnami/spark:3.5` | 7077, 8090 | 3 GB | core |
| airflow (webserver + scheduler) | `apache/airflow:3.0` | 8082 | 2 GB | orchestration |
| postgres-meta (Airflow + Marquez backing DB) | `postgres:16` | 5433 | 512 MB | orchestration |
| marquez + marquez-web | `marquezproject/marquez` | 5000, 3000 | 768 MB | observability |
| prometheus | `prom/prometheus` | 9090 | 256 MB | observability |
| grafana | `grafana/grafana` | 3001 | 256 MB | observability |

```bash
docker compose --profile core up -d          # ~6 GB
docker compose --profile orchestration up -d # + airflow
docker compose --profile observability up -d # + marquez, prometheus, grafana
```

Docker Desktop needs at least 8 GB RAM and 4 CPUs allocated. ~40 GB free disk.

---

## 9. Known failure modes

Read this before hitting them.

| Problem | Cause | Fix |
| --- | --- | --- |
| Debezium snapshot never finishes | `snapshot.mode=initial` on a large table | Seed a small dataset first; use `snapshot.mode=never` after the first run |
| Postgres disk fills up | An inactive replication slot retains WAL forever | Monitor `pg_replication_slots.active`; drop unused slots; alert on WAL size |
| Spark job OOMs on Silver | Too few shuffle partitions, or a skewed `MERGE` | Enable AQE (`spark.sql.adaptive.enabled=true`); salt hot keys |
| Duplicate rows after a restart | Non-idempotent sink | Dedup on `(pk, lsn)` inside `foreachBatch`; make the `MERGE` the idempotency boundary |
| SCD2 windows overlap | Two updates to the same key inside one micro-batch | Rank by LSN within the batch, keep only the latest per key before merging |
| Delta table gets slow | Small-file problem from streaming micro-batches | Scheduled `OPTIMIZE`; tune `maxFilesPerTrigger`; enable auto-compaction |
| `dbt build` can't see the tables | Spark Thrift server not up, or wrong catalog | Verify the Thrift endpoint; set `catalog`/`schema` explicitly in `profiles.yml` |
| Everything is slow on Windows | Docker volumes mounted from a Windows path | Keep the repo **and** all Docker volumes inside WSL2, not `C:\Users\...` or `E:\...` |

The last row applies to this repo: it currently lives on `E:\`. The planned remedy is to relocate the
repo and Docker volumes into WSL2 before Phase 2 — see `../CLAUDE.md` under "Known deviation".
