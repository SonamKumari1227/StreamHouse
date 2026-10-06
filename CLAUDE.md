# CLAUDE.md — StreamHouse

Instructions and current state for any Claude Code session working in this repo.
**Read this file before doing anything else. Update it whenever the state changes.**

---

## What this project is

StreamHouse is a real-time CDC lakehouse. PostgreSQL OLTP changes are captured by Debezium via
logical replication, streamed through Redpanda into a Delta Lake medallion architecture
(Bronze → Silver → Gold), and served as a dbt-modeled star schema — with Avro contracts enforced
at the schema registry, Great Expectations and dbt tests gating each layer boundary, OpenLineage
lineage into Marquez, and freshness/SLO alerting via Prometheus + Grafana.

Domain: a quick-commerce delivery marketplace. Orders mutate through a real state machine
(`PLACED → ACCEPTED → PICKED_UP → DELIVERED | CANCELLED`), riders emit GPS pings at ~200 msg/s,
and menu prices change over time. The mutation is the point — it is what makes CDC, SCD Type 2,
watermarking and exactly-once sinks necessary rather than decorative.

Full design spec: `docs/architecture.md` (trimmed and committed — the source of truth for design).
Reader-facing overview: `README.md`.

---

## Hard rules

These are not preferences. Violating one means the work gets reverted.

### 1. Cloud-free — Phases 0–6 and 8

**No Azure, AWS, or GCP resources, credentials, SDKs, client libraries, or references anywhere in
code, configs, docs, CI, or dependencies.** Everything runs locally in Docker.

Specifically forbidden: `azure-*`, `boto3`, `google-cloud-*`, `adlfs`, `s3fs` against real S3,
`abfss://` URIs, Terraform azurerm/aws/google providers, Databricks/EMR/Dataproc config, cloud
Key Vault / Secrets Manager, managed Kafka or managed Airflow endpoints.

Explicitly allowed, because they are not cloud platforms:
- **MinIO** — self-hosted, S3A-compatible, runs in Docker. `s3a://` URIs point at local MinIO only.
- **Open-Meteo** and **Nager.Date** — free public HTTPS APIs, no account, no key.
- **GitHub** — version control and CI. It is a hosted service, not a cloud platform.

**The one narrow exception, Phase 7 only.** Phase 7 was an Azure deployment slice; it is now
*Portability & IaC (validate-only, no cloud spend)*. When and only when Phase 7 is the active phase,
Terraform describing the cloud-equivalent mapping may live in `infra/terraform/` as a **documented
design exercise**. `terraform validate` and `terraform plan` run offline against no subscription and
are permitted. **`terraform apply` is forbidden, permanently, unless the user explicitly changes this
rule.** No credentials, no subscription, no live deployment, ever.

During Phases 0–6 and 8 this exception does not apply: write no Terraform and no cloud deployment
docs at all.

### 2. No phase-skipping

Work only within the **active phase** marked below. Do not build ahead, even when it looks trivial
or the next phase is "just one file." A phase ends in something demoable; jumping ahead leaves the
repo in a half-working state, which defeats the point.

If something in the active phase genuinely requires a later-phase artifact, stop and say so rather
than building it.

### 3. Git is manual — the user owns it

**Never run a git command that changes anything.** No `init`, `add`, `commit`, `push`, `pull`,
`merge`, `rebase`, `checkout`, `reset`, `stash`, `tag`, or `branch` creation. Create and edit files
on disk and stop there. The user reviews, stages, commits and pushes manually.

Read-only inspection is permitted when the user asks for it — `status`, `log`, `diff`, `remote -v`,
`branch -vv`, `show`. Use it to keep the state block below accurate, not to act on the repo.

### 4. Ask before creating, modifying, or running

State what you are about to do and wait for go-ahead. Do not execute a batch of actions silently.
Show each file or group of files after creating it, then wait before continuing.

---

## Current state

| | |
| --- | --- |
| **Active phase** | **Phase 5 — COMPLETE and verified 2026-10-06.** Phase 6 (Observability: OpenLineage, Grafana, Prometheus) is next and has not started. Phases 0–4 completed earlier. |
| Repo root | **`/home/sonam/streamhouse` inside WSL2 (Ubuntu 26.04).** This is canonical. The old `E:\streamhouse-project\streamhouse\` copy is stale - do not work in it. |
| Git | Remote `origin` → `https://github.com/SonamKumari1227/StreamHouse.git`, branch `master`, in sync with origin at `ff8deaf` as of 2026-10-05. **User handles all staging, commits and pushes manually** (hard rule 3). |
| IDE | PyCharm — `.idea/` present and already gitignored. |
| Python 3.11 | `/usr/bin/python3.11` in the Ubuntu distro. The host Windows install is no longer used. Note Ubuntu 26.04's own `python3` is **3.14**, which PySpark does not support — always go through `.venv`. |
| venv | `.venv` (Python 3.11.16). Deps installed **per phase**, not from `requirements.txt` wholesale — see the Airflow blocker below. **PySpark and Delta are deliberately absent**; see "Where Spark code runs" below. |
| Docker | 27.5.1, Compose v2.32.4. Core stack verified healthy 2026-10-05, all 7 containers. Docker Desktop WSL2 integration is enabled for Ubuntu, so `docker` works from inside the distro against the same daemon. |
| GNU Make | 4.4.1 from Ubuntu's apt, on PATH. The GnuWin32 3.81 caveat below applied to the Windows copy and no longer binds — but nothing in the Makefile relies on Make 4.x either. |

### Phase 0 progress

- [x] `CLAUDE.md`
- [x] `docs/architecture.md` — trimmed design spec
- [x] Folder skeleton with per-folder README stubs
- [x] `docs/decisions/` — ADR template + ADR-0001 (log-based vs query-based CDC)
- [x] `docs/runbook.md` — local run guide; grows into the alert runbook in Phase 6
- [x] `infra/docker-compose.yml` — core profile, healthchecks, `docker compose config` validated
- [x] `infra/postgres/init/01-schema.sql` — 7 tables, idempotent, `REPLICA IDENTITY FULL` on `orders`
- [x] `.env.example`, `Makefile`, `requirements.txt` — **`.gitignore` already existed; left alone**
- [x] `README.md` — cloud references stripped, Phase 7 retitled, run instructions made truthful

**Phase 0 is done when:** `make up` brings up a healthy core stack and
`SELECT * FROM pg_replication_slots;` works against the Postgres container.

**VERIFIED on a running stack, 2026-09-24.** Actual output:

```
sh-connect            Up (healthy)      sh-minio       Up (healthy)
sh-postgres           Up (healthy)      sh-redpanda    Up (healthy)
sh-spark-master       Up (healthy)      sh-spark-worker Up (healthy)
sh-redpanda-console   Up               (no healthcheck by design; HTTP 200 confirmed)

wal_level        : logical        <- the Phase 0 condition
max_repl_slots   : 10
max_wal_senders  : 10
tables           : 7
orders replident : f              <- REPLICA IDENTITY FULL
repl slots       : 0              <- correct; Debezium registers in Phase 2
redpanda         : Healthy: true
schema registry  : []             <- correct until Phase 2
connectors       : []             <- correct until Phase 2
pg plugin        : PostgresConnector
minio live       : OK
spark master     : ALIVE, 1 worker registered, 2 cores / 2048 MB
```

Schema re-applied a second time to prove idempotency: only `... already exists, skipping` notices,
no errors.

**The Makefile has now been executed directly.** `make help`, `make ps`, `make db-init` and
`make health` all run clean; `make health` prints `wal_level : logical` and `workers alive : 1`.

**GNU Make 3.81 is installed at `C:\Program Files (x86)\GnuWin32\bin\make.exe` but is NOT on
PATH**, so a bare `make` fails with `command not found`. Either add that directory to PATH
permanently, or prefix the shell session:

```bash
export PATH="/c/Program Files (x86)/GnuWin32/bin:$PATH"
```

Make 3.81 is from 2006. It works for every target here, but do not use `.ONESHELL`, `!=` or other
Make 4.x features in this Makefile without testing them first.

### Bugs found and fixed during verification

1. **`.env` not read.** Compose resolves `.env` relative to the compose file's directory (`infra/`),
   not the working directory. Fixed: `--env-file .env` in the Makefile's `COMPOSE` variable.
2. **`debezium/connect:2.7` does not exist.** Debezium publishes only `X.Y.Z.Final` tags. Fixed:
   `debezium/connect:2.7.3.Final`.
3. **`minio/minio` on Docker Hub now requires authentication** (`denied: requested access to the
   resource is denied`). MinIO publishes publicly to quay.io. Fixed:
   `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z`.
4. **Host environment silently overrode `.env`.** This machine exports `POSTGRES_USER=ca.team`, and
   Compose gives the shell environment precedence over `.env` — so Postgres initialised with the
   wrong role and every connection failed with `role "streamhouse" does not exist`. Fixed by
   prefixing every project variable with `SH_` (`SH_POSTGRES_USER`, etc.), which makes ambient
   collisions effectively impossible. **Do not remove the prefix.**
5. **Git Bash path mangling.** `psql -f /docker-entrypoint-initdb.d/01-schema.sql` was rewritten by
   MSYS into `C:/Program Files/Git/docker-entrypoint-initdb.d/...`. Fixed: `db-init` now pipes the
   host file into `psql` on stdin, which is also mount-independent.
6. **Makefile variable-ordering bug.** `PSQL := ... -U $(PG_USER) -d $(PG_DB)` was defined *above*
   `PG_USER ?=` / `PG_DB ?=`. `:=` expands immediately, so both were empty and psql parsed the next
   flag as the username: `FATAL: role "-d" does not exist`. Fixed by defining `PG_USER`/`PG_DB`
   first. Only surfaced by running `make db-init` for real — `docker compose` equivalents hid it.
7. **Recipe cosmetics.** `#` comment lines inside a recipe are echoed and passed to the shell; moved
   above the target. GNU Make 3.81 on Windows also mangles non-ASCII in `echo` (the em-dash printed
   as `â€"`), so recipe output is now ASCII-only.

### Phase 1 progress

- [x] `generator/__init__.py`
- [x] `generator/state_machine.py` — pure transition logic, no I/O
- [x] `tests/unit/test_state_machine.py` — 34 tests
- [x] `pyproject.toml` — pytest `pythonpath`, markers, ruff (line 100, py311), mypy strict
- [x] `generator/config.py` — cities, cuisines, dishes, enums, volume knobs, `LoadConfig`
- [x] `tests/unit/test_config.py` — 41 tests, incl. DDL CHECK-constraint parity
- [x] `generator/seed.py` — reference data builders + idempotent writer
- [x] `tests/unit/test_seed.py` — 33 tests against a fake cursor
- [x] `tests/integration/test_seed_idempotency.py` — 8 tests against real Postgres
- [x] `generator/repository.py` — the storage boundary; owns all SQL
- [x] `generator/oltp_generator.py` — the daemon: arrivals, transitions, mutations, recovery
- [x] `tests/unit/test_oltp_generator.py` — 45 tests against a fake repository
- [x] `tests/integration/test_oltp_generator_writes.py` — 15 tests against real Postgres
- [x] `contracts/gps_ping.v1.avsc` — the GPS contract, every field documented
- [x] `generator/gps_producer.py` — Avro pings to Redpanda at ~200 msg/s
- [x] `tests/unit/test_gps_producer.py` — 47 tests, no broker
- [x] `tests/integration/test_gps_producer_redpanda.py` — 9 tests against real Redpanda
- [x] `generator/chaos.py` — named composable scenarios (duplicates, out-of-order)
- [x] `tests/unit/test_chaos.py` — 42 tests
- [x] `tests/integration/test_chaos_duplicates.py` — 10 tests, real logical decoding
- [x] `generator/__main__.py` — CLI
- [x] `tests/unit/test_cli.py` + `tests/integration/test_cli_modes.py` — 23+12 tests

**Verified 2026-09-24**, actual output:

```
ruff check           : All checks passed!
ruff format --check  : 21 files already formatted
mypy (strict)        : Success: no issues found in 21 source files
full suite           : 329 passed in 90s   (unit + integration)
coverage             : generator/  1210 stmts, 7 miss, 99%
```

**Committed GPS demo**, real messages on a real topic:

```
topic gps.pings, 6 partitions, high watermarks 729+741+454+902+775+399 = 4000
pings=4000  trips_started=3  avg_msg=57.6B  throughput=199 msg/s  failures=none
consumed 4000 back: 120 distinct riders, 123 distinct trips, 499 stationary pings
one message: 57 bytes on the wire, decodes cleanly against contracts/gps_ping.v1.avsc
```

**Committed demo run**, not a rolled-back test — real rows in the dev database:

```
seeded   : 500 customers, 80 restaurants, 570 menu items, 120 riders
run 1    : placed=249 transitions=152 delivered=0  (25s wall; a lifecycle takes ~90s at 20x)
run 2    : recovered=245 from run 1, placed=1002 transitions=2811 delivered=756 cancelled=91
database : orders=1251  order_items=3708  payments=1251
status   : DELIVERED 60.4% | PICKED_UP 16.7% | ACCEPTED 12.2% | CANCELLED 7.6% | PLACED 3.0%
mutated  : 31 menu_items, 10 restaurants, 19 riders  (Phase 3 SCD2 material)
CDC      : 1213 of 1251 orders updated after insert — multiple events per entity
```

Order 4939 end to end: 3 line items summing to 3975.96 + 35.34 delivery = 4011.30 total,
timestamps strictly ordered, rider assigned at acceptance, payment CAPTURED, delivered
before promised_ts.

The dev database was left untouched — the integration fixture rolls its transaction back,
confirmed by all four reference tables reading 0 afterwards.

**Test layout.** `pytest` runs unit tests only; `addopts` carries `-m 'not integration'`.
Run `pytest -m integration` for the Postgres round trip, which needs `make up` first and
skips cleanly if the database is unreachable.

**Two things worth knowing about the integration tests:**
- They wrap everything in a transaction and roll it back, so the dev stack is left as found.
- `setval` is **not** transactional in PostgreSQL, so sequence values survive that rollback.
  Harmless — the sequence simply points past ids that no longer exist — but do not be
  surprised by it.

Decisions settled by the user for Phase 1:

- **`--speed` multiplier, default 20.0.** Compresses every duration so a demo shows a realistic
  status distribution in ~90s per order instead of ~30 min. `--speed 1` for an honest overnight run.
  `promised_ts` is scaled too, or the SLA breach rate would be meaningless.
- **`--chaos` takes named composable scenarios**, e.g. `--chaos duplicates,out-of-order`, so each
  test in `tests/chaos/` can request exactly its own scenario. Not a boolean flag.

State machine design, for anyone extending it:

- Table-driven via `DEFAULT_RULES`; adding a state means adding a row.
- Due-time driven (`next_due_at`), not tick-driven, so lifecycles overlap realistically.
- Transitions stamp at `next_due_at`, **not** at `now` — polling lateness must not leak into data.
- `apply_transition` rejects a stale transition rather than absorbing it. Do not relax this; it is
  the same class of bug that corrupts SCD2 in Phase 3.
- It is the **only** writer of `status` and the `*_ts` columns. Chaos wraps it from outside, never
  reaches in — that is what makes a bad row attributable to a named scenario instead of a bug.

### Generator design decisions (Phase 1)

- **The scheduler is in memory, not in the database.** There is no `next_due_at` column on
  `orders` and there must not be: it is generator bookkeeping, not business data, and it would
  push a meaningless column into the CDC stream. Postgres is write-only in the hot path.
- **Restart recovery exists because of that choice.** `OltpGenerator.recover()` reloads
  non-terminal orders and reschedules them with fresh delays. Proven against real data: a
  second process picked up 245 in-flight orders and drove them to terminal.
- **Arrivals are a Poisson process**, not fixed spacing. A perfectly regular stream would let
  Phase 3's watermarking and late-arrival handling pass against a distribution that never
  occurs in reality.
- **Arrival rate is independent of in-flight count.** A backlog must not throttle new orders.
- **Dimension mutations run on simulated time**, every `MUTATION_INTERVAL_S / speed` seconds.
  A high arrival rate therefore finishes an order budget *before* the first mutation is due —
  this caused a real test failure. Drive mutation tests with `until=`, not `max_orders=`.
- **`advance_order` validates the timestamp column against an allowlist** because that name is
  interpolated into SQL. Everything else is parameterised.

### Kafka / Redpanda pinning — do not "simplify" these

- **`confluent-kafka` must be >= 2.6.1.** librdkafka **2.6.0 is a regression**: it sends
  Fetch API v12 even when the broker advertises a maximum of 11. Producing works and
  consuming silently returns **nothing** — no exception, no error, just zero messages. The
  broker logs `Unsupported version 12 for fetch API`. Diagnosed by dumping the negotiated
  ApiVersions: the broker declared `Fetch (1) Versions 4..11`. Verified: 2.4.0, 2.5.3, 2.6.1,
  2.8.0 and 2.11.1 all work; only 2.6.0 fails.
- **Redpanda is on `v24.3.11`.** The upgrade from v24.2.7 was **not** the fix — the client pin
  was. v24.3.11 is kept because it is current and stable here. **`v25.2.3` crashes on this
  machine** (SIGILL, exit 132, Seastar backtrace). Do not move to 25.x without testing.
- The `redpanda-data` volume was wiped once during that diagnosis. Nothing of value was in it;
  the Postgres volume was deliberately left untouched.

### GPS producer notes

- **Phase 1 writes bare Avro** (`fastavro.schemaless_writer`), not the Confluent wire format.
  Phase 2 adds the magic byte and registry schema id once the schema is registered. The
  `.avsc` file is the contract either way; only the framing changes.
- **Messages are keyed by `rider_id`** so one rider's pings keep their order within a
  partition. Sessionization in Phase 3 depends on it; an integration test asserts no rider is
  ever split across partitions.
- **`event_ts` is the device time, never the send time.** Chaos scenario 1 will delay delivery
  well past it, and the Phase 3 watermark depends on the distinction. The Kafka message
  timestamp is set from `event_ts` explicitly, and a test asserts the two match.
- **A leg's length is stored on the track, not redrawn per ping.** It used to be redrawn,
  which made arrival time unrelated to the distance supposedly covered — `trips_started` sat
  at 0 through a 20s demo. After the fix the same run produced 3 trip rollovers.

### Spark runtime - RESOLVED 2026-09-28

The smoke test that was blocking `bronze_cdc_stream.py` is written, passing and committed.
`make spark-smoke` re-runs it. Verified: Delta write/read to `s3a://bronze/_smoke`,
DESCRIBE HISTORY showing real transaction-log versions, append creating version 1, time travel
to version 0, and `spark-sql-kafka` reading `cdc.public.orders`.

Configuration is pinned in `infra/spark/Dockerfile` and `infra/spark/spark-defaults.conf` -
no `--conf` flags at submit time, no `--packages` resolution. See ADR-0009 for the version
constraints and for the reasoning on `aws-java-sdk-bundle` under the cloud-free rule.

**Constraint this exposed:** the Spark image runs **Python 3.8.10**, not 3.11. Anything under
`ingestion/` runs via `spark-submit` inside that container, so it must avoid `StrEnum`,
`slots=True` dataclasses and `match`. The generator is unaffected - it runs on the host.

### Phase 2 starting state (reset 2026-09-24)

Verified immediately before Phase 2:

```
customers 500 | restaurants 80 | menu_items 570 | riders 120
orders 0 | order_items 0 | payments 0
out-of-sequence rows : 0   (chaos residue cleared)
replication slots    : 0   (Debezium creates its own)
topics               : _connect_configs, _connect_offsets, _connect_status, _schemas
                       gps.pings DELETED - 9000 stale messages removed, Phase 2 starts at offset 0
```

Reference data is seeded and deterministic (seed 20260924); no transactional rows exist, so
Debezium's initial snapshot is small and its first CDC events are the generator's own writes.

### Phase 2 progress

- [x] `infra/connect/Dockerfile` — Confluent Avro converters layered onto Debezium, cloud SDKs stripped
- [x] `infra/connectors/orders-postgres.json` — the connector in version control, not in a shell history
- [x] `make connector-register` / `connector-status` — idempotent `PUT .../config`
- [x] `infra/spark/Dockerfile` + `spark-defaults.conf` — Delta 3.2.1, hadoop-aws 3.3.4, spark-sql-kafka, spark-avro
- [x] `generator/registry.py` — schema registry client and the Confluent wire format
- [x] `contracts/orders.v1.avsc`, `orders.v2.avsc` — the captured CDC contract and its evolution
- [x] `ingestion/smoke_test.py` — proves Delta + S3A + Kafka before any streaming job is written
- [x] `ingestion/bronze_cdc_stream.py` — MERGE on `(source_table, pk, lsn)`, DLQ, tombstones handled
- [x] `ingestion/bronze_gps_stream.py` — append-only, dedup on `(topic, partition, offset)`
- [x] `tests/chaos/test_schema_evolution.py` — the registry gate, exercised against the live registry
- [x] `tests/spark/test_bronze_transforms.py` — 12 tests on the transforms (added 2026-10-05)
- [x] ADR-0007 (healthchecks), ADR-0008 (CDC topics and schema capture), ADR-0009 (Spark runtime)
- [x] `docs/runbook.md` §4.7–4.10 — Connect topics, connector operations, load-test results, schema evolution

**Phase 2 is done when:** `UPDATE orders SET status='DELIVERED'` lands in Bronze Delta within
seconds. Met — §4.9 reconciles 8384 messages to 8383 Bronze rows with 0 duplicates, p50 latency
19s, and §4.10 proves the registry refuses an incompatible schema.

**VERIFIED 2026-10-05**, actual output:

```
ruff check           : All checks passed!
ruff format --check  : 29 files already formatted
mypy (strict)        : Success: no issues found in 29 source files
host suite           : 275 passed, 61 deselected in 1.0s
make test-spark      : 12 passed in 12.2s
```

### What 2026-10-05 had to repair

Phase 2 was written and committed with its quality gates red — 51 ruff findings, 31 mypy
errors — and with **no tests at all** on `ingestion/`, the code the phase exists to deliver.
Both are closed now. Three things are worth keeping in mind so it does not recur:

- **`ingestion/` had no `__init__.py`.** mypy then resolved the same file under two module
  names (`bronze_cdc_stream` and `ingestion.bronze_cdc_stream`) the moment a test imported it
  by package path, and refused to check anything. It is a package now, as `generator/` is.
- **A marker does not stop an import.** `tests/spark/` is deselected on the host by the
  `spark` marker, but pytest collects before it deselects, and collection imports pyspark,
  which the host venv has not got. `tests/spark/conftest.py` sets `collect_ignore_glob` so
  the host run skips the directory outright rather than dying on a collection error.
- **`make stream-gps` was documented in the job's own docstring but never existed** in the
  Makefile. Added.

### Where Spark code runs, and where its tests run

**PySpark and Delta are deliberately not installed in `.venv`.** They live in the Spark image
(ADR-0009), and everything under `ingestion/` runs there via `spark-submit`. Installing a
host copy would mean a JDK plus a 300 MB wheel pinned to match the image, maintained in
parallel, testing a runtime that is not the one that ships.

The consequence is a split test suite, and both halves must pass:

| Command | Runs | Where |
| --- | --- | --- |
| `make test` | 275 unit tests | host venv, no containers, ~1s |
| `make test-spark` | 12 transform tests | inside the Spark image, ~13s |
| `pytest -m integration` | Postgres/Redpanda round trips | host, needs `make up` |

`make test-spark` passes `-p no:cacheprovider` because the repo is bind-mounted into Spark
**read-only** and the container runs as `spark`, not as the host user — pytest writing
`.pytest_cache` there fails. Tests in `tests/spark/` are Python 3.8 code, same as the jobs.

### Phase 3 progress

- [x] `transform/silver/fact_order_state.py` — one row per order, latest state
- [x] `tests/spark/test_fact_order_state.py` — 13 tests, incl. the MERGE replay guard
- [x] `transform/silver/dim_scd2.py` — all three dimensions, one parameterised module
- [x] `tests/spark/test_dim_scd2.py` — 15 tests, incl. the non-overlapping-windows bar
- [x] `transform/silver/gps_trips_sessionized.py` — watermarked on event time
- [x] `tests/spark/test_gps_trips.py` — 11 tests
- [x] `quality/expectations.py` + `quality/run_gate.py` — the Bronze → Silver gate
- [x] `tests/spark/test_expectations.py` — 16 tests
- [x] `transform/silver/maintain.py` — `OPTIMIZE`/`ZORDER` + a stated `VACUUM` policy
- [x] ADR-0010 — why the gate is not Great Expectations
- [x] `make silver-orders`, `silver-dim DIM=`, `silver-dims`, `silver-trips`,
      `quality-gate QUALITY_TABLE=`, `silver-maintain` (all take `ONCE=1` where it applies)

**VERIFIED 2026-10-05/06** against the real tables, not fixtures.

`fact_order_state`:

```
bronze change rows                        : 8387
bronze distinct orders                    : 2134
silver rows / distinct keys               : 2134 / 2134   <- no duplicate keys
silver deleted (is_deleted)               : 1
orders whose silver LSN != bronze max LSN : 0             <- ranking provably right
status: DELIVERED 706 | PICKED_UP 648 | ACCEPTED 513 | CANCELLED 158 | PLACED 109 = 2134
types: placed_ts timestamp (microseconds), total_inr decimal(10,2)
```

The SCD2 dimensions, against **the correctness bar in `transform/silver/README.md`**:

```
                        rows   keys   versions per key
dim_restaurant_scd2      118     80   1->50, 2->22, 3->8    (50 + 44 + 24 = 118)
dim_rider_scd2           161    120   1->90, 2->19, 3->11   (90 + 38 + 33 = 161)
dim_menu_item_scd2       625    570   1->530, 2->25, 3->15  (530 + 50 + 45 = 625)

keys with more than one open window      : 0   (all three)
overlapping validity windows             : 0   (all three)
closed windows handing over to nothing   : 0   (all three)
```

Price history, read straight out of `dim_menu_item_scd2` - contiguous windows, one open end:

```
menu_item_id price_inr valid_from                 valid_to                   is_current
1            103.59    2026-10-05 16:18:55.884795 2026-10-05 17:18:55.933130 false
1            113.95    2026-10-05 17:18:55.933130 2026-10-05 18:18:55.947238 false
1            108.25    2026-10-05 18:18:55.947238 NULL                       true
```

`gps_trips_sessionized`: 167 trips covering all 12000 pings - 120 riders each mid-trip at
start, plus the 47 rollovers the producer reported.

The quality gate, over every Silver table:

```
fact_order_state       2134 rows, 2132 passed,    2 quarantined (rider_assigned_once_picked_up)
dim_restaurant_scd2     118 rows,  118 passed,    0 quarantined
dim_rider_scd2          161 rows,  161 passed,    0 quarantined
dim_menu_item_scd2      625 rows,  625 passed,    0 quarantined
gps_trips_sessionized   167 rows,  167 passed,    0 quarantined
```

**The two quarantined rows are real and were hand-made.** Orders 15765 and 16428 were given
`status='DELIVERED'` by direct SQL during earlier CDC testing, bypassing the generator's state
machine, so they are DELIVERED with no rider. The gate caught exactly the rows that did not go
through the front door. Nothing to fix in the pipeline; left quarantined as evidence.

Maintenance, first run:

```
fact_order_state      200 -> 1 file(s), zordered by order_id
dim_restaurant_scd2    61 -> 1          zordered by restaurant_id
dim_rider_scd2         86 -> 1          zordered by rider_id
dim_menu_item_scd2    188 -> 1          zordered by menu_item_id
gps_trips_sessionized 112 -> 1          zordered by trip_id       all vacuumed at 168h
```

#### What the CDC payload actually looks like

Established by reading the registered schema and a real Bronze row. Do not re-derive these
from first principles; they are not what you would guess:

- **Timestamps are ISO-8601 strings, not epoch millis.** A `timestamptz` column under
  `time.precision.mode=connect` becomes `io.debezium.time.ZonedTimestamp`, which is a string
  like `2026-09-28T17:11:53.125601Z`. They are **cast**, never divided by 1000. The GPS
  contract is the opposite - a bare long of millis - so the two jobs differ on purpose.
- **Money carries `logicalType: decimal`** with scale 2, precision 10, so `from_avro` yields a
  real `DecimalType(10,2)` and Bronze's `after_json` holds `2001.06`, a JSON number. Had the
  logical type been absent, Spark would have decoded raw bytes and `to_json` would have
  written **base64** into Bronze - worth checking before trusting any new decimal column.
- **`to_json` omits null fields entirely.** A PLACED order's `after_json` has no `rider_id`
  key at all, not `"rider_id": null`. Parsing must tolerate absence, which `from_json` does.
- **`after` is a bare Avro reference** to the record defined under `before` (named `Value`),
  so a tool walking the schema has to resolve it by name or it finds nothing.

#### Kafka retention ate the dimension history (2026-10-06)

The dimension CDC was gone before Silver was built. `cdc.public.restaurants` reported a high
watermark of 86 but `LOG-START-OFFSET == HIGH-WATERMARK` on every partition: `retention.ms` is
**604800000 (7 days)**, and the Phase 2 snapshot plus the Phase 1 mutations were older than
that. The Bronze stream read the topic and correctly landed nothing.

**CDC that is not landed in Bronze inside the retention window is gone.** Not degraded -
gone, because the WAL slot has long since advanced past it. This is the concrete argument for
Phase 6's freshness alerting: a stream that stops is not an inconvenience, it is a deadline.

The history was rebuilt by applying deliberate, ordered changes in Postgres (`round 0`
re-emits every row as a synthetic re-snapshot, rounds 1 and 2 change shrinking subsets), which
makes better SCD2 material than the original random mutations: keys end up with one, two and
three versions, which is what exercises both the chaining and the close-the-open-row path.

#### `from_avro` honours logical types, and a fixture that lied (2026-10-06)

`bronze_gps_stream.py` had never run against real data - `gps.pings` was deleted before Phase
2 ended - and its first contact failed outright:

```
[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve "(ping.event_ts / 1000)"
... incompatible types ("TIMESTAMP" and "INT")
```

The contract declares `event_ts` as `{"type": "long", "logicalType": "timestamp-millis"}`, and
`from_avro` **honours the logical type**, so it arrives already decoded as a TIMESTAMP. The
`/ 1000` could never have worked.

The part worth remembering is why the tests did not catch it: the fixture schema declared
`event_ts` as `LongType`, invented from the `.avsc`'s *physical* type. A test built on a
guessed schema certifies the guess, not the code. **Mirror what Spark actually produces, not
the wire format** - and the cheapest way to know is to decode one real row and print the
schema, which is what `/tmp/peek_bronze.py` existed for.

Note the CDC jobs are the mirror image: Debezium's `timestamptz` arrives as an ISO-8601
*string* because `ZonedTimestamp` has no Avro logical type. The two conventions differ on
purpose and neither is guessable.

#### `spark-submit` puts the script's directory on sys.path, not the repo root

`quality/run_gate.py` died with `ModuleNotFoundError: No module named 'quality'`. The Silver
jobs never hit it because they import nothing but pyspark. Anything that imports across
packages needs `PYTHONPATH=/opt/streamhouse` on the exec, which `make quality-gate` sets.

#### The tombstone that reached Bronze (found and fixed 2026-10-05)

Two rows in `raw_orders_cdc` had every column null - `pk`, `lsn`, `op`, all three payloads.

**`from_avro` in PERMISSIVE mode returns a struct whose fields are all null, not a null
struct.** The quarantine filter tested `envelope IS NULL`, which is false for such a row, so
it passed straight into Bronze. They were also unkillable: the MERGE predicate `t.pk = s.pk`
never matches when pk is null, so every replay inserted them again.

`classify_batch` now splits a batch into tombstones, undecodable and decodable, probing
`envelope.op` - a required field in the Debezium envelope, so a non-null `op` means the
payload genuinely decoded. The three sets are disjoint and total; a test asserts exactly that,
because the original bug was a *gap between filters*, not a wrong filter. The two bad rows
were deleted from Bronze (Delta keeps the prior version if that ever needs reversing), leaving
8387 rows and 8387 distinct `(pk, lsn)`.

### Phase 4 progress

- [x] `transform/gold_dbt/` — dbt-spark project: 6 staging views, 9 marts, 4 singular tests
- [x] `ingestion/reference_data.py` — Nager.Date holidays + Open-Meteo weather
- [x] `transform/gold_dbt/seeds/india_holidays.csv` — because Nager does not cover India
- [x] `tests/spark/test_reference_data.py` — 8 tests
- [x] dbt in the Spark image, with its own `SPARK_CONF_DIR` and a Derby metastore on a volume
- [x] `make reference-data`, `dbt-build`, `dbt-run`, `dbt-test`, `dbt-docs`

**Phase 4 is done when:** `dbt build` is green and docs generate. Met.

**VERIFIED 2026-10-06:**

```
dbt build : Completed successfully. PASS=74 WARN=0 ERROR=0 SKIP=0 TOTAL=74
dbt docs  : Catalog written to /opt/dbt-target/target/catalog.json

row counts   dim_date 15 | dim_customer 500 | dim_restaurant 118 | dim_rider 161
             dim_menu_item 625 | dim_weather 24 | fact_delivery 2133
             fact_order_item 6380 | agg_sla_daily 80

point-in-time joins actually matched, not merely built:
  date_sk       2133/2133 (100%)      customer_sk  2133/2133 (100%)
  restaurant_sk 2133/2133 (100%)      weather_sk   2133/2133 (100%)
  rider_sk      1898/2133  (89%)  <-  exactly the orders that have a rider: 1898/1898
  orders appearing more than once: 0   <- no fan-out

dim_date: 15 days, 1 holiday (Gandhi Jayanti, from the seed), WEEKDAY 10 / WEEKEND 4
weather : CLEAR 15 | RAIN 7 | HEAVY_RAIN 2   <- real Open-Meteo data, 8 cities
```

`fact_delivery` is 2133, one fewer than `fact_order_state`'s 2134: the deleted order is
excluded by `where not is_deleted`.

#### How dbt reaches path-based Delta tables

Silver and Bronze have **no catalog** - the streaming jobs write to `s3a://` paths on purpose,
so they need no metastore. dbt's `source()` needs a named relation, so:

- `register_external_sources` runs `on-run-start` and registers each path as an **external**
  table. External matters: dbt can never delete data the Spark jobs own.
- The metastore is **Derby on a named volume**, configured in a dbt-only `SPARK_CONF_DIR`
  (`/opt/dbt-conf`). Not in the default conf, because switching the whole image to a Hive
  catalog would make every streaming job open a metastore connection at startup for tables
  none of them reference - a new way for ingestion to fail, bought for nothing.
- Derby rather than Postgres because Derby needs no credentials, and a committed conf file
  with a password would breach the secrets rule.

#### Four things that cost time in Phase 4

- **A root-owned volume makes dbt exit 2 in total silence.** A fresh named volume inherits the
  mode of the image directory it covers. `/opt/metastore` was created world-writable but
  `/opt/dbt-target` was not, so dbt could not open its own log file - and therefore could not
  report that it could not open its own log file. No stdout, no stderr, exit 2.
- **`SELECT * EXCEPT (col)` is a Databricks extension.** Open-source Spark 3.5 rejects it. The
  helper column rides along instead and the callers select named columns.
- **dbt's default schema naming concatenates.** `gold` + custom schema `gold` gave
  `gold_gold.dim_date`. Overridden in `macros/generate_schema_name.sql`.
- **ruff's py311 target broke a Python 3.8 module.** `UP006` rewrote module-level
  `Tuple[date, str, str]` aliases to builtin `tuple[...]`, which is evaluated at import and
  raises `TypeError: 'type' object is not subscriptable` on 3.8. Annotations are safe because
  `from __future__ import annotations` keeps them as strings - **a type alias is not an
  annotation**. Anything under `ingestion/`, `transform/silver/` or `quality/` must therefore
  avoid runtime-evaluated generics entirely, not merely avoid writing them by hand.

#### Nager.Date does not cover India

It is absent from `/AvailableCountries`, and `/PublicHolidays/2026/IN` answers **204 No
Content** - not an error, the API saying it has nothing. The integration is real and works for
countries it does cover (verified: US 2026 returns 200). Indian holidays come from the
`india_holidays` dbt seed instead, and `dim_date` unions both sources.

#### `price_variance_inr` is non-zero, and that is not a bug

`fact_order_item` compares the price charged on the line against the menu price in force at
that moment. Some lines differ, because the menu price history from before 2026-10-05 was lost
to Kafka's 7-day retention: `scd2_effective_from` backdates the earliest *observed* version
over orders that were charged a different price. Real variance, known cause.

#### Known noise: HiveAlterHandler

`ERROR HiveAlterHandler: Failed to alter table ...` appears during a dbt build. It is Hive
failing to update table statistics for a Delta table it does not fully understand. The build
completes, every test passes. Do not chase it.

### Phase 5 progress

- [x] `orchestration` Compose profile — Airflow 3.0.1 on its own `postgres-meta`
- [x] `infra/airflow/Dockerfile` — Airflow + a pinned static Docker CLI
- [x] `orchestration/dags/streamhouse_common.py` — the command builders, written once
- [x] `streaming_health`, `silver_to_gold`, `api_extracts`, `backfill` — 4 DAGs, 0 import errors
- [x] `quality/freshness_check.py`, `quality/weather_coverage.py`, `quality/backfill_check.py`
- [x] `agg_sla_daily` made incremental, `insert_overwrite` over `order_date`
- [x] ADR-0011 — why Airflow drives Spark through the Docker socket
- [x] `make airflow-up / airflow-dags / airflow-trigger / backfill / freshness / weather-coverage`

**Phase 5 is done when:** deleting a day of Gold and re-running the backfill produces
bit-identical output. **Met and proven, 2026-10-06** - the `backfill` DAG asserts it rather
than leaving it to be checked by hand. All six tasks green:

```
before : CHECKSUM table=agg_sla_daily date=2026-09-28 rows=80 hash=153839121457
DELETED agg_sla_daily 2026-09-28; rows now: 0        <- the day was genuinely destroyed
rebuild: OK created sql incremental model gold.agg_sla_daily
after  : CHECKSUM table=agg_sla_daily date=2026-09-28 rows=80 hash=153839121457
```

The checksum is over row **content** - each row rendered to JSON, crc32'd, summed so the
result is independent of row order. "The same number of rows" is a test a wrong rebuild passes
easily.

All four DAGs were run to completion, not merely parsed:

```
backfill          success   6/6 tasks   (the bit-identical proof above)
streaming_health  success   3/3 tasks   FRESHNESS age_hours=12.93 limit=24.00
api_extracts      success  10/10 tasks  8 mapped city checks, 2 at a time
silver_to_gold    success  14/14 tasks  5 Silver + 5 gates + reference + dbt build/docs + maintain
```

#### Airflow 3 is not Airflow 2, in three ways that bite immediately

- **`airflow users create` is gone.** User management belongs to the auth manager now, and the
  default is SimpleAuthManager. The compose file sets `SIMPLE_AUTH_MANAGER_ALL_ADMINS`, so the
  local UI needs no login at all. The first init container ran, printed the CLI help, and
  "succeeded" - a command that does not exist fails in a way that looks like a usage error.
- **SLAs were removed.** `sla` and the SLA-miss callback are gone; deadline alerts (AIP-86)
  are not in 3.0.1. Lateness is enforced with `execution_timeout` plus `on_failure_callback`,
  which fires on overrun but not on a task that never started - so `streaming_health` covers
  the second case by asking whether data is still arriving.
- **The DAG processor is a separate component.** Scheduler, api-server and dag-processor each
  run as their own container.

Also: `airflow dags list-runs` takes the dag id **positionally** in 3.0.1, not via `-d`.

#### Never embed a Spark script inside a DAG

The first `api_extracts` piped an inline Python script to `spark-submit /dev/stdin`. It
produced no output at all: the task failed in two seconds with empty stdout, empty stderr and
`no cities found` - which says nothing about why. Every check now calls a real script in the
repo (`freshness_check.py`, `weather_coverage.py`, `reference_data.py --print-cities`), which
is testable, runnable by hand, and fails legibly. The DAGs contain no Python beyond glue.

#### How Airflow starts Spark work, and what it costs

`docker exec` against the Spark container, issuing exactly the command the Makefile issues, so
a red task is a command that can be pasted into a terminal and there is one definition of how
a job launches. The cost is the Docker socket mounted into the Airflow containers - effectively
host root. Acceptable for a single-user local stack, and **the first thing to change if this is
ever deployed anywhere shared**. Argued in full in ADR-0011; Phase 7 must record it as the
component with no direct cloud equivalent.

The containers join the socket's group via `SH_DOCKER_GID` in `.env` (default 1001); it is
machine-specific - `stat -c %g /var/run/docker.sock`.

#### The gate blocked the pipeline, correctly - and the fix belonged at the source

`silver_to_gold`'s first full run **failed**, at `quality_gate[fact_order_state]`. That was the
design working: the two hand-made rows from earlier CDC testing were still quarantined, the
gate exited 1, and `dbt_build` never ran on data that disagrees with itself.

It did mean the pipeline could never complete, which is a real operational question rather
than a cosmetic one. Two separate problems hid behind one failure:

- **Order 15765 was deleted**, so there is nothing left to repair it against. Silver keeps a
  tombstoned order's last known state as history, and holding history to the same standard as
  live data means one deleted order blocks every run for good - a worse failure than the one
  the rule guards. `rider_assigned_once_picked_up` now exempts `is_deleted` rows.
- **Order 16428 was genuinely wrong** and still present, so it was fixed **in Postgres**, not
  in Silver. Editing Silver would have been overwritten by the next run; fixing the source let
  the correction travel the whole pipeline, which is also the best end-to-end demonstration it
  has had:

```
UPDATE 1 (Postgres)  ->  batch 9: 1 row into Bronze  ->  batch 1: 1 order into Silver
gate: rows 2134, passed 2134, quarantined 0
```

`docker exec` **without `-i`** does not attach stdin, so a heredoc piped into `psql` is
silently a no-op that exits 0. Two repair attempts "succeeded" while changing nothing before
that was spotted.

#### Mapped tasks must be bounded by the cluster, not by Airflow

`api_extracts` maps one task per city, and the first run expanded to **eight concurrent
spark-submits**. Each is a JVM driver holding a few hundred MB, so the WSL VM went from
comfortable to 106 MiB available and the Docker daemon started answering Internal Server
Error - the same wedge as the Phase 3 outage, this time self-inflicted.

The sharp edge: the cluster has **2 cores and every job pins 1** (`spark.cores.max`), so a
third concurrent task cannot execute anyway - it queues *inside Spark* while still holding its
driver's memory. Concurrency past the core count is pure cost. Both DAGs now carry
`max_active_tasks=2`, and the mapped task `max_active_tis_per_dag=2`.

**When running the whole stack, do not also run `make test-spark`.** Core stack + Airflow +
mapped Spark drivers + the test suite's own local sessions do not fit in 9.7 GB together.

#### A bind mount creates its host directory as root

`orchestration/dags` did not exist when the Airflow services were first brought up, so Docker
created it - owned by root, unwritable by the WSL user. With no sudo available, the fix was a
throwaway container: `docker run --rm -v ~/streamhouse/orchestration:/x alpine chown -R ...`.
Create a directory before mounting it, or own it afterwards.

### Chaos and the dedup key (Phase 1)

- **A duplicate is the same WAL record delivered twice, not the same write repeated.**
  Re-running an `UPDATE` produces a genuinely new change with its own LSN, and dedup that
  rejected it would silently drop real history. The identity of a change is
  `(table, pk, lsn)` — `chaos.DedupLedger` keys on exactly that, and Phase 3 expresses the
  same key as a Delta `MERGE` predicate.
- **`pg_current_wal_lsn()` is NOT a per-change LSN.** Measured: six writes inside one
  transaction returned **two** distinct values. It reports the WAL insert pointer. Keying on
  it would reject legitimate changes. Per-change LSNs come from logical decoding only.
- **`pg_logical_slot_peek_changes` is non-destructive**, so calling it twice returns the same
  changes with the same LSNs. That *is* a restarted Debezium connector replaying an
  un-advanced slot — a faithful reproduction, not a simulation. `repository.LogicalSlot`
  wraps it; it is a Phase 1 test harness, **not** CDC ingestion, which is Phase 2.
- **Always drop the slot.** `LogicalSlot` is a context manager for that reason. An inactive
  slot retains WAL forever and fills the source disk — ADR-0001's named hazard.
- **`--chaos duplicates` reports 0 from the CLI, by design.** The generator writes; it does
  not deliver. There is no delivery path until Phase 2, and the CLI says so on stderr rather
  than printing a silent zero.
- Five scenarios remain unimplemented and are rejected **by name with their phase**, so a
  typo and a not-yet-built scenario produce different errors.

### CLI performance traps

- **`--orders N` drains every in-flight order before returning**, and `RealClock` genuinely
  sleeps. At the default 20x a lifecycle is ~90s, so `--orders 15` took 162s in a test.
  Bound it with `--duration`, or raise `--speed` (500x makes a lifecycle ~3.6s). This cut the
  CLI integration suite from 347s to 31s.

### Integration test conventions

- Fixtures clear **dependent tables first** (`order_items`, `payments`, `orders`), then the
  reference tables, all inside a transaction that is rolled back.
- An earlier fixture *skipped* when orders existed. After a committed demo run that turned into
  eight silently skipped tests — and a skip reads as success. Do not reintroduce that guard.
- The suite takes ~46s. Most of it is per-statement latency across the Docker/Windows boundary,
  which is the `E:\` problem already recorded above. Keep order counts small.

### KNOWN BLOCKER — Airflow cannot be installed on native Windows (Phase 5)

**Deferred deliberately. Do not attempt to solve this before Phase 5.**

`apache-airflow==3.0.1` is not reliably installable on native Windows; it expects a POSIX
environment. Phase 5 is the first phase that needs it. When that phase starts, the options are
to run Airflow in a container from the `orchestration` Compose profile (the intended route, and
what `infra/docker-compose.yml` is already shaped for), or to move development into WSL2 — which
the repo is scheduled to do before Phase 2 anyway, for the file-I/O reason recorded above.

`.venv` therefore holds only what each phase actually needs, installed incrementally rather than
via `pip install -r requirements.txt`. Currently installed: `pytest`, `pytest-cov`, `ruff`,
`mypy`, `faker`, `psycopg[binary]`, `pydantic`. Still needed for the rest of Phase 1:
`confluent-kafka` and `fastavro`, both of which install on Windows without trouble.

### Repo location - moved into WSL2 on 2026-09-24

The repo lives at `/home/sonam/streamhouse` in the **Ubuntu** WSL2 distro. Work there, not on
`E:\`. Docker Desktop's WSL2 integration for Ubuntu was already enabled; **no Docker
reconfiguration was needed**.

**Why, with the measurement rather than the folklore.** The spec (and three earlier notes in
this file) claimed Docker *volumes* had to be moved into WSL2. That was wrong for this setup:
`docker volume inspect` showed them already at `/var/lib/docker/volumes/...` inside the
`docker-desktop` distro, which is what the WSL2 backend does automatically. There was exactly
one Windows bind mount - `./postgres/init`, a few KB read once at first boot.

The real cost is **Docker Desktop's port proxy**, measured at 150 TCP connects to :5432:

```
from WINDOWS : 5.49 ms/connect
from WSL2    : 0.69 ms/connect      8x
```

End to end, the full suite went **90.5s on Windows to 49.7s in WSL2** - same 329 tests.

**Copying the repo across is not enough.** A `tar` copy carried NTFS stat data into
`.git/index`, and `git status` then reported all 23 tracked text files as modified even
though `git diff` was empty and the blob hashes were identical (`765c436` == `765c436`).
Neither `--refresh` nor `--really-refresh` could reconcile it. The fix was to discard the
copied `.git` and `git clone` natively inside WSL2, which builds a correct index. If this
repo is ever relocated again, clone it - do not copy it.

Windows-side `core.autocrlf=true` also means its working tree holds CRLF while the committed
blobs are LF; a byte copy therefore looks modified on Linux for that reason too.

### Published Docker ports are NOT reachable from inside WSL2

Docker Desktop forwards published ports to the **Windows** host only. From inside the Ubuntu
distro - which is where this repo lives and where every `make` runs - `localhost:8081`,
`:8083`, `:9000` and `:8090` are all unreachable. Verified 2026-10-05 against a fully healthy
daemon, after first mistaking it for a symptom of the outage below:

```
from Windows : 8081 -> 200    8083 -> 404
from WSL2    : 8081 -> 000    8083 -> 000     (could not connect)
```

Every admin HTTP call in the Makefile therefore runs **from a container on the compose
network**, addressing services by compose name — the `INNET` variable, which carries curl in
the `connect` container because the Debezium image ships one. `make health`,
`connector-register` and `connector-status` all work from WSL2 this way, and the targets no
longer depend on which ports happen to be published.

The browser UIs are a different matter: you open those from Windows, where the published
ports do work. The URLs `make health` prints are correct for a browser and wrong for curl
inside the distro.

The Spark jobs were never affected - they run inside the network already and address
`redpanda:8081` and `minio:9000` directly.

### When Docker stops responding: look at host memory first

On 2026-10-05 the daemon wedged mid-session. `docker ps` hung from **both** Windows and WSL2,
and connections were accepted and then never answered - a far more confusing symptom than a
refusal, and it masqueraded as a networking fault for a while.

Cause was host memory. Windows had **1.1 GB free of 15.6 GB** (Chrome 3.4 GB across 40
processes, PyCharm 1.4 GB), so it trimmed the WSL VM's working set to 1.19 GB while the stack
needs about 6. `.wslconfig` allots the VM `memory=10GB, processors=4`, but that is a ceiling,
not a reservation - Windows reclaims under pressure regardless.

Recovery is to restart Docker Desktop; containers carry `restart: unless-stopped` and all
state is in named volumes, so nothing is lost:

```powershell
Get-Process "Docker Desktop","com.docker.backend" | Stop-Process -Force
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
```

Then wait for it rather than polling blind: `until docker ps >/dev/null 2>&1; do sleep 5; done`.

**Close the browser before a long Spark run.** The stack alone is ~6 GB and a `spark-submit`
driver adds several hundred MB more.

---

## Phase plan

Each phase ends in something demoable. Never leave the repo in a broken state.

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Foundation — repo skeleton, core Compose profile, Postgres DDL + logical replication, ADR-0001, runbook | **DONE, verified 2026-09-24** |
| 1 | Source simulation — OLTP generator (Faker + order state machine), GPS producer, `--chaos` flag | **DONE, verified 2026-09-24** |
| 2 | CDC ingestion + contracts — Debezium connector, Avro schemas registered `BACKWARD`, Bronze streaming, DLQ, exactly-once | **DONE, verified 2026-10-05** |
| 3 | Silver — SCD2 via Delta `MERGE`, dedup on `(pk, lsn)`, GPS sessionization, quality gate, `OPTIMIZE`/`ZORDER` | **DONE, verified 2026-10-06** |
| 4 | Gold — dbt star schema, `dim_date` from Nager.Date, Open-Meteo join, generic + singular tests, docs | **DONE, verified 2026-10-06** |
| 5 | Orchestration — Airflow 3 DAGs, dynamic task mapping, idempotent backfills, SLA callbacks | **DONE, verified 2026-10-06** |
| 6 | Observability — OpenLineage → Marquez, Grafana SLO dashboard, Prometheus alert rules, runbook | Not started |
| 7 | **Portability & IaC (validate-only, no cloud spend)** — Terraform for the cloud-equivalent mapping as a design exercise (`validate`/`plan` only, never `apply`); prove storage portability locally by swapping MinIO → local filesystem or a second MinIO via config alone; ADR recording the local→cloud mapping | Not started |
| 8 | Stretch — Iceberg comparison, Dagster port, streaming Gold, contract CI gate, cost model | Not started |

---

## Repo layout

```
streamhouse/
├── CLAUDE.md          this file — state of the project, read first
├── README.md          reader-facing overview
├── Makefile           make up / down / clean / db-init / health / logs
├── requirements.txt   pinned, Python 3.11
├── .env.example       copy to .env; .env is never committed
├── docs/
│   ├── architecture.md    trimmed design spec — the source of truth for design
│   └── decisions/         ADRs — one per real fork in the road
├── infra/
│   ├── docker-compose.yml  the stack; connect and spark-master are built, not pulled
│   ├── connect/       Dockerfile — Debezium + Confluent Avro converters
│   ├── connectors/    connector definitions, registered by `make connector-register`
│   ├── postgres/      init DDL, applied on first boot and by `make db-init`
│   └── spark/         Dockerfile + spark-defaults.conf — Delta, S3A, Kafka, test tooling
├── contracts/         Avro schemas — the source of truth for shape
├── generator/         synthetic OLTP + GPS producers, chaos injection, registry client
├── ingestion/         Spark Structured Streaming jobs — Python 3.8, run via spark-submit
├── transform/
│   ├── silver/        PySpark: SCD2 MERGE, dedup, sessionization
│   └── gold_dbt/      dbt project: staging, marts, tests, macros, seeds
├── quality/           Great Expectations suites and checkpoints
├── orchestration/     Airflow DAGs
├── serving/           FastAPI metrics service, Streamlit dashboard
└── tests/
    ├── unit/          pure logic, host venv, no containers
    ├── spark/         DataFrame transforms — run in the Spark image via `make test-spark`
    ├── integration/   testcontainers — real Postgres + Kafka
    └── chaos/         one test per chaos scenario
```

---

## Conventions

**Python**
- Target **3.11** on the host. Anything under `ingestion/` (and from Phase 3, `transform/silver/`)
  runs in the Spark image on **3.8.10** — no `StrEnum`, no `slots=True`, no `match`. See ADR-0009.
- Create the venv with `python3.11 -m venv .venv`. Ubuntu's own `python3` is 3.14 and unusable here.
- Pin every dependency in `requirements.txt`. An unpinned portfolio repo stops building within a year.
- Type hints on all function signatures. `mypy` must pass.

**Lint and format**
- `ruff check` and `ruff format --check` — both must pass.
- Line length 100.
- Run via `pre-commit` locally; enforced in CI from Phase 1.
- **Run them before committing, not after.** Phase 2 was pushed with 51 ruff and 31 mypy
  findings and they had to be cleaned up retroactively on 2026-10-05.

**Tests**
- `pytest`. Unit tests need no containers and must stay fast.
- DataFrame equality via `chispa`, never manual `collect()` comparison.
- Integration tests use `testcontainers` (real Postgres + Kafka), never mocks of infrastructure.
- Spark tests run in the image: `make test-spark`. They are marked `spark` and are skipped on
  the host, which has no JVM.
- **A phase is not done until its own code is tested.** Phase 2 shipped `ingestion/` with no
  tests at all; that gap was only closed afterwards.
- Target 80% coverage on `transform/` once that code exists.

**SQL and data**
- All money in INR, suffixed `_inr`. All timestamps UTC, suffixed `_ts`.
- Surrogate keys suffixed `_sk`; natural keys keep their source name.
- Every dbt model documents its **grain** in a one-line statement. Interviewers look for this.

**Docs**
- One ADR per real architectural fork: context, options, decision, consequences. ~200 words.
- ADRs are immutable once accepted — supersede, never edit.

**Secrets**
- Never commit `.env`. `.env.example` holds placeholder values only, never real ones.
- No credentials in compose files, DDL, or code — read from environment.

---

## Services (core profile)

Brought up by `make up` / `docker compose --profile core up -d`. ~6 GB RAM.

| Service | Image | Ports | Purpose |
| --- | --- | --- | --- |
| postgres | `postgres:16` | 5432 | OLTP source, `wal_level=logical` |
| redpanda | `redpandadata/redpanda` | 9092, 8081 | Kafka API + built-in schema registry |
| redpanda-console | `redpandadata/console` | 8080 | Topic/schema browser |
| connect | `debezium/connect:2.7` | 8083 | Kafka Connect — **idle in Phase 0**, no connector registered |
| minio | `minio/minio` | 9000, 9001 | S3A-compatible object store for Delta |
| spark-master / spark-worker | `bitnami/spark:3.5` | 7077, 8090 | **Idle in Phase 0**, no jobs submitted |

`connect` and `spark` are present so the stack is complete and healthy, but nothing is submitted to
them until Phase 2. Bringing them up is not phase-skipping; using them is.
