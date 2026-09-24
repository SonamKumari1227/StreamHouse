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
| **Active phase** | **Phase 1 — Source simulation.** `state_machine.py`, `config.py`, `seed.py`, `repository.py`, `oltp_generator.py` done and verified against real Postgres. Next: `gps_producer.py`. |
| Repo root | `E:\streamhouse-project\streamhouse\` |
| Git | Initialised. Remote `origin` → `https://github.com/SonamKumari1227/StreamHouse.git`, branch `master`. Last commit `c0753c5`. **Everything from item 3 onward is uncommitted** — `docs/decisions/`, `docs/runbook.md`, `infra/docker-compose.yml`, `infra/postgres/`, `Makefile`, `.env.example`, `requirements.txt`, plus edits to `CLAUDE.md`, `README.md`, `docs/README.md`, `docs/architecture.md`. **User handles all staging, commits and pushes manually.** |
| IDE | PyCharm — `.idea/` present and already gitignored. |
| Python 3.11 | Installed at `C:\Users\erson\AppData\Local\Programs\Python\Python311\python.exe`. `py` defaults to 3.13 — always invoke `py -3.11` explicitly. |
| venv | `.venv` (Python 3.11.9). Deps installed **per phase**, not from `requirements.txt` wholesale — see the Airflow blocker below. |
| Docker | 27.5.1, Compose v2.32.4. Core stack verified healthy. |
| GNU Make | 3.81, at `C:\Program Files (x86)\GnuWin32\bin\make.exe`. On the **Windows user PATH** since 2026-09-24, so every newly started process resolves `make` — terminals, PyCharm, PowerShell, Claude Code sessions. Also in `~/.bashrc`. No export prefix needed. |

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
- [ ] `generator/gps_producer.py` — Avro pings to Redpanda
- [ ] `generator/chaos.py` — named composable scenarios
- [ ] `generator/__main__.py` — CLI

**Verified 2026-09-24**, actual output:

```
ruff check           : All checks passed!
ruff format --check  : 12 files already formatted
mypy (strict)        : Success: no issues found in 12 source files
pytest               : 154 passed, 23 deselected      (unit only)
pytest -m integration: 23 passed, 154 deselected in 46s (real Postgres)
full suite           : 177 passed in 58s
coverage             : generator/  693 stmts, 0 miss, 100%
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

### Known deviation from spec

The repo lives on `E:\`, a Windows path. Docker Desktop file I/O across the Windows↔WSL2 boundary
is roughly an order of magnitude slower and will make Spark appear broken in Phases 2–3 when it is
not. **Decision: stay on `E:\` through Phase 1; move the repo and Docker volumes into WSL2 before
starting Phase 2.** Do not silently "fix" this by relocating anything.

---

## Phase plan

Each phase ends in something demoable. Never leave the repo in a broken state.

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Foundation — repo skeleton, core Compose profile, Postgres DDL + logical replication, ADR-0001, runbook | **DONE, verified 2026-09-24** |
| 1 | Source simulation — OLTP generator (Faker + order state machine), GPS producer, `--chaos` flag | **ACTIVE** — OLTP generator done; GPS producer and chaos remain |
| 2 | CDC ingestion + contracts — Debezium connector, Avro schemas registered `BACKWARD`, Bronze streaming, DLQ, exactly-once | Not started |
| 3 | Silver — SCD2 via Delta `MERGE`, dedup on `(pk, lsn)`, GPS sessionization, GE gate, `OPTIMIZE`/`ZORDER` | Not started |
| 4 | Gold — dbt star schema, `dim_date` from Nager.Date, Open-Meteo join, generic + singular tests, docs | Not started |
| 5 | Orchestration — Airflow 3 DAGs, dynamic task mapping, idempotent backfills, SLA callbacks | Not started |
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
├── infra/             docker-compose, connector configs, prometheus/grafana config
├── contracts/         Avro schemas — the source of truth for shape
├── generator/         synthetic OLTP + GPS producers, chaos injection
├── ingestion/         Spark Structured Streaming jobs, API extractors
├── transform/
│   ├── silver/        PySpark: SCD2 MERGE, dedup, sessionization
│   └── gold_dbt/      dbt project: staging, marts, tests, macros, seeds
├── quality/           Great Expectations suites and checkpoints
├── orchestration/     Airflow DAGs
├── serving/           FastAPI metrics service, Streamlit dashboard
└── tests/
    ├── unit/          pure transform logic, chispa DataFrame assertions
    ├── integration/   testcontainers — real Postgres + Kafka
    └── chaos/         one test per chaos scenario
```

---

## Conventions

**Python**
- Target **3.11 only**. PySpark 3.5.x supports 3.8–3.11; 3.13 will not work.
- Create the venv with `py -3.11 -m venv .venv` (Phase 1, not needed yet).
- Pin every dependency in `requirements.txt`. An unpinned portfolio repo stops building within a year.
- Type hints on all function signatures. `mypy` must pass.

**Lint and format**
- `ruff check` and `ruff format --check` — both must pass.
- Line length 100.
- Run via `pre-commit` locally; enforced in CI from Phase 1.

**Tests**
- `pytest`. Unit tests need no containers and must stay fast.
- DataFrame equality via `chispa`, never manual `collect()` comparison.
- Integration tests use `testcontainers` (real Postgres + Kafka), never mocks of infrastructure.
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
