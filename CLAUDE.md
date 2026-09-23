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

**Never run any git command.** Not `init`, not `add`, not `commit`, not `status`, not `log`, not
`diff`. Create and edit files on disk and stop there. The user reviews and commits manually.

### 4. Ask before creating, modifying, or running

State what you are about to do and wait for go-ahead. Do not execute a batch of actions silently.
Show each file or group of files after creating it, then wait before continuing.

---

## Current state

| | |
| --- | --- |
| **Active phase** | **Phase 0 — Foundation** |
| Repo root | `E:\streamhouse-project\streamhouse\` |
| Git | Not initialised. User handles all git manually. |
| Python 3.11 | Installed at `C:\Users\erson\AppData\Local\Programs\Python\Python311\python.exe`. `py` defaults to 3.13 — always invoke `py -3.11` explicitly. |
| venv | Not created. Not needed until Phase 1 — all Phase 0 services are Docker-based. |
| Docker | Running on the host. |

### Phase 0 progress

- [x] `CLAUDE.md`
- [x] `docs/architecture.md` — trimmed design spec
- [ ] Folder skeleton with per-folder README stubs
- [ ] `docs/decisions/` — ADR template + ADR-0001 (log-based vs query-based CDC)
- [ ] `infra/docker-compose.yml` — core profile with healthchecks
- [ ] Postgres DDL + logical replication, verifiable via `SHOW wal_level;`
- [ ] `.env.example`, `Makefile`, `requirements.txt`, `.gitignore`
- [ ] `README.md` — strip Azure section, retitle Phase 7 (carried over from a prior session)

**Phase 0 is done when:** `make up` brings up a healthy core stack and
`SELECT * FROM pg_replication_slots;` works against the Postgres container.

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
| 0 | Foundation — repo skeleton, core Compose profile, Postgres DDL + logical replication, ADR-0001 | **ACTIVE** |
| 1 | Source simulation — OLTP generator (Faker + order state machine), GPS producer, `--chaos` flag | Not started |
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
