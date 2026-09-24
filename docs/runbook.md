# Runbook

How to run StreamHouse locally, verify it is healthy, and fix it when it is not.

> **This is a living document.** It currently covers Phase 0 — bringing up the core stack and proving
> Postgres logical replication works. Each later phase adds its own section: streaming operations in
> Phase 2, backfills in Phase 5, and one entry per configured alert in Phase 6.

> **Nothing here needs a cloud account.** No Azure, AWS, or GCP subscription, credentials, or CLI.
> Everything runs in Docker on your machine. The only external calls in the whole project are to two
> free public APIs (Open-Meteo, Nager.Date), and neither is used before Phase 4.

---

## Status — what actually works today

Phase 0 is in progress. This section is the honest inventory; update it as items land.

| Capability | State |
| --- | --- |
| Repo skeleton, docs, ADRs | ✅ Done |
| `infra/docker-compose.yml` (core profile) | ⬜ Phase 0, item 4 |
| Postgres DDL + logical replication | ⬜ Phase 0, item 5 |
| `Makefile`, `.env.example`, `requirements.txt` | ⬜ Phase 0, item 6 |
| Everything from Phase 1 onward | ⬜ Not started |

**Until items 4–6 land, the commands below will not run.** They describe the target state of Phase 0
and are written first on purpose — the documentation defines what the Makefile has to deliver.

---

## 1. Prerequisites

| Requirement | Minimum | Check |
| --- | --- | --- |
| Docker Engine | 24+ | `docker --version` |
| Docker Compose | v2 | `docker compose version` |
| Docker resources | **8 GB RAM, 4 CPUs** | Docker Desktop → Settings → Resources |
| Free disk | ~40 GB | |
| Python 3.11 | 3.11.x | `py -3.11 --version` |

**Python is not needed for Phase 0.** Postgres, Redpanda, Kafka Connect, MinIO and Spark all run in
containers. The virtualenv first matters in Phase 1, when the data generator runs on the host.

When you do create it, use 3.11 explicitly — `py` defaults to 3.13 on this machine, and PySpark 3.5.x
does not support it:

```bash
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1        # PowerShell
pip install -r requirements.txt
```

---

## 2. From a clean checkout to a healthy stack

```bash
git clone https://github.com/SonamKumari1227/StreamHouse.git
cd StreamHouse

cp .env.example .env                # then edit: Postgres password, MinIO keys
make up                             # docker compose --profile core up -d
make db-init                        # apply DDL, enable logical replication
make health                         # verify every service
```

`make up` starts the **core** profile only — Postgres, Redpanda, Redpanda Console, Kafka Connect,
MinIO, Spark master and worker. Roughly 6 GB of RAM. The `orchestration` and `observability` profiles
are not used until Phases 5 and 6.

If you prefer to drive Compose directly, every Makefile target is a thin wrapper:

```bash
docker compose -f infra/docker-compose.yml --profile core up -d
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml logs -f postgres
```

**Kafka Connect and Spark come up idle in Phase 0.** No connector is registered and no job is
submitted until Phase 2. They are running so the stack is complete and its health is meaningful,
not because anything is using them yet.

---

## 3. Verifying each service

`make health` runs all of these. Run them individually when something looks wrong.

### Everything at once

```bash
docker compose -f infra/docker-compose.yml ps
```

Every service should report `healthy`. A service stuck in `starting` for more than ~90 seconds has a
problem — go to its logs.

### Postgres — the important one

```bash
# reachable?
docker compose -f infra/docker-compose.yml exec postgres pg_isready -U streamhouse

# THE Phase 0 check — must print: logical
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U streamhouse -d streamhouse -c "SHOW wal_level;"

# replication capacity
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U streamhouse -d streamhouse -c "SHOW max_replication_slots; SHOW max_wal_senders;"

# slots — empty in Phase 0, populated once Debezium registers in Phase 2
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U streamhouse -d streamhouse -c "SELECT * FROM pg_replication_slots;"

# tables created?
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U streamhouse -d streamhouse -c "\dt"

# before-images enabled on orders? expect relreplident = f
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U streamhouse -d streamhouse \
  -c "SELECT relname, relreplident FROM pg_class WHERE relname='orders';"
```

`wal_level` returning `logical` is the single condition Phase 0 exists to satisfy. If it returns
`replica`, see §4.1.

### Redpanda

```bash
# broker health
docker compose -f infra/docker-compose.yml exec redpanda rpk cluster health

# topics — empty in Phase 0
docker compose -f infra/docker-compose.yml exec redpanda rpk topic list

# built-in schema registry — expect [] in Phase 0
curl -s localhost:8081/subjects
```

### Redpanda Console

Open <http://localhost:8080>. It should load and show the broker with zero topics. This is how you
will inspect CDC topics and registered schemas from Phase 2 onward.

### Kafka Connect

```bash
# expect a version banner
curl -s localhost:8083/ | jq

# expect [] — no connector is registered until Phase 2
curl -s localhost:8083/connectors | jq

# the Postgres connector plugin should be present and ready for Phase 2
curl -s localhost:8083/connector-plugins | jq '.[].class'
```

### MinIO

```bash
curl -s localhost:9000/minio/health/live      # expect HTTP 200, empty body
```

Console at <http://localhost:9001>, credentials from `.env`. Buckets are created in Phase 2; an empty
console in Phase 0 is correct.

### Spark

Master UI at <http://localhost:8090>. It should show one live worker and zero running applications.
Zero applications is correct — no job is submitted until Phase 2.

---

## 4. Common Phase 0 failures

### 4.1 `SHOW wal_level;` returns `replica` instead of `logical`

The single most common Phase 0 problem. `wal_level` cannot be changed at runtime — it requires a
**restart**, not a reload, and `ALTER SYSTEM` alone will not do it.

```bash
# confirm the setting was written
docker compose -f infra/docker-compose.yml exec postgres \
  psql -U streamhouse -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name='wal_level';"

# restart the container and re-check
docker compose -f infra/docker-compose.yml restart postgres
```

If it is still `replica`, the container is not picking up the config. Check that the compose file
passes `-c wal_level=logical` via `command:`, or that the mounted `postgresql.conf` is actually at the
path Postgres reads. `SHOW config_file;` tells you which file is live.

### 4.2 A service never becomes `healthy`

```bash
docker compose -f infra/docker-compose.yml ps
docker compose -f infra/docker-compose.yml logs --tail=100 <service>
```

Most often one of:

- **Out of memory.** Spark and Redpanda together need real headroom. Docker Desktop → Resources →
  at least 8 GB. A container killed for OOM shows exit code 137.
- **Healthcheck starts too early.** Postgres and Redpanda need a `start_period` long enough to
  initialise; without one, Compose marks them unhealthy before they have finished booting.
- **Image still pulling.** The first `make up` pulls several GB. Watch `docker compose logs -f`.

### 4.3 Port already in use

`Error: bind: address already in use` — something else on the host holds the port. Core profile ports
are 5432, 8080, 8081, 8083, 9000, 9001, 9092, 7077 and 8090.

```powershell
netstat -ano | findstr :5432          # find the PID holding it
```

A local PostgreSQL service on 5432 is the usual culprit on Windows. Stop it, or remap the host side
of the port in the compose file — change `5432:5432` to `5433:5432` and leave the container port
alone.

### 4.4 `make db-init` fails with "database does not exist" or "role does not exist"

The Postgres container initialises its database, user and password from `.env` **only on first
start, against an empty data volume.** If you edited `.env` after the volume was created, the old
credentials are still baked in.

```bash
make clean       # destroys volumes — this is the fix
make up
make db-init
```

### 4.5 Everything is correct but painfully slow

Expected on this machine: the repo lives on `E:\`, a Windows path, and Docker Desktop file I/O across
the Windows↔WSL2 boundary is roughly an order of magnitude slower than native.

Phase 0 barely notices. **Phase 2 onward will**, and it will look like Spark is broken when it is
not. The plan of record is to move the repo and Docker volumes into the WSL2 filesystem before
starting Phase 2 — see `../CLAUDE.md` under "Known deviation". Do not work around it by tuning Spark.

### 4.6 Containers vanish after a Docker Desktop restart

Compose services are not restart-persistent unless a `restart:` policy is set. Just run `make up`
again — named volumes survive, so your data is intact.

---

## 5. Teardown

| Command | What it does | Data |
| --- | --- | --- |
| `make down` | Stops and removes containers and the network. | **Preserved.** Named volumes are untouched — Postgres data, MinIO objects and Kafka topics all survive. This is the one to use daily. |
| `make clean` | `down` plus **removes named volumes**. | **Destroyed, irreversibly.** Next `make up` starts from an empty database and you must re-run `make db-init`. |

Use `make clean` when you have changed anything that Postgres only reads on first initialisation —
credentials, database name, the init DDL — or when you want a genuinely clean reproduction of the
setup path. Otherwise `make down`.

```bash
make down       # daily
make clean      # nuclear; then make up && make db-init
```

To reclaim disk from images as well:

```bash
docker system df                  # see what is using space
docker image prune                # dangling images only; safe
```

---

## 6. Alert runbook

*Added in Phase 6.* One entry per configured Prometheus alert, each with symptom, likely causes in
order of frequency, the first three commands to run, and the recovery procedure — including which
steps are safe to repeat.

The first entry is already determined by ADR-0001: an inactive replication slot retains WAL
indefinitely and will fill the source disk. Restarting the Spark streaming job is always safe because
checkpoints guarantee exactly-once. Dropping a replication slot is never safe without a planned
re-snapshot, because the retained WAL is gone the moment you do.
