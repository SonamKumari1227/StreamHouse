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

## 4.7 Kafka Connect will not start: `cleanup.policy=delete`

**Happened 2026-09-28.** Recorded because the failure is silent, the cause is non-obvious,
and anything that wipes the Redpanda volume will reproduce it.

**Symptom.** `sh-connect` never becomes healthy and its restart count climbs without bound
(it reached **39**). `make health` prints `connectors : UNREACHABLE`. The REST port answers,
so the container does not look dead.

**Root cause.** Connect keeps its worker state in three internal topics and refuses to run
unless every one of them is log-compacted:

```
ConfigException: Topic '_connect_offsets' supplied via the 'offset.storage.topic' property
is required to have 'cleanup.policy=compact' to guarantee consistency and durability of
source connector offsets, but found the topic currently has 'cleanup.policy=delete'.
```

The herder thread throws, Connect stops, the container exits and restarts, forever. The
topics had been auto-created by the **broker** after the `redpanda-data` volume was wiped
during the librdkafka diagnosis, and broker auto-creation defaults to `cleanup.policy=delete`.
Connect creates them correctly when it gets there first; it did not.

**Actual state when found** (correcting an earlier note that called them empty):

| Topic | cleanup.policy | Records |
| --- | --- | --- |
| `_connect_configs` | `delete` | **4** - auto-generated `session-key` entries, one per hour |
| `_connect_offsets` | `delete` | 0 |
| `_connect_status` | `delete` | 0 |

The four records were Connect's own rotating HMAC session keys, not connector configuration -
no connector had ever been registered. They are disposable, and `alter-config` is
metadata-only, so nothing had to be deleted.

**Fix**

```bash
make connect-topics          # idempotent: creates them compacted, or alters them if wrong
docker compose -f infra/docker-compose.yml --env-file .env --profile core restart connect
```

`make up` now runs `connect-topics` between starting Redpanda and starting everything else,
so the ordering cannot be got wrong by accident. Running it by hand is only needed after a
volume wipe or when recovering an already-broken stack.

**Verify**

```bash
docker inspect -f '{{.State.Health.Status}} {{.RestartCount}}' sh-connect   # healthy 0
curl -sf localhost:8083/connectors                                          # []
```

**Why the healthcheck did not catch it.** It probed `/`, which the REST layer answers by
itself. It now probes `/connectors`, which has to reach the herder.

Be precise about what that change buys, though: in *this* failure the process exits, so the
container restarts and its health resets to `starting` every time - it never reported
`healthy`, and it never reached `unhealthy` either, because each crash begins a fresh 60s
`start_period`. The old check was not lying so much as saying nothing useful. The new check
covers the case the old one genuinely would have missed: a herder that is broken or stuck
while the process stays alive, where `/` keeps answering 200 indefinitely.

**Watch for this whenever** the `redpanda-data` volume is removed, the stack is rebuilt from
empty, or Connect is pointed at a new cluster.

---

## 4.8 Debezium: registering, checking and resetting the connector

### Register

```bash
make connector-register     # idempotent: creates if absent, updates if present
make connector-status       # connector and task state
```

Credentials come from `.env` via `envsubst` and are piped straight into curl, so the password
is never written to disk and never committed. The definition lives in
`infra/connectors/orders-postgres.json`.

### What each setting does

| Setting | Value | Why |
| --- | --- | --- |
| `plugin.name` | `pgoutput` | Built into Postgres 16. `decoderbufs`/`wal2json` need an extension installed. |
| `slot.name` | `streamhouse_slot` | Fixed, not generated. A random slot per restart would strand the old one, and a stranded slot retains WAL forever - ADR-0001's hazard. |
| `publication.name` | `streamhouse_pub` | Fixed for the same reason. |
| `publication.autocreate.mode` | `filtered` | Publishes only the seven included tables. The default, `all_tables`, would publish everything and needs superuser. |
| `topic.prefix` | `cdc` | Topics become `cdc.public.<table>`. See ADR-0008. |
| `snapshot.mode` | `initial` | Gives Phase 3's SCD2 dimensions a baseline row per key. Safe here at 1270 reference rows; a trap on large tables. |
| `decimal.handling.mode` | `precise` | Money as Avro decimal, not float. |
| `time.precision.mode` | `connect` | Note: `TIMESTAMPTZ` still arrives as an ISO-8601 **string**, not a timestamp type. |
| `heartbeat.interval.ms` | `10000` | Advances the slot even when the included tables are idle, so WAL is not retained by a quiet database. |

### Verify it is actually working

```bash
make connector-status        # connector RUNNING, tasks[0] RUNNING

# the slot must exist AND be active
docker compose -f infra/docker-compose.yml --env-file .env --profile core exec -T postgres \
  psql -U streamhouse -d streamhouse -c \
  "SELECT slot_name, plugin, active FROM pg_replication_slots;"

# topics appear once a table has produced at least one change
docker compose -f infra/docker-compose.yml --env-file .env --profile core exec -T redpanda \
  rpk topic list
```

An empty `cdc.public.orders` topic is not a fault when `orders` has no rows - Debezium creates
the topic on the first change, not at registration.

### Reset it

Full reset, in this order. **Order matters**: drop the connector first, or it recreates the
slot while you are deleting it.

```bash
# 1. remove the connector
curl -X DELETE localhost:8083/connectors/streamhouse-postgres

# 2. drop the replication slot (it is inactive now the connector is gone)
docker compose -f infra/docker-compose.yml --env-file .env --profile core exec -T postgres \
  psql -U streamhouse -d streamhouse -c \
  "SELECT pg_drop_replication_slot('streamhouse_slot');"

# 3. drop the publication
docker compose -f infra/docker-compose.yml --env-file .env --profile core exec -T postgres \
  psql -U streamhouse -d streamhouse -c "DROP PUBLICATION IF EXISTS streamhouse_pub;"

# 4. delete the CDC topics and their schema subjects
docker compose -f infra/docker-compose.yml --env-file .env --profile core exec -T redpanda \
  rpk topic delete cdc.public.orders cdc.public.order_items cdc.public.payments \
  cdc.public.customers cdc.public.restaurants cdc.public.menu_items cdc.public.riders \
  __debezium-heartbeat.cdc
curl -X DELETE localhost:8081/subjects/cdc.public.orders-value   # repeat per subject

# 5. register again
make connector-register
```

**Never drop the slot while the connector is running.** The WAL it was holding is released
immediately, and any change not yet read is gone - there is no recovery except a re-snapshot.

### Proven working, 2026-09-28

One INSERT and one UPDATE on `orders`, then a DELETE, produced the full lifecycle on
`cdc.public.orders`:

```
offset=0  op=c (CREATE)   before.status=None       after.status=PLACED
offset=1  op=u (UPDATE)   before.status=PLACED     after.status=DELIVERED
offset=2  op=d (DELETE)   before.status=DELIVERED  after.status=None
offset=3  op=TOMBSTONE    (null value, key retained for compaction)
```

The `before` image on the UPDATE is what `REPLICA IDENTITY FULL` buys, and it is the whole
argument of ADR-0001: query-based CDC would have shown neither the intermediate `PLACED` state
nor the delete.

---

## 4.9 Bronze CDC stream: load-test results and operating notes

Measured 2026-09-28 against 180 seconds of generator load - 2131 orders, 4116 transitions,
8384 messages on `cdc.public.orders`.

### Correctness

```
BRONZE ROWS      : 8383
DISTINCT (pk,lsn): 8383  ->  duplicates: 0
```

8384 messages, 1 tombstone skipped, 8383 rows landed. Reconciles exactly, no duplicates.

### Latency - measure it, do not assume it

```
rows   min_s   p50_s   p95_s
8383     6.6    19.0    19.4
```

**p50 is ~19 seconds, not sub-second.** The floor is the fixed cost of a Delta MERGE per
micro-batch, which the history makes plain:

| version | rows inserted | exec_ms |
| --- | --- | --- |
| 1 | 2 | 9378 |
| 2 | 1 | 8589 |
| 3 | 8377 | 3119 |

A batch of **8377 rows took less time than a batch of 1**. The cost is per-batch, not
per-row, so latency is dominated by MERGE overhead and does not improve by sending less data.
Anyone quoting "sub-minute end-to-end latency" should quote this number instead.

### Resource contention - `spark.cores.max` is mandatory

A continuous streaming query with no cap takes every core in the cluster and never gives them
back. During the load test:

```
cores total : 2 | cores used: 2
ACTIVE : bronze-cdc-orders | cores: 2 | state: RUNNING
ACTIVE : probe             | cores: 0 | state: WAITING    <- starved indefinitely
```

`make stream-bronze` now passes `--conf spark.cores.max=1` (override with `STREAM_CORES=`).
Verified afterwards: a second job ran alongside the stream instead of waiting. **Phase 5's
Airflow-triggered batch jobs would have starved in exactly this way**, and the symptom - a job
that simply never starts, with no error - is unpleasant to diagnose.

### Small files

Not yet a problem: each MERGE added a single file, and there are four versions. The risk
appears with frequent small batches, so revisit when the stream runs continuously under
steady load rather than in bursts. `OPTIMIZE` and a `VACUUM` retention policy are Phase 3.

### Running it

```bash
make stream-bronze                    # continuous, orders, 1 core
make stream-bronze ONCE=1             # drain what is available and stop
make stream-bronze TABLE=payments     # a different table
make stream-bronze STREAM_CORES=2     # give it the whole cluster, deliberately
```

Stopping a detached stream: kill it through the Spark master UI rather than with `pkill`,
which leaves the application registered and its cores allocated:

```bash
curl -sf "localhost:8090/app/kill/?id=<app-id>&terminate=true"
```

---

## 4.10 Schema evolution: what the registry accepts, and the DDL trap

Chaos scenario 4, exercised end to end on 2026-09-28 against the live registry.

### What the gate does

| Change | Verdict |
| --- | --- |
| Add a nullable column (optional field, default null) | **accepted** - registered as a new version, connector keeps running |
| Add a required field with no default | refused, `is_compatible: false` |
| Change a field's type (`status` string -> int) | refused, and registering it returns **HTTP 409** |
| Remove an optional field | **accepted** - a reader ignores what it lacks |

`contracts/orders.v2.avsc` is committed specifically to be rejected: it is v1 with
`status` changed from string to int. `tests/chaos/test_schema_evolution.py` asserts the
registry refuses it, and that a rejected schema does not appear in the subject's versions.

### Proven against the real pipeline

A breaking DDL change (`ALTER COLUMN promo_code TYPE INTEGER`) made the connector **fail
loudly** rather than corrupt Bronze:

```
connector: RUNNING | task: FAILED
Caused by: ConfigException: Failed to access Avro data from topic cdc.public.orders :
  Schema being registered is incompatible with an earlier schema ...
  errorType:'MISSING_UNION_BRANCH' ... compatibility: 'BACKWARD'; error code: 409
```

Bronze was checked afterwards: 8384 rows, 8384 distinct. Nothing malformed got in.

### THE TRAP: a dropped column leaves a stale registered version

This cost the most time, and the obvious diagnosis is wrong.

After `ALTER TABLE orders DROP COLUMN promo_code`, the connector **stayed failed** even though
the database was back to its original 17 columns. The reason is not that "dropping a column
breaks compatibility" - it does not, and there is a test asserting so. The reason is:

1. version 2 in the registry still described `promo_code`
2. BACKWARD compatibility is checked against the **latest** version
3. Debezium's 17-field schema was therefore checked against an 18-field v2 and refused

Worse, Debezium's **cached table schema also went stale**: after a task restart it registered
a *new* version that still contained `promo_code`, describing a column that no longer existed.
A task restart is not enough - the connector must be restarted for Debezium to re-read the
table.

### Recovery from a stale schema

```bash
# 1. which versions exist, and do they match the table?
curl -sf localhost:8081/subjects/cdc.public.orders-value/versions
docker compose -f infra/docker-compose.yml --env-file .env --profile core exec -T postgres \
  psql -U streamhouse -d streamhouse -tAc \
  "SELECT count(*) FROM information_schema.columns WHERE table_name='orders';"

# 2. soft-delete versions describing columns that no longer exist
curl -X DELETE localhost:8081/subjects/cdc.public.orders-value/versions/<n>

# 3. restart the CONNECTOR, not just the task - only that re-reads the table schema
curl -X POST "localhost:8083/connectors/streamhouse-postgres/restart?includeTasks=true"

# 4. force a change and confirm the field count matches the table
```

Verified recovery: versions back to `[1]`, 17 fields, no `promo_code`, task RUNNING, and the
registered schema byte-identical to `contracts/orders.v1.avsc`.

**Before dropping a column in a later phase:** plan it. Drop the column, delete the versions
that describe it, restart the connector, and re-capture the contract - in that order.

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
