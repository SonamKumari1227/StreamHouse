# StreamHouse — local operations.
#
# Every target here works today. Targets for later phases are added when that phase
# lands, not before, so `make help` is never a list of promises.
#
# Requires GNU make. On Windows: `winget install GnuWin32.Make`, or run the
# `docker compose` commands underneath directly — every target is a thin wrapper.

# Mirrors the defaults in docker-compose.yml so targets work before .env is copied.
# These MUST be defined before PSQL: `:=` expands immediately, so a later definition
# would expand to empty and psql would read the next flag as the username.
PG_USER ?= streamhouse
PG_DB   ?= streamhouse

# --env-file is required: Compose resolves .env relative to the compose file's directory
# (infra/), not the working directory, so the repo-root .env must be named explicitly.
COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
CORE    := $(COMPOSE) --profile core
PSQL    := $(CORE) exec -T postgres psql -v ON_ERROR_STOP=1 -U $(PG_USER) -d $(PG_DB)

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs db-init health connect-topics minio-init \
	connector-register connector-status spark-smoke stream-bronze stream-gps \
	silver-orders silver-dim silver-dims silver-trips quality-gate silver-maintain \
	reference-data dbt-build dbt-run dbt-test dbt-docs test test-spark build \
	airflow-up airflow-down airflow-logs airflow-dags airflow-trigger backfill \
	freshness weather-coverage backfill-checksum

help:  ## Show available targets
	@echo "StreamHouse - Phase 0"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- lifecycle

up:  ## Start the core stack (postgres, redpanda, console, connect, minio, spark)
	@test -f .env || (echo "ERROR: .env not found. Run: cp .env.example .env" && exit 1)
	@# Redpanda first, then fix its internal topics, THEN everything else. Connect dies on
	@# startup if those topics are not log-compacted, so the ordering is not optional.
	$(CORE) up -d redpanda
	@$(MAKE) --no-print-directory connect-topics
	$(CORE) up -d
	@echo ""
	@echo "Starting. Watch readiness with:  make ps"
	@echo "Then verify everything with:     make health"

down:  ## Stop and remove containers. Volumes and data are PRESERVED.
	$(CORE) down

clean:  ## Stop and remove containers AND volumes. DESTROYS ALL DATA.
	@echo "This deletes the Postgres database, MinIO objects and Kafka topics."
	@echo "Press Ctrl-C to abort, Enter to continue."
	@read _
	$(CORE) down -v

ps:  ## Show service status
	$(CORE) ps

logs:  ## Tail logs. Use: make logs S=postgres
	$(CORE) logs -f --tail=100 $(S)

# ---------------------------------------------------------------- kafka connect

# Connect keeps its worker state in these three topics and REFUSES to start unless every one
# of them is log-compacted - the herder thread dies with a ConfigException and the REST port
# stays up, so the container looks alive while doing nothing. Broker auto-creation gives them
# cleanup.policy=delete, which is why this has to run before Connect does.
CONNECT_TOPICS := _connect_configs _connect_offsets _connect_status

connect-topics:  ## Ensure Connect internal topics exist and are compacted (idempotent)
	@$(CORE) exec -T redpanda sh -c 'for i in $$(seq 1 30); do \
		rpk cluster health 2>/dev/null | grep -q "Healthy:.*true" && exit 0; sleep 2; \
	done; echo "redpanda did not become healthy" >&2; exit 1'
	@for t in $(CONNECT_TOPICS); do \
		pol=$$($(CORE) exec -T redpanda rpk topic describe $$t -c 2>/dev/null \
			| awk '/^cleanup.policy/{print $$2}'); \
		if [ -z "$$pol" ]; then \
			$(CORE) exec -T redpanda rpk topic create $$t -p 1 -r 1 \
				-c cleanup.policy=compact >/dev/null \
				&& echo "  created  $$t  (cleanup.policy=compact)"; \
		elif [ "$$pol" != "compact" ]; then \
			$(CORE) exec -T redpanda rpk topic alter-config $$t \
				--set cleanup.policy=compact >/dev/null \
				&& echo "  altered  $$t  ($$pol -> compact)"; \
		else \
			echo "  ok       $$t  (cleanup.policy=compact)"; \
		fi; \
	done

# ---------------------------------------------------------------- database

# Piped from the host rather than referenced by container path: Git Bash on Windows rewrites
# absolute paths like /docker-entrypoint-initdb.d/... into C:/Program Files/Git/...
db-init:  ## Apply the OLTP schema (idempotent; safe to re-run)
	@echo "Applying infra/postgres/init/01-schema.sql ..."
	@$(PSQL) < infra/postgres/init/01-schema.sql
	@echo "Done. Tables:"
	@$(PSQL) -c "\dt"

# ---------------------------------------------------------------- debezium

CONNECTOR_FILE := infra/connectors/orders-postgres.json
CONNECTOR_NAME := streamhouse-postgres

# Docker Desktop publishes container ports to the WINDOWS host. This repo lives inside the
# WSL2 distro, where `localhost:<published port>` is NOT reachable - the connection is
# accepted and then hangs until it times out, which is a far more annoying symptom than a
# refusal. Every admin HTTP call therefore runs from a container on the compose network and
# addresses services by their compose name. That works from WSL2, from Windows, and on any
# machine regardless of which ports happen to be published.
#
# `connect` carries the curl because the Debezium image ships one (its own healthcheck uses
# it). If connect is itself down these report UNREACHABLE for everything; the container
# listing that `make health` prints first is what tells you why.
INNET := $(CORE) exec -T connect curl -sf --max-time 10

# Credentials live in .env, never in the committed JSON. The rendered config is piped
# straight into curl and the response captured in a shell variable, so the password is never
# written to disk. PUT /connectors/<name>/config creates the connector when absent and
# updates it when present, which is what makes this idempotent.
connector-register:  ## Register or update the Debezium connector (idempotent)
	@test -f .env || (echo "ERROR: .env not found. Run: cp .env.example .env" && exit 1)
	@set -a; . ./.env; set +a; \
	resp=$$(envsubst < $(CONNECTOR_FILE) \
		| python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['config']))" \
		| $(CORE) exec -T connect curl -s -w '\n%{http_code}' -X PUT \
			-H 'Content-Type: application/json' --data @- \
			http://localhost:8083/connectors/$(CONNECTOR_NAME)/config); \
	code=$$(printf '%s' "$$resp" | tail -n1); \
	if [ "$$code" = "200" ] || [ "$$code" = "201" ]; then \
		echo "  $(CONNECTOR_NAME): registered/updated (HTTP $$code)"; \
	else \
		echo "  FAILED (HTTP $$code):"; printf '%s\n' "$$resp" | sed '$$d'; exit 1; \
	fi

connector-status:  ## Show the connector and its task state
	@$(INNET) http://localhost:8083/connectors/$(CONNECTOR_NAME)/status \
		| python3 -m json.tool 2>/dev/null \
		|| echo "  $(CONNECTOR_NAME) is not registered (run: make connector-register)"

# ---------------------------------------------------------------- object store

# The medallion layout, plus the two paths the pipeline needs that are not layers:
# quarantine for rows a quality gate rejects, checkpoints for Spark's exactly-once state.
MINIO_BUCKETS := bronze silver gold quarantine checkpoints

minio-init:  ## Create the Delta buckets in MinIO (idempotent)
	@$(CORE) exec -T minio sh -c '\
		mc alias set local http://localhost:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" \
			>/dev/null 2>&1; \
		for b in $(MINIO_BUCKETS); do \
			if mc ls "local/$$b" >/dev/null 2>&1; then \
				echo "  ok       $$b"; \
			else \
				mc mb "local/$$b" >/dev/null 2>&1 && echo "  created  $$b"; \
			fi; \
		done'

# ---------------------------------------------------------------- spark

TABLE ?= orders

# spark.cores.max is a job policy, not a cluster policy, so it lives here rather than in
# spark-defaults.conf. A continuous streaming query with no cap takes every core the cluster
# has and holds them forever: during the load test it occupied both, and a second job sat in
# WAITING with 0 cores indefinitely. Phase 5's Airflow-triggered batch jobs would have
# starved exactly the same way.
STREAM_CORES ?= 1

stream-bronze:  ## Stream CDC into Bronze Delta. Use: make stream-bronze TABLE=payments ONCE=1
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/ingestion/bronze_cdc_stream.py \
		--table $(TABLE) $(if $(ONCE),--once,)

stream-gps:  ## Stream GPS pings into Bronze Delta. Use: make stream-gps ONCE=1
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/ingestion/bronze_gps_stream.py \
		$(if $(ONCE),--once,)

# ---------------------------------------------------------------- silver

silver-orders:  ## Build Silver fact_order_state from Bronze. Use: make silver-orders ONCE=1
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/transform/silver/fact_order_state.py \
		$(if $(ONCE),--once,)

DIM ?= restaurants

silver-dim:  ## Build an SCD2 dimension. Use: make silver-dim DIM=menu_items ONCE=1
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/transform/silver/dim_scd2.py \
		--dim $(DIM) $(if $(ONCE),--once,)

silver-trips:  ## Sessionize Bronze GPS pings into trips. Use: make silver-trips ONCE=1
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/transform/silver/gps_trips_sessionized.py \
		$(if $(ONCE),--once,)

# ---------------------------------------------------------------- orchestration (Phase 5)

ORCH := $(COMPOSE) --profile orchestration

airflow-up:  ## Start Airflow (separate profile from the core stack)
	$(ORCH) up -d
	@echo ""
	@echo "Airflow UI: http://localhost:$${SH_AIRFLOW_PORT:-8088}  (no login - local dev)"

airflow-down:  ## Stop Airflow. The core stack keeps running.
	$(ORCH) down

airflow-logs:  ## Tail the scheduler and dag-processor
	$(ORCH) logs -f --tail=50 airflow-scheduler airflow-dag-processor

airflow-dags:  ## List the DAGs Airflow has parsed, and any import errors
	@$(ORCH) exec -T airflow-scheduler airflow dags list
	@echo ""
	@echo "=== import errors (empty is correct) ==="
	@$(ORCH) exec -T airflow-scheduler airflow dags list-import-errors

DAG ?= silver_to_gold

airflow-trigger:  ## Trigger a DAG. Use: make airflow-trigger DAG=backfill
	@$(ORCH) exec -T airflow-scheduler airflow dags trigger $(DAG)

BACKFILL_DATE ?= 2026-09-28

backfill:  ## Prove the backfill is idempotent for one day
	@$(ORCH) exec -T airflow-scheduler airflow dags trigger backfill \
		--conf '{"backfill_date": "$(BACKFILL_DATE)"}'

# ---------------------------------------------------------------- gold (dbt)

# dbt runs inside the Spark image. SPARK_CONF_DIR points at the dbt-only conf, which is the
# only place the Hive metastore is configured - the streaming jobs never touch it.
# DBT_PROFILES_DIR points at the project, which carries its own profiles.yml.
DBT := $(CORE) exec -T \
	-e SPARK_CONF_DIR=/opt/dbt-conf \
	-e DBT_PROFILES_DIR=/opt/streamhouse/transform/gold_dbt \
	-w /opt/streamhouse/transform/gold_dbt \
	spark-master dbt

dbt-build:  ## Build Gold and run every test (dbt build)
	@$(DBT) build

dbt-run:  ## Build the Gold models only
	@$(DBT) run

dbt-test:  ## Run the Gold tests only
	@$(DBT) test

dbt-docs:  ## Generate dbt docs (written to the dbt-target volume)
	@$(DBT) docs generate

reference-data:  ## Fetch holidays (Nager.Date) and weather (Open-Meteo) into Bronze
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/ingestion/reference_data.py

QUALITY_TABLE ?= fact_order_state

# PYTHONPATH is set because spark-submit puts the SCRIPT's directory on sys.path, not the
# repo root - so `from quality.expectations import ...` fails with ModuleNotFoundError. The
# Silver jobs do not hit this: they import nothing but pyspark. executorEnv is belt and
# braces; the suites build Column expressions in the driver and never run on an executor.
quality-gate:  ## Gate a Silver table. Use: make quality-gate QUALITY_TABLE=gps_trips_sessionized
	@$(CORE) exec -T -e PYTHONPATH=/opt/streamhouse spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		--conf spark.executorEnv.PYTHONPATH=/opt/streamhouse \
		/opt/streamhouse/quality/run_gate.py --table $(QUALITY_TABLE)

FRESHNESS_PATH ?= s3a://bronze/raw_orders_cdc
MAX_AGE_HOURS  ?= 24

freshness:  ## Fail if a Delta table has stopped receiving rows
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/quality/freshness_check.py \
		--path $(FRESHNESS_PATH) --max-age-hours $(MAX_AGE_HOURS)

CITY ?= Bengaluru

weather-coverage:  ## Fail if a city has no weather in the reference extract
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/quality/weather_coverage.py --city "$(CITY)"

BACKFILL_TABLE ?= agg_sla_daily

backfill-checksum:  ## Checksum one day of a Gold table (content, order-independent)
	@$(CORE) exec -T -e PYTHONPATH=/opt/streamhouse spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		--conf spark.executorEnv.PYTHONPATH=/opt/streamhouse \
		/opt/streamhouse/quality/backfill_check.py \
		--table $(BACKFILL_TABLE) --date $(BACKFILL_DATE) --action checksum

silver-maintain:  ## OPTIMIZE + ZORDER + VACUUM the Silver tables. DRY_RUN=1 to report only
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=$(STREAM_CORES) \
		/opt/streamhouse/transform/silver/maintain.py \
		$(if $(DRY_RUN),--dry-run,)

silver-dims:  ## Build all three SCD2 dimensions, once each
	@$(MAKE) --no-print-directory silver-dim DIM=restaurants ONCE=1
	@$(MAKE) --no-print-directory silver-dim DIM=riders ONCE=1
	@$(MAKE) --no-print-directory silver-dim DIM=menu_items ONCE=1

spark-smoke:  ## Prove Delta + S3A + Kafka work before writing a streaming job
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/streamhouse/ingestion/smoke_test.py

# ---------------------------------------------------------------- tests

build:  ## Rebuild the custom Connect and Spark images
	$(CORE) build connect spark-master

test:  ## Unit tests on the host (no containers, no JVM)
	@.venv/bin/python -m pytest

# The DataFrame transforms cannot run on the host: there is no JVM and no pyspark there, by
# the decision in pyproject.toml. They run inside the Spark image instead, against the exact
# Spark, Delta and Python the streaming jobs themselves use.
#
# -p no:cacheprovider: /opt/streamhouse is the repo bind-mounted from the host and owned by
# the host user, while this container runs as `spark`. Writing .pytest_cache there fails.
test-spark:  ## PySpark transform tests, inside the Spark image
	@$(CORE) exec -T spark-master bash -c 'cd /opt/streamhouse \
		&& export PYTHONDONTWRITEBYTECODE=1 \
		&& export PYTHONPATH="/opt/spark/python:$$(ls /opt/spark/python/lib/py4j-*-src.zip)" \
		&& python3 -m pytest tests/spark -m spark -p no:cacheprovider'

# ---------------------------------------------------------------- verification

# printf, not `echo -n`: under /bin/sh `echo -n` prints a literal "-n" on some shells.
health:  ## Verify every core service
	@echo "=== containers ==="
	@$(CORE) ps --format "table {{.Name}}\t{{.Status}}"
	@echo ""
	@echo "=== postgres ==="
	@$(CORE) exec -T postgres pg_isready -U $(PG_USER) -d $(PG_DB)
	@printf "wal_level        : "; $(PSQL) -tAc "SHOW wal_level;"
	@printf "max_repl_slots   : "; $(PSQL) -tAc "SHOW max_replication_slots;"
	@printf "max_wal_senders  : "; $(PSQL) -tAc "SHOW max_wal_senders;"
	@printf "tables           : "; $(PSQL) -tAc \
		"SELECT count(*) FROM information_schema.tables WHERE table_schema='public';"
	@printf "orders replident : "; $(PSQL) -tAc \
		"SELECT relreplident FROM pg_class WHERE relname='orders';"
	@printf "repl slots       : "; $(PSQL) -tAc "SELECT count(*) FROM pg_replication_slots;"
	@echo ""
	@echo "=== redpanda ==="
	@$(CORE) exec -T redpanda rpk cluster health
	@printf "schema registry  : "; $(INNET) http://redpanda:8081/subjects && echo "" || echo "UNREACHABLE"
	@echo ""
	@echo "=== kafka connect ==="
	@printf "connectors       : "; $(INNET) http://localhost:8083/connectors \
		&& echo "" || echo "UNREACHABLE"
	@printf "pg plugin        : "; $(INNET) http://localhost:8083/connector-plugins \
		| grep -o PostgresConnector | head -1 || echo "NOT FOUND"
	@echo ""
	@echo "=== minio ==="
	@$(INNET) http://minio:9000/minio/health/live >/dev/null \
		&& echo "live             : OK" || echo "live             : UNREACHABLE"
	@echo ""
	@echo "=== spark ==="
	@$(INNET) http://spark-master:8080 >/dev/null \
		&& echo "master UI        : OK" || echo "master UI        : UNREACHABLE"
	@printf "workers alive    : "; $(INNET) http://spark-master:8080/json/ \
		| grep -o '"aliveworkers" : [0-9]*' | grep -o '[0-9]*$$' || echo "?"
	@echo ""
	@echo "UIs: console http://localhost:8080 | minio http://localhost:9001 | spark http://localhost:8090"
