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
	connector-register connector-status spark-smoke stream-bronze

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

# Credentials live in .env, never in the committed JSON. The rendered config is piped
# straight into curl and the response captured in a shell variable, so the password is never
# written to disk. PUT /connectors/<name>/config creates the connector when absent and
# updates it when present, which is what makes this idempotent.
connector-register:  ## Register or update the Debezium connector (idempotent)
	@test -f .env || (echo "ERROR: .env not found. Run: cp .env.example .env" && exit 1)
	@set -a; . ./.env; set +a; \
	resp=$$(envsubst < $(CONNECTOR_FILE) \
		| python3 -c "import json,sys; print(json.dumps(json.load(sys.stdin)['config']))" \
		| curl -s -w '\n%{http_code}' -X PUT \
			-H 'Content-Type: application/json' --data @- \
			localhost:$${SH_CONNECT_PORT:-8083}/connectors/$(CONNECTOR_NAME)/config); \
	code=$$(printf '%s' "$$resp" | tail -n1); \
	if [ "$$code" = "200" ] || [ "$$code" = "201" ]; then \
		echo "  $(CONNECTOR_NAME): registered/updated (HTTP $$code)"; \
	else \
		echo "  FAILED (HTTP $$code):"; printf '%s\n' "$$resp" | sed '$$d'; exit 1; \
	fi

connector-status:  ## Show the connector and its task state
	@curl -sf localhost:8083/connectors/$(CONNECTOR_NAME)/status \
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

stream-bronze:  ## Stream CDC into Bronze Delta. Use: make stream-bronze TABLE=payments ONCE=1
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/streamhouse/ingestion/bronze_cdc_stream.py \
		--table $(TABLE) $(if $(ONCE),--once,)

spark-smoke:  ## Prove Delta + S3A + Kafka work before writing a streaming job
	@$(CORE) exec -T spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/streamhouse/ingestion/smoke_test.py

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
	@printf "schema registry  : "; curl -sf localhost:8081/subjects && echo "" || echo "UNREACHABLE"
	@echo ""
	@echo "=== kafka connect ==="
	@printf "connectors       : "; curl -sf localhost:8083/connectors \
		&& echo "  <- empty is correct until Phase 2" || echo "UNREACHABLE"
	@printf "pg plugin        : "; curl -sf localhost:8083/connector-plugins \
		| grep -o PostgresConnector | head -1 || echo "NOT FOUND"
	@echo ""
	@echo "=== minio ==="
	@curl -sf localhost:9000/minio/health/live >/dev/null \
		&& echo "live             : OK" || echo "live             : UNREACHABLE"
	@echo ""
	@echo "=== spark ==="
	@curl -sf localhost:8090 >/dev/null \
		&& echo "master UI        : OK" || echo "master UI        : UNREACHABLE"
	@printf "workers alive    : "; curl -sf localhost:8090/json/ \
		| grep -o '"aliveworkers" : [0-9]*' | grep -o '[0-9]*$$' || echo "?"
	@echo ""
	@echo "UIs: console http://localhost:8080 | minio http://localhost:9001 | spark http://localhost:8090"
