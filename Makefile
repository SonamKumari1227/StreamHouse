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
.PHONY: help up down clean ps logs db-init health

help:  ## Show available targets
	@echo "StreamHouse - Phase 0"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- lifecycle

up:  ## Start the core stack (postgres, redpanda, console, connect, minio, spark)
	@test -f .env || (echo "ERROR: .env not found. Run: cp .env.example .env" && exit 1)
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

# ---------------------------------------------------------------- database

# Piped from the host rather than referenced by container path: Git Bash on Windows rewrites
# absolute paths like /docker-entrypoint-initdb.d/... into C:/Program Files/Git/...
db-init:  ## Apply the OLTP schema (idempotent; safe to re-run)
	@echo "Applying infra/postgres/init/01-schema.sql ..."
	@$(PSQL) < infra/postgres/init/01-schema.sql
	@echo "Done. Tables:"
	@$(PSQL) -c "\dt"

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
