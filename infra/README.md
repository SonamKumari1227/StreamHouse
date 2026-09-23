# infra/

Everything needed to run the stack locally. No cloud resources, credentials, or SDKs — see
`../CLAUDE.md` for the cloud-free rule.

| Path | Purpose | Status |
| --- | --- | --- |
| `docker-compose.yml` | Service definitions, split across `core` / `orchestration` / `observability` profiles. | Phase 0 |
| `postgres/` | Init DDL and `postgresql.conf` overrides for logical replication. | Phase 0 |
| `connectors/` | Debezium connector JSON configs, registered against Kafka Connect. | Phase 2 |
| `prometheus/` | Scrape config and alert rules. | Phase 6 |
| `grafana/` | Provisioned datasources and dashboards as JSON. | Phase 6 |
| `terraform/` | Phase 7 only — a documented design exercise. `validate` and `plan` only; `apply` is never run. | Phase 7 |

`connect` and `spark` come up in the `core` profile but stay idle until Phase 2. Bringing them up is
not phase-skipping; submitting work to them would be.
