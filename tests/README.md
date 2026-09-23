# tests/

The testing pyramid. Fast tests at the bottom, slow and realistic ones above.

| Path | Scope | Speed | Status |
| --- | --- | --- | --- |
| `unit/` | Pure transform logic, no containers. `chispa` DataFrame equality. Target 80% coverage on `transform/`. | Fast | Phase 1 onward |
| `integration/` | `testcontainers` — a real Postgres and Kafka, CDC end to end. | Slow | Phase 2 onward |
| `chaos/` | One test per failure scenario in `docs/architecture.md` §5.1. | Slow | Phase 1 onward |

Infrastructure is never mocked. If a test needs Kafka, it gets a real Kafka in a container.

```bash
pytest tests/unit                  # fast loop
pytest tests/unit --cov            # with coverage
pytest tests/integration           # needs Docker
```
