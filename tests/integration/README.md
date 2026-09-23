# tests/integration/

End-to-end tests against real infrastructure via `testcontainers` — a real Postgres with logical
replication and a real Kafka broker, started and torn down per test session.

What belongs here:

- **CDC round trip** — mutate a row in Postgres, assert it lands in Bronze with the right before and
  after image
- **Exactly-once** — kill the streaming job mid-batch, restart, assert no duplicates and no loss
- **Contract enforcement** — publish a violating payload, assert it lands in the DLQ rather than Bronze

Slow by nature. Runs in CI on every pull request, not on every local save.

**Status:** Phase 2 onward.
