# ADR-0007: Health checks probe the work path, not the port

- **Status:** Accepted
- **Date:** 2026-09-28
- **Phase:** 1 (arising from a Phase 2 blocker)

## Context

Kafka Connect crash-looped 39 times without `make health` saying anything actionable about
why. Its healthcheck curled `/`, which the REST layer serves on its own, while the component
that does the actual work - the herder - was dead. The same shape exists elsewhere in the
stack: `spark-worker` was checked by opening a TCP port, which stays open whether or not the
worker has registered with the master.

A health check that a broken service can pass is worse than no health check, because
`make health` is the thing a runbook tells you to trust at 2am.

## Options considered

| Option | Pros | Cons |
| --- | --- | --- |
| **A - probe an endpoint that requires the work path** (chosen) | Fails when the service cannot do its job, not merely when the process is gone. | Slightly more expensive; needs a real endpoint to exist. |
| B - keep probing the port, add detail lines to `make health` | Cheap; `make health` already printed `connectors : UNREACHABLE`. | Container status and the detail line disagree, so Compose, `docker ps` and `depends_on: service_healthy` all still believe a broken service is fine. |
| C - parse logs for known errors | Catches causes the endpoint cannot see. | Brittle; every new failure mode needs a new pattern. |

## Decision

Probe the shallowest endpoint that still exercises the work path. For Connect that is
`/connectors`, which must reach the herder; for Spark it is the master's worker count rather
than an open socket.

Be honest about the limit. In the crash-loop that prompted this, the process exits, so the
container restarts and health resets to `starting` on every cycle - it never reported
`healthy`, and never reached `unhealthy` either, because each crash starts a fresh
`start_period`. The old check was uninformative rather than actively wrong. The case this
genuinely fixes is a service whose process survives while its work path is broken, where a
port probe answers 200 forever.

## Consequences

**Easier:** A stuck-but-alive service now fails its check. `depends_on: service_healthy` is
meaningful, so dependants wait for real readiness instead of an open socket.

**Harder:** Checks must be written per service - there is no generic "is it up" probe. Each
one needs an endpoint that is cheap enough to run every 15s and still touches the work path.

**Commits us to:** reviewing the health check whenever a service is added, and treating
`starting` that never resolves as a failure signal in its own right, since a crash-looping
container never reaches `unhealthy`.

## Notes

Revisit if Compose gains a way to distinguish "restarting repeatedly" from "still starting";
that distinction is currently only visible in `RestartCount`.
