# ADR-0009: Pin the Spark runtime in an image, and the AWS SDK that s3a:// requires

- **Status:** Accepted
- **Date:** 2026-09-28
- **Phase:** 2

## Context

`apache/spark:3.5.3` ships no Delta Lake, no `hadoop-aws`, and no `spark-sql-kafka`. All three
are needed before a single line of Bronze streaming code can run, and one of them drags in a
dependency the project's cloud-free rule appears to forbid.

## Options considered

| Option | Pros | Cons |
| --- | --- | --- |
| **A - bake the jars into a built image** (chosen) | Config lives in the repository. No network at submit time. Versions pinned where they can be reviewed. | An image to rebuild when a version moves. |
| B - `--packages` at submit time | Nothing to build. | Ivy resolution on every submit, needs network, and the working combination survives only in someone's shell history. A streaming job restarting at 3am should not depend on Maven Central. |
| C - mount a jars directory from the host | No rebuild. | The contents are untracked; "works on my machine" by construction. |

Versions are not free choices. The image dictates them: Scala **2.12.18**, Hadoop **3.3.4**,
Spark **3.5.3**. `hadoop-aws` must match Hadoop exactly - a mismatch fails at runtime with
`NoSuchMethodError`, not at build time.

## Decision

Build `infra/spark/Dockerfile` with the jars pinned, and bake `spark-defaults.conf` alongside
so no `--conf` flag is ever needed at submit time.

**On `aws-java-sdk-bundle` and the cloud-free rule.** `hadoop-aws` is the only implementation
of `s3a://`, and it requires the AWS SDK. CLAUDE.md rule 1 forbids cloud client libraries -
but its own allow-list states: *"MinIO - self-hosted, S3A-compatible, runs in Docker. `s3a://`
URIs point at local MinIO only."* Permitting `s3a://` necessarily permits the library that
implements it, so this is read as covered by the existing allowance rather than an amendment
to it.

The distinction that makes this defensible, and the line to hold:

- **Permitted:** a protocol client whose endpoint is pinned to `http://minio:9000`, which
  never contacts a cloud service and would fail closed if it tried.
- **Not permitted:** anything that authenticates to, or depends on, a cloud service. The 13
  cloud KMS jars were stripped from the Connect image on exactly this basis - they existed to
  talk to AWS/Azure/GCP key services we do not use.

If the endpoint is ever pointed at real S3, that is a rule violation regardless of which jars
are installed. The jar is not the thing being controlled; the destination is.

## Consequences

**Easier:** A submit needs no flags and no network. The whole runtime is reviewable in one
Dockerfile. Phase 7's portability claim gets cheaper - swapping the S3A endpoint is a config
change, which is the point of using MinIO at all.

**Harder:** Four pinned versions must move together. `aws-java-sdk-bundle` is ~200 MB, so the
image is large. A Spark upgrade means re-checking every pin against the new Hadoop version.

**Commits us to:** keeping the S3A endpoint pointed at MinIO, and to documenting any future
cloud-adjacent dependency against the "destination, not the jar" test above.

## Notes

The Spark image runs **Python 3.8.10**, while the project targets 3.11. Anything executed by
`spark-submit` must therefore be 3.8-compatible - no `StrEnum`, no `slots=True` dataclasses,
no `match`. The generator code is unaffected because it runs on the host, but `ingestion/`
is not. Recorded as technical debt; a 3.11 base for the Spark image would remove the split.
