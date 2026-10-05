# ADR-0010: A native expectation suite over Great Expectations

- **Status:** Accepted
- **Date:** 2026-10-06
- **Phase:** 3

## Context

`docs/architecture.md` names Great Expectations as the quality gate on the Bronze → Silver
boundary, and `transform/silver/README.md` states the behaviour required of it: *failures
quarantine the offending rows rather than crashing the job*.

The constraint is where the gate has to run. Everything that touches a DataFrame runs inside
the Spark image via `spark-submit`, on **Python 3.8.10** (ADR-0009), and the image is built
from a pinned Dockerfile rather than resolved at submit time. A validation library therefore
has to be installed into that image and has to work against Spark there.

Great Expectations is not a small addition. The 0.18 line supports Python 3.8, but it arrives
with a large transitive dependency set and its own configuration model - a Data Context,
datasources, checkpoints - that has been reworked across recent releases. Wiring its Spark
execution engine into a `foreachBatch` callback is a meaningful piece of work, and it would
sit on the critical path of a phase whose hard problems are SCD2 and watermarking.

What the pipeline actually needs from the gate is narrow: evaluate a set of row predicates,
keep the two halves separate, and record which rules each failing row broke.

## Options considered

| Option | Pros | Cons |
| --- | --- | --- |
| **A — a native expectation suite (chosen)** | No new dependency in the image. Runs on 3.8 as-is. The rules are ordinary Spark `Column` expressions, so they are readable, unit-testable and cost nothing extra to evaluate. Written to GE's shape - suite, validation result, quarantine - so a later swap is mechanical. | No Data Docs, no profiling, no community expectation library. The vocabulary is ours, so a reader who knows GE must still read the code. |
| B — Great Expectations in the Spark image | The named tool, recognisable to reviewers. Data Docs are genuinely good. A large library of expectations, and profiling for free. | ~100 transitive dependencies pinned into an image that currently has a handful. Its Spark integration and config model are the fiddly part, and the API has moved between versions. Nothing in Phase 3 needs what it adds beyond the predicates themselves. |
| C — no gate until Phase 6 | Fastest. | Silver would be built with no statement of what makes a row valid, and Phase 6's quality metric would have nothing to measure. The gate is where the invariants get written down, and writing them later means writing them from memory. |

## Decision

Implement the gate natively in `quality/expectations.py`, as suites of named row predicates,
and defer Great Expectations.

The reasoning is proportion. The gate's job here is a dozen predicates and a two-way split;
GE's value is in everything around that - profiling, Data Docs, a shared vocabulary - none of
which Phase 3 consumes. Taking the dependency now would mean carrying its configuration model
through every later phase for a benefit that only starts to pay in Phase 6 at the earliest.

The interface is deliberately GE-shaped: a suite is a tuple of named expectations, validation
returns `passed` and `failed`, and failures carry a `violations` array. Replacing the engine
later changes `validate()` and leaves the suites and their tests where they are.

## Consequences

- The invariants are written down now, in `quality/expectations.py`, and tested - 15 tests in
  `tests/spark/test_expectations.py`, including the three-valued-logic case where a null
  predicate must count as a violation rather than quietly passing.
- `make quality-gate TABLE=...` exits 1 when anything is quarantined, which is the hook Phase 5
  orchestration needs and the number Phase 6 alerts on.
- No Data Docs. If the project later wants them, that is the moment to revisit this, and the
  suites port across as predicates.
- The deviation from `docs/architecture.md` is deliberate and recorded here; the architecture
  document still names GE as the intended end state.
