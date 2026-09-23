# quality/

Great Expectations, gating Bronze → Silver.

| Path | Purpose | Status |
| --- | --- | --- |
| `expectations/` | Suites — schema conformance, null rates, value ranges, distributional drift. | Phase 3 |
| `checkpoints/` | Checkpoint configs wiring suites to runs, invoked from Airflow. | Phase 3 |

GE runs on raw data and catches structural and statistical drift. dbt tests run on modelled data and
catch business-rule violations. Two layers, because they detect different classes of problem — see
ADR-0006.

Failures quarantine rows to `quarantine/` rather than crashing the pipeline. Nothing is ever silently
dropped.
