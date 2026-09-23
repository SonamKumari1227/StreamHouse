# transform/

Bronze → Silver → Gold. Two layers, two tools, two distinct jobs.

| Path | Purpose | Status |
| --- | --- | --- |
| `silver/` | PySpark — dedup, conforming, SCD2 `MERGE`, sessionization. | Phase 3 |
| `gold_dbt/` | dbt-spark — the star schema, its tests, and its docs. | Phase 4 |

Silver is PySpark because it needs `MERGE INTO` semantics and stateful streaming that SQL cannot
express. Gold is dbt because it is declarative SQL that benefits from free tests, docs, and lineage.
