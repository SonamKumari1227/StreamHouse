# serving/

Read-only consumers of Gold. Nothing here writes to the lakehouse.

| Path | Purpose | Status |
| --- | --- | --- |
| `api/` | FastAPI metrics service over the Gold aggregates, with its own tests. | Phase 4 onward |
| `dashboard/` | Streamlit dashboard — SLA breach rate, unit economics, rider utilisation. | Phase 4 onward |

Ad-hoc SQL against Gold goes through Trino or DuckDB rather than either of these.
