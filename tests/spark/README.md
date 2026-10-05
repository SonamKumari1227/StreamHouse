# tests/spark/

DataFrame transforms, asserted against a real Spark session.

```bash
make test-spark
```

These do **not** run on the host. PySpark and Delta are not installed in `.venv` by the
decision recorded in `pyproject.toml`: they live in the Spark image, and the jobs under
`ingestion/` run there via `spark-submit`. A host copy would be a second runtime to keep in
step with the image, testing something other than what ships.

So the tests run inside the image instead — same Spark 3.5.3, same Delta 3.2.1, same Python
3.8.10. Two consequences:

- **Write Python 3.8**, as the jobs themselves do. No `StrEnum`, no `slots=True`, no `match`.
- **Nothing may be written to the repo.** It is bind-mounted read-only and the container runs
  as `spark`, not the host user, so `make test-spark` passes `-p no:cacheprovider`.

Everything here carries the `spark` marker, which the host run deselects. Deselection happens
after collection, though, and collection would import pyspark — so `conftest.py` also drops
the directory from collection outright when pyspark is absent.

## What belongs here

The streaming plumbing is Spark's code; asserting on it tests the framework. What belongs
here is the part that is ours and that later phases inherit:

- the dedup key `(source_table, pk, lsn)` — it must collapse a redelivered change and must
  **not** collapse two genuine changes to the same row
- where the primary key is read from — `after` for inserts and updates, `before` for deletes
- event time as the device clock, never arrival time, which Phase 3's watermark depends on

**Status:** Phase 2 onward. 12 tests on `ingestion/`; Phase 3's Silver transforms land here too.
