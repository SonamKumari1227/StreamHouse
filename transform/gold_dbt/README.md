# transform/gold_dbt/

The dbt-spark project producing the Gold star schema. **Status: Phase 4, done 2026-10-06.**

```bash
make dbt-build          # models + seeds + tests, the whole thing
make dbt-run            # models only
make dbt-test           # tests only
make dbt-docs           # catalog.json + manifest.json
make reference-data     # refresh holidays and weather first
```

## Where this runs, and why

dbt runs **inside the Spark image**, not on the host — the same reason the PySpark jobs do:
that is where Spark, Delta and the S3A client are. It uses dbt-spark's `session` method, which
creates a SparkSession in-process, so there is no Thrift server and no extra container.

Two environment variables make it work, both set by the Makefile:

| Variable | Why |
| --- | --- |
| `SPARK_CONF_DIR=/opt/dbt-conf` | A dbt-only Spark config carrying the Hive metastore and warehouse settings. The default conf deliberately has none: switching the whole image to a Hive catalog would make every streaming job open a metastore connection at startup, for tables none of them reference. |
| `DBT_PROFILES_DIR` | The project ships its own `profiles.yml`. No credentials in it — S3A reads its keys from the container environment. |

The metastore is **Derby on a named volume**, not Postgres. Derby needs no credentials, and a
committed config file with a password in it would breach CLAUDE.md's rule on secrets. Derby is
single-writer, which is fine: dbt is the only thing that uses it, one invocation at a time.

`target/` and `logs/` go to a second volume at `/opt/dbt-target`, because the repo is mounted
read-only. Both mount points are created world-writable in the Dockerfile — a fresh named
volume inherits the image directory's mode, and a root-owned one makes dbt exit 2 without
printing anything at all, since it cannot open its own log file to say why.

## How dbt sees path-based Delta tables

Silver and Bronze are written straight to `s3a://` paths with no catalog — deliberately, so the
streaming jobs need no metastore. `source()` needs a named relation, so the
`register_external_sources` macro runs `on-run-start` and registers each path as an **external**
table: `CREATE TABLE IF NOT EXISTS`, so a re-run costs nothing, and external so dbt can never
delete data the Spark jobs own.

## Layout

| Path | Materialisation | Contents |
| --- | --- | --- |
| `models/staging/` | view | Renaming, typing, and latest-state-per-key for the two CDC tables Silver does not model (customers, order_items). Views, because materialising a rename buys nothing. |
| `models/marts/` | table | The star: six dimensions, two facts, one aggregate. |
| `seeds/` | table | `india_holidays` — see below. |
| `tests/` | — | Four singular tests; the generic ones live beside the models in `schema.yml`. |
| `macros/` | — | `latest_cdc_state`, `surrogate_key`, `scd2_effective_from`, `scd2_as_of`, `generate_schema_name`. |

Every model states its **grain** in its description, which goes verbatim into the dbt docs.
See `docs/architecture.md` §4.2.

Generic tests: `unique`, `not_null`, `relationships`, `accepted_values`. Singular tests that
encode real rules rather than structure:

- `assert_line_totals_reconcile_to_order` — the order lines must sum to the order subtotal.
  The strongest statement Gold makes: that the two facts describe the same orders. A fan-out in
  the line grain shows up here, where a row count would not.
- `assert_one_restaurant_version_per_order` — the point-in-time join must match exactly one
  dimension version. Two means overlapping validity windows, caught at the point where it would
  duplicate revenue.
- `assert_delivery_timeline_is_ordered` — no pickup before acceptance, no delivery before pickup.
- `assert_sla_breach_pct_within_bounds` — a rate outside 0..100 means the numerator and
  denominator disagree about what they are counting.

A failing test blocks everything downstream. That is the point.

## Three decisions worth knowing

**Point-in-time dimension joins.** A fact joins to the dimension version in force when the
order was placed, never to the current one — otherwise renegotiating a commission rate would
restate historical margin. `scd2_as_of()` is the predicate.

**The first version of each key is backdated to 1900.** An SCD2 dimension only knows history
from when CDC started capturing it, so facts older than that match no version and a
point-in-time join silently returns null for every one of them. `scd2_effective_from()` opens
the earliest window backwards; the true `valid_from` is kept beside it. This is a presentation
decision in Gold, deliberately not made in Silver — Silver records what was observed.

A visible consequence: `fact_order_item.price_variance_inr` is non-zero on some lines. The menu
price history from before 2026-10-05 was lost to Kafka's 7-day retention, so the earliest
*observed* version is backdated over orders that were charged a different price. The variance
is real and the cause is known; it is not a join bug.

**Nager.Date does not cover India.** It is absent from `/AvailableCountries` and answers
`204 No Content` for `IN`. The API integration is real and works for countries it does cover
(US 2026 returns 200); the Indian holidays come from the `india_holidays` seed, and `dim_date`
unions both. Sixteen rows that change once a year are exactly what a seed is for.

## Known noise

`ERROR HiveAlterHandler: Failed to alter table ...` appears during a build and is **not** a
failure — it is Hive trying to update table statistics for a Delta table it does not fully
understand. `dbt build` completes, every test passes, the data is correct. Do not chase it.
