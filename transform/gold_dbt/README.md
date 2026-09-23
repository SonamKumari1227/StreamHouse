# transform/gold_dbt/

The dbt-spark project producing the Gold star schema.

```
dbt_project.yml
models/
  staging/     1:1 with Silver, renamed and typed
  marts/       dim_* and fact_* with surrogate keys
tests/         singular tests encoding real business rules
macros/
seeds/         city reference data
```

Every model documents its **grain** in a one-line statement. See `docs/architecture.md` §4.2.

Generic tests: `unique`, `not_null`, `relationships`, `accepted_values`.

Singular tests that encode business rules, not just structure:

- no order reaches `DELIVERED` without an `accepted_ts`
- `contribution_margin_inr` is never below `-1 × gross_revenue_inr`
- every `fact_delivery` row joins to exactly one *current* SCD2 dimension row

A failing test blocks everything downstream. That is the point.

**Status:** Phase 4.
