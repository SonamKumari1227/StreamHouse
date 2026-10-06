{#
  Collapse a Bronze CDC table to one row per key: the change with the highest LSN.

  The same rule `fact_order_state` applies in PySpark, expressed in SQL for the two tables
  Silver does not model. Highest LSN, not latest ingest time - arrival order is not change
  order, and a batch replayed after a restart would otherwise win.

  `payload_schema` is a Spark DDL string. The payload is read from `after_json`, falling back
  to `before_json` because a delete carries its row only in the before image.
#}

{% macro latest_cdc_state(source_relation, payload_schema, key) %}
    with parsed as (
        select
            lsn,
            op,
            from_json(coalesce(after_json, before_json), '{{ payload_schema }}') as payload
        from {{ source_relation }}
    ),

    typed as (
        select
            payload.*,
            lsn,
            op = 'd' as is_deleted
        from parsed
        -- A payload that did not parse has no key to collapse on.
        where payload.{{ key }} is not null
    ),

    ranked as (
        select
            *,
            row_number() over (partition by {{ key }} order by lsn desc) as _rn
        from typed
    )

    -- `select * except (_rn)` would read better, but EXCEPT in a star expression is a
    -- Databricks extension that open-source Spark 3.5 rejects outright. The helper column
    -- rides along instead; every caller selects named columns, so it never reaches a model.
    select *
    from ranked
    where _rn = 1
{% endmacro %}


{#
  A surrogate key: a deterministic hash of the business key and, for an SCD2 dimension, the
  version's start. Deterministic matters - a rebuild must produce the same keys, or every fact
  row pointing at a dimension would be orphaned by the next full refresh.

  Nulls get an explicit sentinel, because concat_ws skips them and ('a', null) would otherwise
  collide with (null, 'a').
#}
{% macro surrogate_key(columns) %}
    md5(concat_ws('||'
        {%- for column in columns -%}
            , coalesce(cast({{ column }} as string), '<null>')
        {%- endfor -%}
    ))
{% endmacro %}
