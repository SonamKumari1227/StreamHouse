{#
  Grain: one row per (date, city, restaurant).

  The aggregate the SLO dashboard reads in Phase 6. It is built on delivered orders only -
  a breach rate whose denominator included orders still in transit would fall every time
  volume rose, which is exactly backwards.

  Weather and day type ride along because the first question asked of a bad day is always
  whether it was raining or a holiday.
#}

{#
  Incremental, overwriting whole day-partitions.

  This is the model the backfill correctness bar is written against: delete a day, re-run for
  that day, get bit-identical output. `insert_overwrite` with a partition column means a
  backfill replaces exactly the days its SELECT produces and leaves every other day untouched
  - so re-running 2026-09-28 cannot disturb 2026-09-29, and running it twice is the same as
  running it once.

  Determinism is what makes "bit-identical" true rather than lucky: every column here is a
  pure aggregate of the fact rows for that day. Nothing reads the clock, and nothing depends
  on the order rows arrive in.
#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['order_date'],
) }}

with delivered as (
    select *
    from {{ ref('fact_delivery') }}
    where is_delivered

    {% if is_incremental() %}
      {% if var('backfill_date', '') != '' %}
        -- A targeted backfill: this one day, and only this one.
        and to_date(placed_ts) = date '{{ var("backfill_date") }}'
      {% else %}
        -- The routine run. Late-arriving CDC can still change a recent day, so the last few
        -- are rebuilt every time rather than assumed settled.
        and to_date(placed_ts) >= date_sub(current_date(), 3)
      {% endif %}
    {% endif %}
),

joined as (
    select
        f.*,
        d.day_type,
        d.is_weekend,
        d.is_holiday,
        w.weather_band,
        w.is_rainy
    from delivered f
    left join {{ ref('dim_date') }} d on f.date_sk = d.date_sk
    left join {{ ref('dim_weather') }} w on f.weather_sk = w.weather_sk
)

select
    to_date(placed_ts)                                      as order_date,
    city,
    restaurant_id,

    max(day_type)                                           as day_type,
    max(is_weekend)                                         as is_weekend,
    max(is_holiday)                                         as is_holiday,
    max(weather_band)                                       as weather_band,
    max(is_rainy)                                           as is_rainy,

    count(*)                                                as delivered_orders,
    sum(case when sla_breach_flag then 1 else 0 end)        as breached_orders,
    round(
        100.0 * sum(case when sla_breach_flag then 1 else 0 end) / count(*), 2
    )                                                       as sla_breach_pct,

    round(avg(prep_minutes), 2)                             as avg_prep_minutes,
    round(avg(transit_minutes), 2)                          as avg_transit_minutes,
    round(avg(total_minutes), 2)                            as avg_total_minutes,
    -- Percentiles, because an average delivery time hides the tail that customers complain
    -- about. p95 is the number a delivery SLO is actually written against.
    round(percentile_approx(total_minutes, 0.5), 2)         as p50_total_minutes,
    round(percentile_approx(total_minutes, 0.95), 2)        as p95_total_minutes,

    sum(gross_revenue_inr)                                  as gross_revenue_inr,
    sum(commission_inr)                                     as commission_inr,
    sum(contribution_margin_inr)                            as contribution_margin_inr
from joined
group by to_date(placed_ts), city, restaurant_id
