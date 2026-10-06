{#
  Grain: one row per calendar day.

  The spine spans the days the orders actually cover, widened a week each way so a fact near
  the boundary still joins. Holidays come from Nager.Date via ingestion/reference_data.py; if
  that API was unreachable the table is empty and every day simply reads as a non-holiday,
  which is why the join is a left one.
#}

with bounds as (
    select
        date_sub(min(to_date(placed_ts)), 7) as lo,
        date_add(max(to_date(placed_ts)), 7) as hi
    from {{ ref('stg_orders') }}
),

spine as (
    select explode(sequence(lo, hi, interval 1 day)) as date_day
    from bounds
),

-- Two sources, unioned: whatever Nager.Date returned, plus the India seed. Nager does not
-- cover India (204 No Content), and India is the domain - so the seed carries the real
-- holidays while the API path stays live for any country Nager does cover.
holiday_union as (
    select holiday_date, holiday_name from {{ source('bronze', 'ref_holidays') }}
    union all
    select holiday_date, holiday_name from {{ ref('india_holidays') }}
),

holidays as (
    select
        holiday_date,
        max(holiday_name) as holiday_name
    from holiday_union
    group by holiday_date
)

select
    {{ surrogate_key(['s.date_day']) }}     as date_sk,
    s.date_day,

    year(s.date_day)                        as year,
    quarter(s.date_day)                     as quarter,
    month(s.date_day)                       as month,
    date_format(s.date_day, 'MMMM')         as month_name,
    day(s.date_day)                         as day_of_month,
    weekofyear(s.date_day)                  as week_of_year,
    dayofweek(s.date_day)                   as day_of_week,
    date_format(s.date_day, 'EEEE')         as day_name,

    dayofweek(s.date_day) in (1, 7)         as is_weekend,
    h.holiday_date is not null              as is_holiday,
    h.holiday_name,

    -- Demand on a holiday does not look like a Tuesday, and neither does a weekend. This is
    -- the column the SLA aggregates group by when asking whether a breach was seasonal.
    case
        when h.holiday_date is not null then 'HOLIDAY'
        when dayofweek(s.date_day) in (1, 7) then 'WEEKEND'
        else 'WEEKDAY'
    end                                     as day_type
from spine s
left join holidays h
    on s.date_day = h.holiday_date
