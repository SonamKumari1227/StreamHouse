{#
  Grain: one row per order.

  Every order, not only the terminal ones. The architecture's grain note reads "one row per
  order, at its terminal state", and that is what the measures are: `transit_minutes`,
  `sla_breach_flag` and the margin columns are null until the order reaches the state that
  defines them. Dropping in-flight orders instead would make the fact unable to answer how
  many orders exist, which is the first question anyone asks of it - and the aggregates that
  care filter on `is_delivered` themselves.

  Every dimension join is point-in-time: the version of the restaurant and rider that was in
  force when the order was PLACED, not whichever version is current now.
#}

with orders as (
    select * from {{ ref('stg_orders') }} where not is_deleted
),

restaurant as (select * from {{ ref('dim_restaurant') }}),
rider      as (select * from {{ ref('dim_rider') }}),
customer   as (select * from {{ ref('dim_customer') }}),
weather    as (select * from {{ ref('dim_weather') }}),
dates      as (select * from {{ ref('dim_date') }})

select
    {{ surrogate_key(['o.order_id']) }}     as order_sk,
    o.order_id,

    -- Foreign keys. Point-in-time for the SCD2 dimensions.
    d.date_sk,
    c.customer_sk,
    r.restaurant_sk,
    rd.rider_sk,
    w.weather_sk,

    o.customer_id,
    o.restaurant_id,
    o.rider_id,
    r.city,
    r.cuisine,

    o.status,
    o.placed_ts,
    o.accepted_ts,
    o.picked_up_ts,
    o.delivered_ts,
    o.promised_ts,

    o.prep_minutes,
    o.transit_minutes,
    o.total_minutes,

    -- A breach is only meaningful once delivered. Null for everything else, deliberately: an
    -- undelivered order has not breached, but neither has it met the promise.
    case when o.is_delivered then o.sla_breach_minutes end          as sla_breach_minutes,
    case when o.is_delivered then o.sla_breach_minutes > {{ var('sla_grace_minutes') }} end
                                                                    as sla_breach_flag,

    o.subtotal_inr                                                  as gross_revenue_inr,
    o.discount_inr,
    o.delivery_fee_inr,
    o.total_inr,

    -- Unit economics, using the commission rate in force at order time.
    round(o.subtotal_inr * r.commission_pct / 100.0, 2)             as commission_inr,
    -- The rider keeps the delivery fee. A crude model, but an explicit one.
    o.delivery_fee_inr                                              as rider_payout_inr,
    round(
        (o.subtotal_inr * r.commission_pct / 100.0) + o.delivery_fee_inr
        - o.delivery_fee_inr - o.discount_inr,
        2
    )                                                               as contribution_margin_inr,

    o.is_cancelled                                                  as cancelled_flag,
    o.cancel_reason,
    o.is_delivered,
    o.is_terminal
from orders o
left join dates d
    on to_date(o.placed_ts) = d.date_day
left join customer c
    on o.customer_id = c.customer_id
left join restaurant r
    on o.restaurant_id = r.restaurant_id
   and {{ scd2_as_of('r', 'o.placed_ts') }}
left join rider rd
    on o.rider_id = rd.rider_id
   and {{ scd2_as_of('rd', 'o.placed_ts') }}
left join weather w
    on r.city = w.city
   and to_date(o.placed_ts) = w.weather_date
