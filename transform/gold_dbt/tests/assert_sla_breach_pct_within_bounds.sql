-- A percentage outside 0..100 means the numerator and denominator disagree about what they
-- are counting - which is the failure mode of every rate metric, and invisible in a chart
-- until someone notices a bar above the top of the axis.
select
    order_date,
    city,
    restaurant_id,
    delivered_orders,
    breached_orders,
    sla_breach_pct
from {{ ref('agg_sla_daily') }}
where sla_breach_pct < 0
   or sla_breach_pct > 100
   or breached_orders > delivered_orders
