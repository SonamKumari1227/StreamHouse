{#
  Grain: one row per (order, menu item) - the order line.

  The reason this is its own fact rather than a column on fact_delivery: an order has many
  lines, and folding them in would multiply every order-level measure by its line count. The
  classic fan-out trap.

  The interesting column is `price_variance_inr`. `unit_price_inr` is what the line was
  actually charged, captured at order time; `menu_price_inr` is what the menu said at that
  same moment, read from the SCD2 dimension. They should agree, and when they do not it is
  either a promotion or a bug - and the singular test in tests/ asserts the gap stays small.
#}

with items as (
    select * from {{ ref('stg_order_items') }} where not is_deleted
),

orders as (
    select order_id, placed_ts, restaurant_id, status
    from {{ ref('stg_orders') }}
    where not is_deleted
),

menu as (select * from {{ ref('dim_menu_item') }})

select
    {{ surrogate_key(['i.order_item_id']) }}    as order_item_sk,
    {{ surrogate_key(['i.order_id']) }}         as order_sk,
    m.menu_item_sk,

    i.order_item_id,
    i.order_id,
    i.menu_item_id,
    o.restaurant_id,

    m.menu_item_name,
    i.qty,
    i.unit_price_inr,
    i.line_total_inr,

    -- The menu price in force when the order was placed, not today's.
    m.menu_price_inr,
    i.unit_price_inr - m.menu_price_inr         as price_variance_inr,

    o.placed_ts,
    o.status                                    as order_status
from items i
inner join orders o
    on i.order_id = o.order_id
left join menu m
    on i.menu_item_id = m.menu_item_id
   and {{ scd2_as_of('m', 'o.placed_ts') }}
