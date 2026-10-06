{#
  Grain: one row per order item, latest known state.

  As with customers, read from Bronze: Silver does not model order_items, and the whole
  transformation is latest-state-per-key.
#}

{% set payload = 'order_item_id bigint, order_id bigint, menu_item_id bigint, qty int, unit_price_inr decimal(10,2), updated_at string' %}

with latest as (
    {{ latest_cdc_state(source('bronze', 'raw_order_items_cdc'), payload, 'order_item_id') }}
)

select
    order_item_id,
    order_id,
    menu_item_id,
    qty,
    -- The price actually charged, captured on the line at order time. This is NOT the menu
    -- price today - that is what dim_menu_item's history is for, and the two can differ.
    unit_price_inr,
    qty * unit_price_inr        as line_total_inr,
    cast(updated_at as timestamp) as updated_at,
    is_deleted
from latest
