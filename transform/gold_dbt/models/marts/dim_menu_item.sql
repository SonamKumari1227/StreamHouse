{#
  Grain: one row per (menu item, validity window). SCD2.

  This is the dimension that most justifies the pattern: price changes, and an order line has
  to be read against the price that was on the menu when it was placed.
#}

select
    {{ surrogate_key(['menu_item_id', 'valid_from']) }}  as menu_item_sk,
    menu_item_id,
    restaurant_id,
    menu_item_name,
    price_inr                                            as menu_price_inr,
    is_available,

    valid_from,
    {{ scd2_effective_from('menu_item_id') }}            as effective_from,
    valid_to,
    is_current
from {{ ref('stg_menu_items') }}
where not is_deleted
