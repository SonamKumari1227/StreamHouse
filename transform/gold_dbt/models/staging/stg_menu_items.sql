{#
  Grain: one row per (menu item, validity window).

  Carries the price history, which is what lets fact_order_item compare the price charged on
  the line against the menu price in force at that moment.
#}

select
    menu_item_id,
    restaurant_id,
    name        as menu_item_name,
    price_inr,
    is_available,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ source('silver', 'dim_menu_item_scd2') }}
