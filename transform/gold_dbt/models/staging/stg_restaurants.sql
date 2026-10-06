{#
  Grain: one row per (restaurant, validity window).

  Every version is kept, not just the current one. A fact joined to the *current* commission
  would restate history every time a rate was renegotiated - which is the entire reason the
  SCD2 dimension exists.
#}

select
    restaurant_id,
    name        as restaurant_name,
    city,
    cuisine,
    lat,
    lon,
    rating,
    commission_pct,
    is_open,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ source('silver', 'dim_restaurant_scd2') }}
