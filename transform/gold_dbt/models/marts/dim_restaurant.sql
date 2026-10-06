{#
  Grain: one row per (restaurant, validity window). SCD2.

  The surrogate key includes valid_from, so each version is its own row and a fact points at
  the version that was in force when it happened - which is the whole point: joining to the
  current commission would restate historical margin every time a rate was renegotiated.
#}

select
    {{ surrogate_key(['restaurant_id', 'valid_from']) }} as restaurant_sk,
    restaurant_id,
    restaurant_name,
    city,
    cuisine,
    lat,
    lon,
    rating,
    commission_pct,
    is_open,

    valid_from,
    {{ scd2_effective_from('restaurant_id') }}           as effective_from,
    valid_to,
    is_current
from {{ ref('stg_restaurants') }}
where not is_deleted
