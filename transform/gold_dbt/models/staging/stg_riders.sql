{#
  Grain: one row per (rider, validity window).
#}

select
    rider_id,
    name        as rider_name,
    city,
    vehicle_type,
    tier,
    is_online,
    valid_from,
    valid_to,
    is_current,
    is_deleted
from {{ source('silver', 'dim_rider_scd2') }}
