{#
  Grain: one row per (rider, validity window). SCD2.
#}

select
    {{ surrogate_key(['rider_id', 'valid_from']) }}  as rider_sk,
    rider_id,
    rider_name,
    city,
    vehicle_type,
    tier,
    is_online,

    valid_from,
    {{ scd2_effective_from('rider_id') }}            as effective_from,
    valid_to,
    is_current
from {{ ref('stg_riders') }}
where not is_deleted
