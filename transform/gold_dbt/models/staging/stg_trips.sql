{#
  Grain: one row per (rider, trip).

  Trips are not joinable to orders: the GPS stream carries no order_id, by design - a device
  emits where it is, not what it is carrying. They are used for rider-level measures
  (utilisation, distance covered), not as a fact dimension.
#}

select
    rider_id,
    trip_id,
    started_ts,
    ended_ts,
    duration_s / 60.0   as duration_minutes,
    ping_count,
    avg_speed_kmph,
    max_speed_kmph,
    straight_line_km,
    trip_date
from {{ source('silver', 'gps_trips_sessionized') }}
