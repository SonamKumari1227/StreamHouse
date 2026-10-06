{#
  Grain: one row per customer, latest known state.

  Read from Bronze rather than Silver because Silver does not model customers: they need no
  SCD2 (nothing analytical depends on a customer's history of tier changes yet) and no
  sessionization, so latest-state-per-key is the entire transformation.
#}

{% set payload = 'customer_id bigint, name string, phone string, city string, signup_ts string, tier string, is_active boolean, updated_at string' %}

with latest as (
    {{ latest_cdc_state(source('bronze', 'raw_customers_cdc'), payload, 'customer_id') }}
)

select
    customer_id,
    name                        as customer_name,
    city,
    tier,
    is_active,
    -- ISO-8601 strings on the wire (io.debezium.time.ZonedTimestamp), so cast, never divide.
    cast(signup_ts as timestamp) as signup_ts,
    cast(updated_at as timestamp) as updated_at,
    is_deleted
from latest
-- phone is deliberately dropped: it is personal data with no analytical use downstream.
