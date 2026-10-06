{#
  Grain: one row per order, latest known state.

  A thin pass over Silver: rename to Gold's vocabulary and derive the durations every
  downstream mart needs. The deduplication and replay guarding already happened upstream.
#}

select
    order_id,
    customer_id,
    restaurant_id,
    rider_id,
    status,
    placed_ts,
    accepted_ts,
    picked_up_ts,
    delivered_ts,
    promised_ts,
    cancel_reason,

    subtotal_inr,
    delivery_fee_inr,
    discount_inr,
    total_inr,

    -- Durations in minutes, null until the order reaches the state that defines them. A
    -- coalesce to zero here would quietly turn "not yet delivered" into "delivered instantly"
    -- and drag every average down.
    (unix_timestamp(picked_up_ts) - unix_timestamp(placed_ts)) / 60.0    as prep_minutes,
    (unix_timestamp(delivered_ts) - unix_timestamp(picked_up_ts)) / 60.0 as transit_minutes,
    (unix_timestamp(delivered_ts) - unix_timestamp(placed_ts)) / 60.0    as total_minutes,

    -- Measured against the order's own promise, which the generator sets per order. A fixed
    -- SLA would make the breach rate a property of the constant, not of the service.
    (unix_timestamp(delivered_ts) - unix_timestamp(promised_ts)) / 60.0  as sla_breach_minutes,

    status = 'CANCELLED'    as is_cancelled,
    status = 'DELIVERED'    as is_delivered,
    status in ('DELIVERED', 'CANCELLED') as is_terminal,

    is_deleted,
    lsn
from {{ source('silver', 'fact_order_state') }}
