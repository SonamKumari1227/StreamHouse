-- An order cannot be picked up before it was accepted, or delivered before it was picked up.
--
-- The generator's state machine maintains this invariant, so a breach means the CDC parsing
-- or a point-in-time join has gone wrong rather than the source. Nulls are fine: they mean
-- the order has not reached that state yet.
select
    order_id,
    status,
    placed_ts,
    accepted_ts,
    picked_up_ts,
    delivered_ts
from {{ ref('fact_delivery') }}
where accepted_ts  < placed_ts
   or picked_up_ts < accepted_ts
   or delivered_ts < picked_up_ts
