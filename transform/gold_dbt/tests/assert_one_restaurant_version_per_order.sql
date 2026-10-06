-- The point-in-time join must match exactly one restaurant version per order.
--
-- Two matches means overlapping validity windows in the dimension - the failure Silver's
-- correctness bar forbids, caught here from the other side, at the point where it would
-- actually duplicate revenue. fact_delivery would silently gain a row per extra match, and
-- every sum over it would be wrong by an amount nobody could reconcile.
select
    order_id,
    count(*) as row_count
from {{ ref('fact_delivery') }}
group by order_id
having count(*) > 1
