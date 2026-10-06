-- The order lines must sum to the order subtotal.
--
-- This is the one test that crosses the two facts, and it is the strongest statement the Gold
-- layer makes: that fact_delivery and fact_order_item describe the same orders. If the line
-- grain ever fanned out - a duplicated menu item version, a bad join - this catches it, where
-- a row count would not.
--
-- A rupee of tolerance, because the subtotal is a sum of rounded line totals.
with lines as (
    select
        order_id,
        sum(line_total_inr) as line_sum_inr
    from {{ ref('fact_order_item') }}
    group by order_id
)

select
    f.order_id,
    f.gross_revenue_inr,
    l.line_sum_inr,
    abs(f.gross_revenue_inr - l.line_sum_inr) as difference_inr
from {{ ref('fact_delivery') }} f
inner join lines l on f.order_id = l.order_id
where abs(f.gross_revenue_inr - l.line_sum_inr) > 1.00
