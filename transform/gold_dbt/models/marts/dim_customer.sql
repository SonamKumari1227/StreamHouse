{#
  Grain: one row per customer.

  Type 1, not Type 2: nothing downstream asks what a customer's tier was at order time, and a
  dimension that keeps history nobody queries is cost without benefit. If cohort analysis ever
  needs it, the CDC is in Bronze and this becomes an SCD2 build like the others.
#}

select
    {{ surrogate_key(['customer_id']) }}    as customer_sk,
    customer_id,
    customer_name,
    city,
    tier,
    is_active,
    signup_ts,
    date_trunc('month', signup_ts)          as signup_month,
    updated_at
from {{ ref('stg_customers') }}
where not is_deleted
