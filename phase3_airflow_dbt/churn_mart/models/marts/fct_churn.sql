with int_customers as (
    select *
    from {{ ref('int_customer_features') }}
)
select
    customer_id,
    is_churned,
    monthly_charges,
    total_charges,
    clv,
    tenure,
    risk_score,
    tenure_segment,
    contract
from int_customers
