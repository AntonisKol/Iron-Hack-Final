with int_customers as (
    select *
    from {{ ref('int_customer_features') }}
)
select
    customer_id,
    gender,
    is_senior,
    has_partner,
    has_dependents,
    tenure,
    tenure_segment,
    contract,
    payment_method,
    is_paperless,
    monthly_charges,
    total_charges,
    clv
from int_customers
