-- dimension table: one row per customer with their profile information
-- this answers WHO — demographics and account details
-- built on top of the intermediate model which has clean data + calculated features
with int_customers as (
    select *
    from {{ ref('int_customer_features') }}
)
select -- primary key: unique identifier for each customer
    customer_id,
    -- demographics
    gender,
    is_senior,
    has_partner,
    has_dependents,
    -- account details
    tenure,
    tenure_segment,
    -- New / Developing / Established / Loyal
    contract,
    payment_method,
    is_paperless,
    -- financials
    monthly_charges,
    total_charges,
    clv -- customer lifetime value = monthly_charges * tenure
from int_customers