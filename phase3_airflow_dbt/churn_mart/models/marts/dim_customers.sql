-- Dimension table: one row per customer with their profile and account details.
-- Answers WHO — demographics, contract type, payment method, and financials.
-- Built from the intermediate model, which already has clean data and calculated features.
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