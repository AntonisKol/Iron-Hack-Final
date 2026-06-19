-- Fact table: one row per customer with their churn outcome and key metrics.
-- The center of the star schema — links to dim_customers and dim_services via customer_id.
-- Contains the measurable numbers (charges, CLV, risk_score) and the target variable (is_churned).
with int_customers as (
    select *
    from {{ ref('int_customer_features') }}
)
select -- primary key
    customer_id,
    -- churn outcome: 1 = left the company, 0 = still active
    -- this is the main fact we're analyzing
    is_churned,
    -- financial metrics
    monthly_charges,
    total_charges,
    clv,
    -- customer lifetime value
    -- behavioral metrics
    tenure,
    risk_score,
    -- calculated in intermediate: higher = more likely to churn
    tenure_segment,
    -- New / Developing / Established / Loyal
    -- contract info (key churn driver)
    contract
from int_customers