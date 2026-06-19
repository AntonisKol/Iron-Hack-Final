-- Q7: Customer Churn Data Mart — Intermediate Layer
-- Adds two calculated columns on top of the staging data:
--   clv (customer lifetime value) and risk_score (churn risk points, 0–6).
-- All downstream models (dim_customers, dim_services, fct_churn) build from this.
with stg as (
    select *
    from {{ ref('stg_churn') }}
),
features as (
    select *,
        monthly_charges * tenure as clv,
        case
            when tenure <= 12 then 'New'
            when tenure <= 24 then 'Developing'
            when tenure <= 48 then 'Established'
            else 'Loyal'
        end as tenure_segment,
        (
            case when contract = 'Month-to-month' then 2 else 0 end
            + case when internet_service = 'Fiber optic' then 1 else 0 end
            + case when online_security = 'No' then 1 else 0 end
            + case when tech_support = 'No' then 1 else 0 end
            + case when payment_method = 'Electronic check' then 1 else 0 end
            + case when tenure < 12 then 1 else 0 end
        ) as risk_score
    from stg
)
select *
from features
