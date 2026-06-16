-- intermediate model: add calculated features on top of clean staging data
-- ref() tells dbt to use the output of stg_churn as input here
-- dbt automatically runs stg_churn first because of this dependency
with stg as (
    select *
    from {{ ref('stg_churn') }}
),
features as (
    select *,
        -- CLV: how much revenue this customer has generated
        -- same formula as Phase 2: monthly charges x months active
        monthly_charges * tenure as clv,
        -- tenure bucket: group customers by how long they've stayed
        case
            when tenure <= 12 then 'New'
            when tenure <= 24 then 'Developing'
            when tenure <= 48 then 'Established'
            else 'Loyal'
        end as tenure_segment,
        -- risk score: count negative signals per customer
        -- each condition that increases churn risk adds 1 point
        (
            case
                when contract = 'Month-to-month' then 2
                else 0
            end -- highest risk contract
            + case
                when internet_service = 'Fiber optic' then 1
                else 0
            end + case
                when online_security = 'No' then 1
                else 0
            end + case
                when tech_support = 'No' then 1
                else 0
            end + case
                when payment_method = 'Electronic check' then 1
                else 0
            end + case
                when tenure < 12 then 1
                else 0
            end -- new customers churn more
        ) as risk_score
    from stg
)
select *
from features