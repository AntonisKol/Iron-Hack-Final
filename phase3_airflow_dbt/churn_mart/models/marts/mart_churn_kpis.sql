-- kpi model: aggregated churn metrics for reporting and dashboards
-- this is the final output — one row per contract type with all key metrics
-- built on top of the fact table
with fct as (
    select *
    from {{ ref('fct_churn') }}
),
kpis as (
    select -- group by contract type — biggest churn driver from Phase 2
        contract,
        -- volume
        count(*) as total_customers,
        sum(is_churned) as churned_customers,
        -- churn rate: what % of customers in this group left
        round(sum(is_churned) / count(*) * 100, 2) as churn_rate_pct,
        -- revenue metrics
        round(avg(monthly_charges), 2) as avg_monthly_charges,
        round(avg(clv), 2) as avg_clv,
        -- annual revenue lost to churn
        round(
            sum(
                case
                    when is_churned = 1 then monthly_charges * 12
                    else 0
                end
            ),
            2
        ) as annual_revenue_lost,
        -- risk
        round(avg(risk_score), 2) as avg_risk_score,
        round(avg(tenure), 1) as avg_tenure_months
    from fct
    group by contract
)
select *
from kpis
order by churn_rate_pct desc