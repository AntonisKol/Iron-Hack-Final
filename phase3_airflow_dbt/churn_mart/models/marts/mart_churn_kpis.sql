-- Q7: Customer Churn Data Mart — Mart Layer
-- Aggregates fct_churn by contract type to produce business KPIs:
-- churn rate %, average CLV, and annual revenue at risk per contract segment.
-- This is the table the Airflow BI report and dashboards read from.
with fct as (
    select *
    from {{ ref('fct_churn') }}
),
kpis as (
    select
        contract,
        count(*) as total_customers,
        sum(is_churned) as churned_customers,
        round(sum(is_churned) / count(*) * 100, 2) as churn_rate_pct,
        round(avg(monthly_charges), 2) as avg_monthly_charges,
        round(avg(clv), 2) as avg_clv,
        round(
            sum(case when is_churned = 1 then monthly_charges * 12 else 0 end),
            2
        ) as annual_revenue_lost,
        round(avg(risk_score), 2) as avg_risk_score,
        round(avg(tenure), 1) as avg_tenure_months
    from fct
    group by contract
)
select *
from kpis
order by churn_rate_pct desc
