# Phase 2 — Pandas & Exploratory Data Analysis
**Tool:** Python 3 · Pandas · NumPy | **Dataset:** Telecom Customer Churn (7,043 rows)

## Dataset
`data/telecom_churn.csv` — telecom customers with demographics, services subscribed, charges, and churn outcome.

Key columns: `customerID`, `gender`, `SeniorCitizen`, `Partner`, `Dependents`, `tenure`, `Contract`,  
`PaymentMethod`, `InternetService`, `MonthlyCharges`, `TotalCharges`, `Churn`

---

## Folder Structure
```
phase2_pandas/
├── data/
│   └── telecom_churn.csv               ← 7,043 customer records
├── notebooks/
│   ├── module1_data_quality.ipynb      ← Q1–Q4:  data profiling & cleaning
│   ├── module2_customer_analytics.ipynb← Q6–Q10: customer segmentation & CLV
│   ├── module3_revenue.ipynb           ← Q11–Q15: revenue & Pareto analysis
│   ├── module4_service_intelligence.ipynb ← Q16–Q20: service adoption & churn
│   └── module5_advanced_challenges.ipynb  ← Q21–Q24: ML features & risk engine
└── README.md
```

## How to Run
```bash
cd phase2_pandas
jupyter notebook
```
Each notebook reads `../data/telecom_churn.csv` relative to the `notebooks/` folder. Run them in order — later modules reuse the `TotalCharges` fix from Module 1.

---

## Module 1 — Data Quality & Profiling (Q1–Q4)

| Q | Task | Key Technique |
|---|------|--------------|
| Q1 | Data quality report: dtype, null count/%, distinct values, sample values per column | `pd.DataFrame` of column-level stats |
| Q2 | Fix `TotalCharges` stored as object (blank strings coerced to `NaN`, then cast to float) | `pd.to_numeric(errors='coerce')` |
| Q3 | Classify every column as `numerical` / `categorical` / `binary` / `identifier` | `nunique()` thresholds |
| Q4 | Flag customers where `TotalCharges` deviates >10% from `MonthlyCharges × tenure` | Vectorised `abs(actual − expected) / expected` |

---

## Module 2 — Customer Analytics (Q6–Q10)

| Q | Task | Key Technique |
|---|------|--------------|
| Q6 | Churn rate broken down by Gender, SeniorCitizen, Partner, Dependents | `groupby().apply(lambda)` |
| Q7 | Segment customers by tenure (New 0–12m / Growing 12–24m / Mature 24–48m / Loyal 48–72m) and compare churn per segment | `pd.cut()` + `groupby` |
| Q8 | Top 10 multi-dimension customer profiles most likely to churn (Contract × PaymentMethod × InternetService) | `groupby` three columns + `sort_values` |
| Q9 | Customer Lifetime Value `CLV = MonthlyCharges × tenure`, compare mean CLV for churned vs retained | `groupby('Churn').agg()` |
| Q10 | Risk score function: points awarded for Month-to-month contract, low tenure, no TechSupport, no OnlineSecurity | Row-wise `apply()` |

---

## Module 3 — Revenue Analysis (Q11–Q15)

| Q | Task | Key Technique |
|---|------|--------------|
| Q11 | Annualised revenue loss from churn — `SUM(MonthlyCharges) × 12` for churned customers | Filter + `sum()` |
| Q12 | Pareto analysis — do the top 20% of customers by revenue generate 80% of total revenue? | Cumulative revenue `cumsum()` |
| Q13 | ARPU (Average Revenue Per User) segmented by Contract type, Internet service, Payment method | `groupby().mean()` |
| Q14 | Revenue leakage report: churned customers with above-average charges and below-25th-percentile tenure | Multi-condition boolean filter |
| Q15 | Flag customers paying more than 1.5 standard deviations above the mean MonthlyCharge | `mean()` + `std()` threshold |

---

## Module 4 — Service Intelligence (Q16–Q20)

| Q | Task | Key Technique |
|---|------|--------------|
| Q16 | Find the InternetService × TechSupport combination with the lowest churn rate | `groupby(['InternetService', 'TechSupport'])` + `apply` |
| Q17 | Rank each add-on service by its association with churn reduction (churn rate with vs without the service) | Loop over service columns, compare group means |
| Q18 | Service Adoption Score (count of subscribed add-ons per customer) and its correlation with churn | Binary column sum per row + `groupby` |
| Q19 | Churn rate for the highest-risk cohort: Fiber optic + no TechSupport + no OnlineSecurity | Boolean mask + `.mean()` |
| Q20 | Most profitable service bundle: identify the bundle label with the highest average TotalCharges | `apply()` row label → `groupby` + `mean()` |

---

## Module 5 — Advanced Challenges (Q21–Q24)

| Q | Task | Key Technique |
|---|------|--------------|
| Q21 | Build an ML-ready feature matrix: binary-encode Yes/No columns, label-encode categoricals, keep numerics as-is | `map({'Yes':1,'No':0})` + `pd.get_dummies()` |
| Q23 | Retention KPI dashboard dataset: total / churned / retained customers, churn rate, avg tenure, avg CLV by contract type | Dict-of-aggregates → `pd.DataFrame` |
| Q24 | Rule-based churn risk engine: scores each customer 0–9 across four dimensions, assigns Low / Medium / High / Critical label | `apply(row_func)` with scoring rubric |
