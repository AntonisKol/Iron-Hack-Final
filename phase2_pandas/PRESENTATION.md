# Phase 2 — Pandas & Exploratory Data Analysis
**Presentation Guide**

---

## What Was Asked (Technical Brief)

Perform exploratory data analysis (EDA) on a telecom customer churn dataset using Python and Pandas. The work is split across five Jupyter notebooks covering 24 analytical questions: data quality and profiling, customer segmentation, revenue analysis, service adoption intelligence, and advanced feature engineering. The deliverable is five self-contained notebooks with code, output, and brief commentary for each question.

**Dataset:** `telecom_churn.csv` — 7,043 rows representing individual telecom subscribers. Key columns: `tenure` (months as customer), `Contract`, `PaymentMethod`, `InternetService`, `MonthlyCharges`, `TotalCharges` (stored incorrectly as string), `Churn` (Yes/No target label).

---

## Technical Breakdown — Notebook by Notebook

---

### `module1_data_quality.ipynb` — Data Profiling & Cleaning (Q1–Q4)

**Purpose:** Understand what the data looks like before touching it. Fix known data quality issues.

**Q1 — Data quality report function**
Defines `data_quality_report(df)`. For every column it computes: `dtype`, `null_count`, `null_percentage` (nulls divided by total rows × 100), `distinct_values` (cardinality), and five representative `sample_values`. Returns a `pd.DataFrame` — one row per column. This is the first thing any analyst runs on a new dataset.

**Q2 — TotalCharges type fix**
`df['TotalCharges']` is stored as `object` (string) because 11 rows contain a blank space instead of a number. `pd.to_numeric(errors='coerce')` converts the column to float and silently turns the blanks into `NaN`. The code then counts how many became NaN and reports them — those are new customers with zero charges, not missing data.

**Q3 — Column categorisation function**
Defines `categorize_columns(df)` which loops over every column and uses `nunique()` to sort it into one of four buckets: `identifier` (unique-per-row like IDs), `binary` (only 2 distinct values), `categorical` (few values, not numeric), or `numerical` (continuous numbers). Returns a dictionary of four lists — a reusable schema map.

**Q4 — TotalCharges vs MonthlyCharges × tenure**
Adds `expected_total = MonthlyCharges × tenure` and computes `diff_percentage = abs(actual - expected) / expected × 100`. Filters rows where this exceeds 10%. These are customers whose billing history doesn't match the formula — could indicate promotions, mid-contract changes, or data entry errors.

---

### `module2_customer_analytics.ipynb` — Customer Segmentation (Q6–Q10)

**Purpose:** Understand who churns, when, and what characteristics they share.

**Q6 — Churn by demographic dimension**
Loops over `['gender', 'SeniorCitizen', 'Partner', 'Dependents']`. For each, uses `groupby(dim)['Churn'].apply(lambda x: (x == 'Yes').sum() / len(x) * 100)`. Produces a rate per group so demographics can be compared directly.

**Q7 — Tenure segmentation with pd.cut()**
`pd.cut(df['tenure'], bins=[0,12,24,48,72], labels=['New','Growing','Mature','Loyal'])` assigns each customer to a 12-month bucket. Then `groupby('tenure_segment')['Churn'].apply(...)` shows churn rate rising steeply for New customers and falling for Loyal — validates that early retention is critical.

**Q8 — Top 10 high-churn profiles**
Groups on three columns simultaneously: `Contract × PaymentMethod × InternetService`. Computes churn rate per combination, sorts descending. The top rows reveal the exact customer persona most likely to leave — e.g. Month-to-month + Electronic check + Fiber optic.

**Q9 — Customer Lifetime Value (CLV)**
`df['CLV'] = df['MonthlyCharges'] * df['tenure']` — total money a customer has paid over their life with the company. `groupby('Churn').agg(['mean','median','min','max'])` compares CLV distribution for churned vs retained. Churned customers have significantly lower CLV, confirming they leave early.

**Q10 — Risk score function**
`calculate_risk_score(row)` uses a point system: Month-to-month contract +3, One year +1, tenure < 12 months +2, no TechSupport +1, no OnlineSecurity +1. Maximum score: 7. Applied via `df.apply(calculate_risk_score, axis=1)`. Higher scores = customers the retention team should contact first.

---

### `module3_revenue.ipynb` — Revenue Analysis (Q11–Q15)

**Purpose:** Quantify the financial impact of churn and identify high-value segments.

**Q11 — Annualised revenue loss**
`churned_df['MonthlyCharges'].sum() × 12` — if all currently-churned customers had stayed one more year, this is how much revenue the company would have kept. Provides a dollar figure to justify investment in retention programmes.

**Q12 — Pareto analysis**
Sorts customers by `TotalCharges` descending. Computes `cumsum() / total_revenue` as a running percentage. Finds the customer index where cumulative revenue crosses 80%, then checks what percentage of the customer base that represents. If it's below 20%, the Pareto principle holds.

**Q13 — ARPU by segment**
`groupby('Contract')['MonthlyCharges'].mean()` — Average Revenue Per User broken down by contract type, internet service type, and payment method. Shows that two-year contract customers pay less per month but stay longer, making them more valuable in total.

**Q14 — Revenue leakage**
Filters: `MonthlyCharges > mean (above-average payer)` AND `tenure < 25th percentile (short tenure)` AND `Churn == Yes`. These are customers who paid a lot, barely stayed, and then left — the highest-value churn events. The report lists them so the business can investigate why.

**Q15 — Statistical outlier payers**
`threshold = mean + 1.5 × std`. Customers above this line are paying unusually high charges relative to the rest of the base. Can indicate loyalty programme gaps or pricing anomalies that inflate churn risk.

---

### `module4_service_intelligence.ipynb` — Service Adoption & Churn (Q16–Q20)

**Purpose:** Understand which services retain customers and which bundles are most profitable.

**Q16 — Lowest churn service combination**
`groupby(['InternetService', 'TechSupport'])['Churn'].apply(...)` — finding which combination of internet service and tech support has the lowest churn rate. Reveals that DSL + TechSupport Yes is significantly safer than Fiber optic + No support.

**Q17 — Services associated with churn reduction**
Loops over six add-on columns (`OnlineSecurity`, `OnlineBackup`, `DeviceProtection`, `TechSupport`, `StreamingTV`, `StreamingMovies`). For each, computes churn rate with and without the service and records the difference. Ranks services by how much they reduce churn.

**Q18 — Service Adoption Score**
`df['adoption_score'] = sum of 1 for each service == 'Yes'` across the six add-ons. Score ranges 0–6. `groupby('adoption_score')['Churn'].mean()` shows a clear negative correlation — more services = lower churn rate. Customers who buy into the full bundle are the most sticky.

**Q19 — High-risk cohort analysis**
Applies three simultaneous filters: `InternetService == 'Fiber optic'` AND `TechSupport == 'No'` AND `OnlineSecurity == 'No'`. This cohort has the highest churn rate in the dataset — they have the most demanding service but zero support. The output quantifies how large this group is and how dangerous it is.

**Q20 — Most profitable bundle**
Creates a `bundle` label per customer by joining the names of all active services. `groupby('bundle')['TotalCharges'].mean()` reveals which specific combination of services generates the highest average total charge per customer over their lifetime.

---

### `module5_advanced_challenges.ipynb` — Advanced Feature Engineering (Q21–Q24)

**Purpose:** Prepare data for machine learning and build interpretable risk tools.

**Q21 — ML-ready feature matrix**
Converts the raw dataset into a fully numerical table a model can consume. Binary columns (`Churn`, `SeniorCitizen`, `Partner`, etc.) are mapped to 0/1. Categorical columns with multiple values (`Contract`, `PaymentMethod`, `InternetService`) are one-hot encoded with `pd.get_dummies()`. Numeric columns (`tenure`, `MonthlyCharges`, `TotalCharges`) are kept as-is. Result: a clean matrix with no strings.

**Q23 — Retention KPI dashboard dataset**
Builds a summary dictionary then converts it to a one-row DataFrame: `total_customers`, `churned_customers`, `retained_customers`, `churn_rate_pct`, `avg_tenure_months`, `avg_clv`. Broken down by contract type. This is the data that would power a management dashboard.

**Q24 — Rule-based churn risk engine**
`churn_risk_level(row)` scores each customer 0–9 using four weighted rules: contract type (0/1/3), tenure (0/2), add-on adoption (0–3), and payment method (0/1). Maps the score to `Low (0–2)` / `Medium (3–4)` / `High (5–6)` / `Critical (7+)`. Applied to the whole dataset with `df.apply(...)`. Produces an actionable risk list — the retention team calls Critical customers first.

---

## For the Room (Non-Technical)

Imagine a phone company with 7,000 customers. They've noticed that a lot of people are cancelling their contracts. The question is: **why are they leaving, and can we predict who's going to leave next?**

This phase is about sitting down with a spreadsheet of all those customers — looking at their age, what plan they're on, how long they've been with the company, how much they pay — and finding patterns.

The first thing we do is **clean the data**. Imagine someone filled in the spreadsheet wrong — some cells have a blank space instead of a number. We fix that before we start, otherwise our calculations would be wrong.

Then we start asking questions:

- *Are older people more likely to leave than younger ones?* (They're not — being a senior citizen makes almost no difference.)
- *Does having a month-by-month contract vs a yearly one matter?* (Huge difference — month-by-month customers are three times more likely to quit.)
- *How much money is walking out the door every year?* (We calculate it: `monthly charge × 12 × number of churned customers`.)
- *What's the one service that retains people the most?* (Technical support — customers with tech support barely leave.)

By the end, we build a **score card**. Every customer gets a number from 0 to 9 — the higher the score, the more likely they are to cancel. The sales team can then focus their calls on the people with the highest scores.

It's like a doctor looking at a patient's health chart and saying *"this person is at high risk of a heart attack — let's intervene now."* Same logic, different industry.

---

**One thing worth knowing about the tools:** We did all of this in Python, inside a Jupyter notebook. A Jupyter notebook is like a Word document that can also run code — so you write a few lines, press play, and the result appears right below it. You see the data and the analysis side by side. That's why it's popular for this kind of exploratory work.
