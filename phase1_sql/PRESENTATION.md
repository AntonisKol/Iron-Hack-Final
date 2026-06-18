# Phase 1 — SQL & Fraud Analytics
**Presentation Guide**

---

## What Was Asked (Technical Brief)

Write 28 SQL queries against a 1-million-row bank transactions dataset loaded into Snowflake. The queries are organised into 6 modules of increasing complexity: data exploration, customer behaviour analytics, fraud pattern analysis, advanced fraud investigation, window functions, and a capstone reporting challenge. The deliverable is six executable `.sql` files that produce analytical outputs directly in the Snowflake worksheet.

**Dataset:** `bank_fraud.csv` — 1M rows, columns include `transaction_id`, `customer_id`, `country`, `city`, `transaction_amount`, `merchant_category`, `payment_method`, `device_type`, `fraud_type`, `is_fraud`, `failed_attempts`, `is_international`, `is_night_transaction`, `credit_score`, `account_balance`, `age`.

---

## Technical Breakdown — File by File

---

### `00_setup.sql` — Database & Table Initialisation

**Purpose:** One-time setup. Creates the Snowflake environment and loads the raw CSV.

- **CREATE DATABASE / SCHEMA block:** Creates `FRAUD_DB` and `FRAUD_SCHEMA` if they don't already exist. Uses `IF NOT EXISTS` so the script is safe to re-run.
- **CREATE TABLE block:** Defines `BANK_TRANSACTIONS` with all column types (VARCHAR for IDs and categories, FLOAT for amounts, INTEGER for flags).
- **Stage + COPY block:** Creates an internal Snowflake stage, uploads the CSV file to it via `PUT`, then runs `COPY INTO` which bulk-loads all 1M rows in seconds using Snowflake's optimised loader.

---

### `module1_exploration.sql` — Data Profiling (Q1–Q5)

**Purpose:** Understand the shape and quality of the dataset before any analysis.

- **Q1 — Aggregate metrics:** `COUNT(*)` for total rows, `COUNT(DISTINCT customer_id)` for unique customers, `SUM(is_fraud)` for total fraud cases, then a division to compute the overall fraud percentage.
- **Q2 — Top countries:** `GROUP BY country` with `SUM(transaction_amount)` sorted descending and limited to 10. Shows where the most money moves.
- **Q3 — Top cities by value:** Same pattern as Q2 but on `city`. Reveals geographic concentration of high-value transactions.
- **Q4 — Data profiling report:** A multi-column SELECT that computes `COUNT(DISTINCT)` and `SUM(CASE WHEN IS NULL)` for every column — produces a one-row-per-column quality report.
- **Q5 — Distribution by category:** Uses `COUNT(*) / total_count * 100` in subqueries to compute percentage splits for `payment_method`, `device_type`, and `merchant_category`.

---

### `module2_customer.sql` — Customer Behaviour Analytics (Q6–Q10)

**Purpose:** Profile customers — who spends the most, who churns, who's dormant.

- **Q6 — Top 20 by spend:** `GROUP BY customer_id`, `SUM(transaction_amount)`, `ORDER BY DESC`, `LIMIT 20`.
- **Q7 — Avg amount by age group:** `CASE WHEN age BETWEEN 18 AND 25 THEN '18-25' ...` bucketing inside a CTE, then `AVG(transaction_amount)` per bucket.
- **Q8 — Highest fraud-rate age group:** Same bucketing as Q7 but computes `SUM(is_fraud)/COUNT(*)*100` per group instead.
- **Q9 — Dormant accounts:** Filters for `account_balance > AVG(account_balance)` subquery AND `COUNT(transactions) < 5` — high balance, infrequent activity.
- **Q10 — Avg balance and credit score by country:** Simple `GROUP BY country`, `AVG(account_balance)`, `AVG(credit_score)`, ordered by balance.

---

### `module3_fraud_patterns.sql` — Fraud Pattern Analysis (Q11–Q15)

**Purpose:** Identify which transaction attributes correlate with fraud.

- **Q11–Q13 — Fraud rate by category, payment method, device:** Same formula repeated across three dimensions: `SUM(is_fraud) / COUNT(*) * 100 AS fraud_rate` grouped by the dimension column.
- **Q14 — Most common fraud type:** `GROUP BY fraud_type`, `COUNT(*)`, adds `COUNT(*) / SUM(COUNT(*)) OVER () * 100` for percentage using a window function.
- **Q15 — Night vs day / weekend vs weekday:** Uses `DAYOFWEEK()` and `HOUR()` inside CASE expressions to categorise each transaction, then computes fraud rate per category.

---

### `module4_investigation.sql` — Advanced Fraud Investigation (Q16–Q20)

**Purpose:** Deeper queries combining multiple conditions to find high-risk transactions and customers.

- **Q16 — Amount anomalies:** `WHERE transaction_amount > (SELECT AVG(transaction_amount) + 3 * STDDEV(transaction_amount) FROM BANK_TRANSACTIONS)` — classic statistical outlier detection (3-sigma rule).
- **Q17 — Failed attempts + fraud:** `WHERE is_fraud = 1 AND failed_attempts > 3` — customers who both failed multiple times AND committed fraud.
- **Q18 — PIN change and fraud:** `GROUP BY pin_change` (binary flag), computes fraud rate per group to test correlation between recent PIN changes and fraud probability.
- **Q19 — International vs domestic fraud:** `GROUP BY is_international`, computes fraud rate — shows whether cross-border transactions are riskier.
- **Q20 — Multi-factor risk ranking:** `WHERE is_international = 1 AND is_night_transaction = 1 AND failed_attempts > 2`, then `ORDER BY failed_attempts DESC, credit_score ASC` — sorts by most dangerous customers first.

---

### `module5_windows.sql` — Window Functions (Q21–Q24)

**Purpose:** Demonstrate SQL window function syntax for ranking and cumulative analysis.

- **Q21 — RANK() within country:** `RANK() OVER (PARTITION BY country ORDER BY SUM(transaction_amount) DESC)` — restarts rank numbering for each country.
- **Q22 — ROW_NUMBER() top 5 per merchant:** Uses a CTE with `ROW_NUMBER() OVER (PARTITION BY merchant_category ORDER BY transaction_amount DESC)`, then outer query filters `WHERE rn <= 5`.
- **Q23 — Cumulative spend per customer:** `SUM(transaction_amount) OVER (PARTITION BY customer_id ORDER BY transaction_date ROWS UNBOUNDED PRECEDING)` — running total that grows with each transaction.
- **Q24 — NTILE(10) deciles:** `NTILE(10) OVER (ORDER BY total_spend DESC)` divides customers into 10 equal-sized buckets — decile 1 = top 10% spenders.

---

### `module6_capstone.sql` — Executive Reports & Views (Q25–Q28)

**Purpose:** Production-grade queries combining everything learned — CTEs, CASE, windows, views, materialised views.

- **Q25 — Executive fraud report:** A single query using three CTEs (`transaction_kpis`, `fraud_analysis`, `customer_risk`) joined together. Each CTE computes a different summary. The final SELECT pulls from all three into one result row — the kind of query an executive dashboard would run.
- **Q26 — CREATE VIEW:** `CREATE VIEW VW_HIGH_RISK_TRANSACTIONS AS SELECT ... WHERE is_international = 1 AND is_night_transaction = 1 AND failed_attempts > 2` — a reusable saved query the fraud team can call at any time.
- **Q27 — CREATE MATERIALIZED VIEW:** `CREATE MATERIALIZED VIEW MV_DAILY_FRAUD_REPORT AS SELECT transaction_date, COUNT(*), SUM(is_fraud) ... GROUP BY transaction_date` — unlike a view, Snowflake stores the result physically and can refresh it on a schedule. Ideal for dashboards querying the same daily summary repeatedly.
- **Q28 — Suspicious customer detection:** Combines four conditions with AND/OR: above-average transaction amount, international transaction, more than 2 failed attempts, credit score below 600. Outputs a prioritised list for manual review.

---

## For the Room (Non-Technical)

Imagine a bank has one million transaction records sitting in a spreadsheet. Every row is a payment someone made — where, when, how much, from what device. The bank suspects some of those transactions are fraud, but they can't read a million rows by hand.

That's what this phase does. **We ask the database very specific questions**, and it gives us the answers instantly.

For example:
- *"Which countries have the most suspicious transactions?"* — the database counts and ranks them for us.
- *"If someone failed to enter their PIN more than three times and the transaction was still marked as fraud — find those customers."* — we give the database a filter, it returns the list.
- *"Who are the top 10% of spenders?"* — the database sorts everyone, cuts the list into 10 equal groups, and hands us group one.

By the end of this phase, we've built six files full of questions, each one telling a different story about the data. The last file creates permanent **reports and views** — reusable shortcuts so the fraud team doesn't have to retype the same complex question every morning. They just open the dashboard, and the answer is already there.

Think of it like building a really smart filter for a massive haystack. The needle (fraud) was always in there — we just had to write the right questions to find it.
