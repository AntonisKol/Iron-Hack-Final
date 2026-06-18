# Phase 1 — SQL & Fraud Analytics
**Presentation Guide**

---

## What Was Asked (Technical Brief)

Write 28 SQL queries against a 1-million-row bank transactions dataset loaded into Snowflake. The queries are organised into 6 modules of increasing complexity: data exploration, customer behaviour analytics, fraud pattern analysis, advanced fraud investigation, window functions, and a capstone reporting challenge. The deliverable is six executable `.sql` files that produce analytical outputs directly in the Snowflake worksheet.

**Dataset:** `bank_fraud.csv` — 1M rows. Columns include `transaction_id`, `customer_id`, `country`, `city`, `transaction_amount`, `merchant_category`, `payment_method`, `device_type`, `fraud_type`, `is_fraud` (0/1), `failed_attempts`, `is_international` (0/1), `is_night_transaction` (0/1), `credit_score`, `account_balance`, `customer_age`.

---

## Technical Breakdown — File by File

---

### `00_setup.sql` — Database & Table Initialisation

One-time setup that creates the Snowflake environment and loads the raw CSV. `CREATE DATABASE IF NOT EXISTS FRAUD_DB` and `CREATE SCHEMA IF NOT EXISTS FRAUD_SCHEMA` establish the namespace — the `IF NOT EXISTS` guards make the script safe to re-run without errors. `CREATE TABLE BANK_TRANSACTIONS` defines all column types. An internal Snowflake stage is then created, the CSV is uploaded via `PUT`, and `COPY INTO BANK_TRANSACTIONS FROM @stage` bulk-loads all 1M rows using Snowflake's parallel loader — far faster than row-by-row inserts.

---

### `module1_exploration.sql` — Data Profiling (Q1–Q5)

**Q1 — Aggregate metrics:** A single SELECT combining `COUNT(*)` for total transactions, `COUNT(DISTINCT customer_id)` for unique customers (deduplicating the million rows to just unique people), `SUM(is_fraud)` which works because `is_fraud` is a 0/1 integer column so its sum equals the count of fraud cases, and `ROUND(SUM(is_fraud)/COUNT(*)*100, 2)` for the overall fraud rate.

**Q2 — Top 10 countries by volume:** `GROUP BY country`, `COUNT(*) AS transaction_count`, `ORDER BY transaction_count DESC`, `LIMIT 10`. Groups all rows by country and counts them, then surfaces the ten busiest.

**Q3 — Top 10 cities by total value:** Same pattern as Q2 but aggregates `SUM(transaction_amount)` rather than count — reveals which cities move the most money, not just which have the most transactions.

**Q4 — Data profiling report:** A 15-block `UNION ALL` query, one block per column. Each block computes `COUNT(DISTINCT ...)` for cardinality and `SUM(CASE WHEN IS NULL THEN 1 ELSE 0 END)` for null count. The result is one row per column, giving a full quality report across the entire schema. No data quality issues were found.

**Q5 — Percentage distribution:** Three `UNION ALL` blocks — one each for `payment_method`, `device_type`, `merchant_category`. Each uses a window function `SUM(COUNT(*)) OVER ()` as the denominator so the percentage is computed in a single pass rather than a self-join.

---

### `module2_customer.sql` — Customer Behaviour Analytics (Q6–Q10)

**Q6 — Top 20 customers by spend:** `GROUP BY customer_id`, `SUM(transaction_amount)`, `COUNT(*)`, `ORDER BY total_amount DESC`, `LIMIT 20`. Identifies the highest-value customers by total money moved.

**Q7 — Average transaction by age group:** `CASE WHEN customer_age BETWEEN 18 AND 25 THEN '18-25' ...` bucketing inside the SELECT creates five age bands. `AVG(transaction_amount)` per band sorted descending reveals which age group transacts at the highest average value.

**Q8 — Highest fraud-rate age group:** Same age bucketing as Q7 but the metric changes to `SUM(is_fraud)*100.0/COUNT(*)` — the fraud rate per group. Shows whether fraud risk varies by customer age.

**Q9 — Dormant high-balance accounts:** Uses a `HAVING` clause with two correlated subqueries. `AVG(account_balance) > (SELECT AVG(account_balance) FROM BANK_TRANSACTIONS)` keeps only customers with above-average balances. `AVG(transaction_freq_monthly) < (SELECT AVG(...))` further filters to those who transact infrequently. The intersection is high-net-worth customers who barely use their accounts — a classic dormancy signal.

**Q10 — Balance and credit score by country:** `GROUP BY country`, `AVG(account_balance)`, `AVG(credit_score)`, sorted by balance. Country-level financial health overview in one query.

---

### `module3_fraud_patterns.sql` — Fraud Pattern Analysis (Q11–Q15)

**Q11 — Fraud rate by merchant category:** `GROUP BY merchant_category`, `SUM(is_fraud)/COUNT(*)*100 AS fraud_rate`, `ORDER BY fraud_rate DESC`. Reveals which merchant types are disproportionately targeted.

**Q12 — Fraud rate by payment method:** Same formula, `GROUP BY payment_method`. Identifies whether certain payment channels carry higher fraud risk.

**Q13 — Fraud rate by device type:** Same pattern, `GROUP BY device_type`. Tests whether mobile, desktop, or other device types correlate with fraud.

**Q14 — Most common fraud type:** Filters `WHERE is_fraud = 1` to look only at confirmed fraud, then `GROUP BY fraud_type`, `COUNT(*)`, plus a window function `COUNT(*)*100.0/SUM(COUNT(*)) OVER ()` for percentage of total fraud cases. Results sorted by count to identify the dominant fraud pattern.

**Q15 — Timing analysis:** Two `CASE WHEN` expressions in the `SELECT` — one converting `is_night_transaction` to 'Night'/'Day', one converting `is_weekend` to 'Weekend'/'Weekday' — create a 2×2 grouping. Fraud rate is computed for each of the four combinations.

---

### `module4_investigation.sql` — Advanced Fraud Investigation (Q16–Q20)

**Q16 — Statistical anomalies:** `WHERE transaction_amount > (SELECT AVG(transaction_amount) + 3*STDDEV(transaction_amount) FROM BANK_TRANSACTIONS)` — the 3-sigma rule. Transactions more than three standard deviations above the mean are statistically rare and warrant investigation. A `CASE WHEN is_fraud = 1` labels each result as confirmed or unconfirmed.

**Q17 — Failed attempts with confirmed fraud:** `WHERE is_fraud = 1 AND failed_attempts > 3` — customers who both failed multiple authentication attempts and whose transaction was confirmed fraud. Both conditions must be true simultaneously.

**Q18 — PIN change correlation:** `GROUP BY pin_changed_recently` (0/1 flag), computes fraud rate per group. Tests whether a recently changed PIN is a leading indicator of fraud — account takeover attacks often begin with a PIN reset.

**Q19 — International vs domestic:** `GROUP BY` on a `CASE WHEN is_international = 1` expression, computes fraud rate. Quantifies how much riskier cross-border transactions are versus domestic ones.

**Q20 — Multi-factor risk ranking:** `WHERE is_international = 1 AND is_night_transaction = 1 AND failed_attempts > 2` — all three conditions must be met. Then `RANK() OVER (ORDER BY SUM(is_fraud) DESC)` assigns a risk rank to each qualifying customer. The most dangerous customer appears at rank 1.

---

### `module5_windows.sql` — Window Functions (Q21–Q24)

**Q21 — RANK() within country:** `RANK() OVER (PARTITION BY country ORDER BY SUM(transaction_amount) DESC)` — `PARTITION BY` restarts the rank counter for each country. Customer ranked 1 in Germany is the top spender in Germany, independent of what the top UK spender spent.

**Q22 — Top 5 per merchant category:** A subquery applies `ROW_NUMBER() OVER (PARTITION BY merchant_category ORDER BY transaction_amount DESC)`. The outer query filters `WHERE row_num <= 5`. This returns the five highest-value transactions for each merchant type.

**Q23 — Cumulative spend per customer:** `SUM(transaction_amount) OVER (PARTITION BY customer_id ORDER BY transaction_date)` — a running total that grows with each chronological transaction per customer. Row 3 for a customer shows their total spend after three transactions.

**Q24 — Deciles with NTILE:** `NTILE(10) OVER (ORDER BY SUM(transaction_amount) DESC)` divides all customers into 10 equal-sized buckets. A `CASE WHEN` maps decile numbers to labels: Top 10%, Top 20%, etc. Customers in decile 1 are the top 10% by total spend.

---

### `module6_capstone.sql` — Executive Reports & Views (Q25–Q28)

**Q25 — Executive fraud report:** A single query using four CTEs chained together. `transaction_kpis` computes overall volume and average. `fraud_kpis` computes total fraud count, fraud percentage, and most common fraud type via a `MODE()` subquery. `risk_kpis` uses `FIRST_VALUE() OVER (ORDER BY SUM(is_fraud) DESC)` to find the highest-risk country, merchant category, and payment method in a single pass. `customer_kpis` profiles confirmed fraudsters specifically. The final `SELECT` cross-joins all four CTEs into one summary row — a single-query executive dashboard.

**Q26 — VIEW for the fraud team:** `CREATE OR REPLACE VIEW VW_HIGH_RISK_TRANSACTIONS AS SELECT ... WHERE is_international = 1 AND is_night_transaction = 1 AND failed_attempts > 2`. The view is a saved query — every time the fraud team calls it, it executes against the latest data. No one needs to remember the filter logic.

**Q27 — MATERIALIZED VIEW for daily reporting:** `CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_FRAUD_REPORT AS SELECT transaction_date, COUNT(*), SUM(is_fraud), fraud_rate, total_amount FROM BANK_TRANSACTIONS GROUP BY transaction_date`. Unlike a regular view, Snowflake stores the result physically and refreshes it automatically. Dashboard queries hit the pre-aggregated result instead of scanning 1M rows each time — dramatically faster.

**Q28 — Suspicious customer detection:** Four conditions combined: `is_international = 1`, `failed_attempts > 2`, `credit_score < 600`, `transaction_amount > (SELECT AVG(...))`. All four must be true. Results sorted by confirmed fraud count descending — the most dangerous verified customers appear first for manual review.

---

## For the Room — Plain-Language Walkthrough

---

### Module 1 — Getting to Know the Data (Q1–Q5)

**Q1 — The First Number That Matters**

Before you do anything with a million rows of bank data, you need a single number to anchor the whole conversation: how bad is the fraud problem? Q1 delivers exactly that. One query, running in seconds, comes back with: total transactions processed, how many unique customers that covers, how many were confirmed fraud, and what percentage that represents. If the answer is 0.5%, you're dealing with a needle-in-a-haystack problem. If it's 12%, you have a crisis. That number frames every analysis that follows.

**Q2 — Where Is the Money Moving?**

With a million transactions spanning multiple countries, the first geographic question is simple: which countries generate the most activity? Q2 counts transactions by country and returns the top ten, sorted from busiest to quietest. This tells the business which markets are the most active — and sets the stage for finding out whether those same markets also have the highest fraud rates.

**Q3 — Which Cities Have the Biggest Transactions?**

Q2 counted how many transactions happened per country. Q3 asks a different question: where is the money the largest? It sums up the total value of every transaction by city and returns the top ten. A city might rank tenth in transaction count but first in total value — which means it has a small number of very large payments. That kind of insight is what changes where a fraud team focuses its attention.

**Q4 — Is the Data Clean?**

You cannot trust analysis done on broken data. Q4 is a systematic health check: for every single column in the dataset — all fifteen of them — it asks two questions: how many distinct values does it have, and how many cells are blank? The output is a clean summary table. In this case, the result was reassuring: no data quality issues found. But running this check is the responsible thing to do before drawing any conclusions, and skipping it is how analytical errors make their way into business decisions.

**Q5 — How Are People Paying and What Are They Buying?**

This question breaks down the entire dataset by three categories — payment method, device type, and merchant category — and shows what percentage of all transactions falls into each group. The output reveals the normal distribution: maybe 40% of transactions are by debit card, 25% by credit card, 15% by bank transfer. Once you know what is "normal," anything that deviates from it becomes interesting. If a particular combination shows up far more in fraud cases than its baseline share would predict, that combination is worth flagging.

---

### Module 2 — Understanding the Customers (Q6–Q10)

**Q6 — Who Are the Big Spenders?**

Q6 is about identifying the customers who matter most to the bank's revenue. It adds up every transaction per customer and returns the top twenty by total spend. These are the people who drive a disproportionate share of the bank's business — and also the people whose accounts, if compromised, would cause the most damage. Knowing who they are is the first step toward giving them appropriate attention.

**Q7 — Do Different Ages Spend Differently?**

Customers range from 18 to 65+, and Q7 asks whether age affects how much people spend per transaction. It slices the customer base into five age bands — 18–25, 26–35, 36–50, 51–65, 65+ — and computes the average transaction amount for each. The pattern this surfaces matters for product design: younger customers might be doing many small mobile payments while older customers might be making fewer but larger transactions. Each group needs a different approach.

**Q8 — Which Age Group Is Most at Risk of Fraud?**

Q7 looked at spending. Q8 asks the same question but focuses on fraud rates. The same five age bands are computed, but this time the metric is the proportion of transactions in each group that turned out to be fraud. Age can be a significant fraud predictor — and knowing which group needs the most protection informs where to invest in security measures and customer education.

**Q9 — The Quiet Rich: Finding Dormant High-Value Accounts**

Some of the most interesting customers in any bank are the ones with large balances who almost never transact. Q9 finds them: customers whose average account balance is above the overall average, but whose monthly transaction frequency is below average. This group is interesting for two reasons. First, their inactivity means they might not notice if someone else was using their account. Second, they represent untapped business — a relationship manager conversation might reactivate significant assets.

**Q10 — Country-Level Financial Profile**

Q10 steps back from individual customers to paint a picture at the country level: what is the average account balance and average credit score of customers in each country? This tells the business not just where the transactions are happening, but where the financial health of those customers sits. A country with high balances and low credit scores is a different risk profile from one with the reverse pattern.

---

### Module 3 — Where Does Fraud Concentrate? (Q11–Q15)

**Q11 — Which Merchant Types See the Most Fraud?**

Not all shops are equally safe. Q11 calculates the fraud rate — confirmed fraud as a percentage of all transactions — for every merchant category in the dataset: electronics, travel, food, health, and others. The ranking that comes out of this tells the fraud team where to focus detection rules. If electronics transactions have a 3% fraud rate and food transactions have 0.2%, those two categories need very different levels of scrutiny.

**Q12 — Does the Payment Method Matter?**

Q12 runs the same calculation but across payment methods: credit card, debit card, bank transfer, cryptocurrency, and so on. Some payment channels offer stronger authentication than others, and this shows up in the fraud rate. The result gives the bank a clear signal about which payment methods need tighter controls — and which are already working well.

**Q13 — Phone, Laptop, or Something Else?**

Device type is a third lens on the same question. Q13 computes fraud rates by device — mobile, desktop, tablet, ATM, and others. Certain devices have weaker authentication or are more commonly used in automated fraud attacks. Knowing which device type correlates with fraud helps inform where to invest in two-factor authentication and behavioural monitoring.

**Q14 — What Kind of Fraud Is Most Common?**

Q14 filters down to only the confirmed fraud cases and asks: what are they? Identity theft? Card cloning? Phishing? Account takeover? It counts each fraud type and expresses each as a percentage of all fraud. The most common type is the one that deserves the most investment in detection and prevention. The least common might still be worth investigating if it involves unusually large amounts.

**Q15 — Is Fraud Worse at Night or on Weekends?**

Timing matters. Q15 splits all transactions into four categories — night/weekday, night/weekend, day/weekday, day/weekend — and computes the fraud rate for each. The hypothesis being tested is straightforward: fraud is harder to detect when fewer people are watching. If night-weekend transactions show a significantly higher fraud rate, that's a strong signal to increase automated monitoring intensity during those hours.

---

### Module 4 — Digging Deeper: Finding the Suspects (Q16–Q20)

**Q16 — The Suspiciously Large Transaction**

Statistics has a concept called "3 sigma" — if something is more than three standard deviations above average, it is statistically unusual. Q16 applies this to transaction amounts. It first calculates the average and standard deviation across all million transactions, then returns every transaction that exceeds that threshold. Each is then labelled: was it confirmed fraud, or just flagged as suspicious without a fraud confirmation? This is anomaly detection in its simplest, most interpretable form — no machine learning required.

**Q17 — Failed Three Times and Still Went Through**

Q17 looks for a very specific red flag: customers who failed authentication more than three times on a transaction that was ultimately confirmed as fraud. This combination — repeated failures followed by a fraudulent success — is a classic signature of a brute-force attack or someone trying different stolen card details. The query finds every customer who triggered this pattern and lists their worst-case failure count.

**Q18 — Did a PIN Change Predict the Fraud?**

Account takeover attacks often follow a predictable playbook: steal credentials, change the PIN to lock out the real owner, then drain the account. Q18 tests this hypothesis: are customers who recently changed their PIN more likely to have fraudulent transactions? It groups all transactions into two buckets — PIN changed recently, PIN not changed — and compares their fraud rates. If the "recently changed" group has a significantly higher rate, that flag should become a trigger for extra verification.

**Q19 — Cross-Border Transactions Are Riskier**

Q19 provides the simplest test in the investigation module: comparing fraud rates between international and domestic transactions. The question almost answers itself intuitively — cross-border transactions are harder to verify in real time, harder for the customer to challenge quickly, and more likely to involve stolen card details used far from the cardholder's home. Q19 quantifies exactly how much riskier they are.

**Q20 — The Highest-Risk Customer in the Dataset**

Q20 combines three risk factors simultaneously: the transaction was international, it happened at night, and the customer failed authentication more than twice before it went through. Any one of these alone might be explainable. All three together in the same transaction is a serious signal. Q20 finds every customer who appeared in this intersection, counts their confirmed frauds, and ranks them — the most dangerous customer sits at rank one.

---

### Module 5 — Ranking and Running Totals (Q21–Q24)

**Q21 — Who Is the Top Spender in Each Country?**

Q21 uses a SQL window function to rank customers within each country by their total spend. The key concept is that the ranking resets per country: the number-one spender in Germany is ranked first in Germany, regardless of whether they would rank tenth globally. This produces a leaderboard for each market simultaneously, in one query, without needing to run a separate query per country.

**Q22 — The Five Biggest Transactions in Each Shop Category**

Q22 uses a similar technique to find the top five highest-value transactions in each merchant category — electronics, travel, food, and so on. A numbering function counts each transaction within its category from largest to smallest. Then the outer query keeps only the top five per category. This is useful for understanding what a "large" transaction looks like in each context — a £5,000 food delivery is suspicious; a £5,000 electronics purchase might be a television.

**Q23 — How Much Has Each Customer Spent Over Time?**

Q23 computes a running total: for each customer, every transaction adds to a cumulative sum, in chronological order. The first transaction shows the amount of that transaction. The second shows the combined total of the first two. By the tenth transaction, you can see exactly how much a customer has contributed since they joined. This is the basis for calculating lifetime value — and for spotting customers who suddenly stopped transacting after building up a history.

**Q24 — Sorting Customers into Ten Equal Groups**

Q24 divides all customers into ten equal-sized buckets based on total spend — a technique called decile analysis. The top 10% of spenders are in decile one. The bottom 10% are in decile ten. A `CASE WHEN` maps those numbers to readable labels: Top 10%, Top 20%, and so on. This is how banks build tiered customer service models — gold-tier customers get a personal manager, standard-tier customers use the app.

---

### Module 6 — Reports the Business Can Actually Use (Q25–Q28)

**Q25 — The One-Row Executive Summary**

Q25 is the showpiece query of the phase. It produces a single row of output that answers every executive question at once: How many transactions? What is the total value? What is the fraud rate? What is the most common fraud type? Which country has the most fraud? Which merchant and payment method are highest risk? What does the average fraudster look like in terms of credit score, balance, and failed attempts? It achieves this by running four separate mini-analyses in parallel — called CTEs, or Common Table Expressions — and then combining them into one output row. This is the kind of query a daily fraud dashboard runs every morning.

**Q26 — The Fraud Team's Shortcut**

Instead of typing the same complex filter every day — international, night-time, more than two failed attempts — Q26 saves it as a view. A database view is a named, saved query. Every time the fraud team wants to see the current list of high-risk transactions, they just say "show me VW_HIGH_RISK_TRANSACTIONS" and the database runs the full filter automatically against whatever data is current. No copy-pasting, no risk of a typo changing the filter, always up to date.

**Q27 — The Fast Dashboard**

The materialised view in Q27 takes the idea one step further. A regular view runs its query every time you open it — which is fine for small datasets but slow against a million rows. A materialised view pre-computes the result and stores it physically. Snowflake refreshes it automatically. When a dashboard asks for the daily fraud report, it reads from a pre-built summary table instead of crunching a million rows in real time. The dashboard loads instantly. The business gets the same answer. The query runs once per day instead of hundreds of times.

**Q28 — Building the Suspect List**

The final query is the most targeted: it combines four specific risk factors — international transaction, more than two failed authentication attempts, credit score under 600, above-average transaction amount — and finds every customer who meets all four criteria simultaneously. The result is not a broad population or a statistical average; it is a specific named list of customers, sorted by number of confirmed fraud cases descending. This list goes directly to the fraud team. They start making calls at the top.
