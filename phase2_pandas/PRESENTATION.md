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

**Q1 — Data quality report function**
Defines `data_quality_report(df)`. For every column it computes: `dtype`, `null_count`, `null_percentage` (nulls / total rows × 100), `distinct_values` (cardinality), and five representative `sample_values`. Returns a `pd.DataFrame` — one row per column. This is the first pass any analyst runs on a new dataset — it surfaces hidden issues before they corrupt downstream calculations.

**Q2 — TotalCharges type fix**
`df['TotalCharges']` is stored as `object` (string) because 11 rows contain a blank space instead of a number. `pd.to_numeric(errors='coerce')` converts the column to `float64` and silently turns the blanks into `NaN`. The code then counts how many became NaN — those are new customers with zero charges, not genuinely missing data. Without this fix, any arithmetic on `TotalCharges` would either throw an error or produce a silently wrong answer.

**Q3 — Column categorisation function**
Defines `categorize_columns(df)` which loops over every column and uses `nunique()` to classify it into one of four buckets: `identifier` (cardinality equal to row count — e.g. customer IDs), `binary` (exactly 2 distinct values), `categorical` (few values, non-numeric), or `numerical` (continuous floats or integers). Returns a dictionary of four lists — a reusable schema map consumed by downstream notebooks.

**Q4 — TotalCharges vs MonthlyCharges × tenure**
Adds `expected_total = MonthlyCharges × tenure` and computes `diff_percentage = abs(actual − expected) / expected × 100`. Filters to rows where this exceeds 10%. These are customers whose billing history doesn't match a straight-line calculation — indicating promotions, mid-contract price changes, upgrades, or data entry errors. The output flags them for investigation rather than silently including them in revenue totals.

---

### `module2_customer_analytics.ipynb` — Customer Segmentation (Q6–Q10)

**Q6 — Churn by demographic dimension**
Loops over `['gender', 'SeniorCitizen', 'Partner', 'Dependents']`. For each dimension, uses `groupby(dim)['Churn'].apply(lambda x: (x == 'Yes').sum() / len(x) * 100)` to produce a churn rate per group. Key finding: gender is essentially neutral; senior citizens and customers without partners or dependents churn at noticeably higher rates.

**Q7 — Tenure segmentation with pd.cut()**
`pd.cut(df['tenure'], bins=[0,12,24,48,72], labels=['New','Growing','Mature','Loyal'])` assigns each customer to a 12-month lifecycle bucket. `groupby('tenure_segment')['Churn'].apply(...)` computes churn rate per stage. New customers (0–12 months) churn at the highest rate; Loyal customers (48–72 months) barely leave. Validates that the first year is the most critical retention window.

**Q8 — Top 10 high-churn profiles**
Groups on three columns simultaneously: `Contract × PaymentMethod × InternetService`. Computes churn rate per combination, sorts descending, returns the top 10. The top rows consistently reveal the same persona: Month-to-month + Electronic check + Fiber optic.

**Q9 — Customer Lifetime Value (CLV)**
`df['CLV'] = df['MonthlyCharges'] × df['tenure']` — total revenue a customer has contributed over their relationship. `groupby('Churn').agg(['mean','median','min','max'])` compares CLV for churned vs retained. Churned customers have significantly lower CLV because they leave early.

**Q10 — Risk score function**
`calculate_risk_score(row)` implements a transparent point system: Month-to-month contract +3, One-year contract +1, tenure under 12 months +2, no TechSupport +1, no OnlineSecurity +1. Maximum score: 7. Applied via `df.apply(calculate_risk_score, axis=1)`. Higher scores identify the customers the retention team should contact first.

---

### `module3_revenue.ipynb` — Revenue Analysis (Q11–Q15)

**Q11 — Annualised revenue loss**
`churned_df['MonthlyCharges'].sum() × 12` — if every churned customer had stayed one more year, this is the additional revenue retained. Provides the dollar figure that justifies investment in a retention programme.

**Q12 — Pareto analysis**
Sorts customers by `TotalCharges` descending. Computes `cumsum() / total_revenue` as a running cumulative percentage. Finds the customer index where cumulative revenue first crosses 80%, then checks what share of the total base that represents. If below 20%, the Pareto principle holds — justifying tiered service and VIP retention strategy.

**Q13 — ARPU by segment**
`groupby('Contract')['MonthlyCharges'].mean()` — Average Revenue Per User by contract type. Two-year contract customers pay less per month (discount for commitment) but stay far longer, making them more valuable in total. Challenges the naive assumption that highest monthly payers are the most important customers.

**Q14 — Revenue leakage**
Filters: `MonthlyCharges > mean` AND `tenure < 25th percentile` AND `Churn == Yes`. These are customers who paid a premium, barely stayed, and left — the most financially painful churn events. The output lists them for root-cause investigation.

**Q15 — Statistical outlier payers**
`threshold = mean + 1.5 × std`. Customers above this line are paying significantly more than typical. Outlier payers may be at elevated churn risk — they may feel they're overpaying. Proactive outreach can prevent high-value churn before it happens.

---

### `module4_service_intelligence.ipynb` — Service Adoption & Churn (Q16–Q20)

**Q16 — Lowest churn service combination**
`groupby(['InternetService', 'TechSupport'])['Churn'].apply(...)` — finds which two-way combination produces the lowest churn rate. DSL + TechSupport=Yes is consistently the safest configuration. Fiber optic + TechSupport=No has among the highest churn.

**Q17 — Services associated with churn reduction**
Loops over six add-on columns. For each, computes churn rate for customers with vs without the service and records the difference. Ranks services by magnitude of churn reduction. Security and support services consistently outrank entertainment add-ons.

**Q18 — Service Adoption Score**
`df['adoption_score'] = sum of binary indicators` across six add-on columns. Score 0–6. `groupby('adoption_score')['Churn'].mean()` reveals a clear negative correlation — more services, lower churn. The stickiness effect: more embedded customers have higher switching costs.

**Q19 — High-risk cohort analysis**
Filters: `InternetService == 'Fiber optic'` AND `TechSupport == 'No'` AND `OnlineSecurity == 'No'`. This cohort has the highest churn rate in the dataset — premium service, zero safety net. The output quantifies the size and danger of this group.

**Q20 — Most profitable bundle**
Creates a `bundle` label per customer by joining active add-on service names. `groupby('bundle')['TotalCharges'].mean()` identifies which specific combination generates the highest average lifetime revenue — accounting for both charge level and retention duration.

---

### `module5_advanced_challenges.ipynb` — Advanced Feature Engineering (Q21–Q24)

**Q21 — ML-ready feature matrix**
Converts the raw dataset into a fully numerical table. Binary text columns mapped to 0/1. Multi-value categorical columns one-hot encoded with `pd.get_dummies()`. Numeric columns kept as-is. Result: a clean all-numeric matrix any classification algorithm can consume directly.

**Q23 — Retention KPI dashboard dataset**
Builds a summary dictionary then converts to a structured DataFrame: `total_customers`, `churned_customers`, `retained_customers`, `churn_rate_pct`, `avg_tenure_months`, `avg_clv` — broken down by contract type. The exact data structure that would power a live BI dashboard.

**Q24 — Rule-based churn risk engine**
`churn_risk_level(row)` scores each customer 0–9 using four weighted rules: contract type (0/1/3), tenure (0/2), add-on adoption (0–3), payment method (0/1). Maps score to Low (0–2) / Medium (3–4) / High (5–6) / Critical (7+). Applied via `df.apply(...)`. The retention team works from Critical downward.

---

## For the Room — Plain-Language Walkthrough

A phone company has 7,043 customers in a spreadsheet. They know some of them have already cancelled — and they suspect more are about to. The question they want answered is not just "why are people leaving?" but "can we tell which specific customers are most likely to leave next, and what should we do about it?"

This phase answers that question in five steps, building from data cleaning all the way to a working risk score for every customer.

---

### Module 1 — Making Sure the Data Can Be Trusted (Q1–Q4)

**Q1 — The Health Check**
Before you trust any number that comes out of an analysis, you need to know whether the data going in is good. Q1 runs a systematic health check across every column in the spreadsheet: how many blank cells are there? How many unique values? What does a typical value look like? The output is a clean summary — one row per column — that lets any analyst see the state of the data at a glance before touching anything else. It takes seconds to run and prevents a category of mistakes that would otherwise take days to find.

**Q2 — The Hidden Blank Space Problem**
The column recording how much each customer has paid in total should be a number. But it was saved as text, and eleven rows contain just an empty space where a number should be. If you try to calculate an average from this, you'll either get an error or — worse — a quietly wrong answer. Q2 fixes this: it converts the column to a proper number, and those eleven spaces become "not available" markers. The code also explains why they exist — those are brand-new customers who haven't been billed yet, not a data error. A small fix that has a big downstream impact.

**Q3 — Sorting the Columns into Buckets**
A dataset with 21 columns is hard to work with if you treat every column identically. Some are simple Yes/No flags. Some are categories with a handful of values. Some are numbers you can add up. Some are ID codes that shouldn't be analysed at all. Q3 writes a function that automatically classifies every column into the right bucket. Once that map exists, later analyses can automatically pick the right columns for the right operations — instead of a human having to remember which is which every time.

**Q4 — Catching Billing That Doesn't Add Up**
If a customer pays £50 a month and has been around for 24 months, their total should be around £1,200. Q4 checks this arithmetic for every customer and flags anyone where the actual total differs from the calculated total by more than 10%. Some of those differences are innocent — mid-contract promotions, for example. But some might be billing errors. Either way, the company now has a list of anomalies to investigate, rather than assumptions buried in the data that would make revenue calculations unreliable.

---

### Module 2 — Understanding Who Leaves and Why (Q6–Q10)

**Q6 — Do Demographics Predict Who Churns?**
The first question any business asks about churn is whether certain types of people are more likely to leave. Q6 tests four demographic dimensions simultaneously — gender, age (senior citizen or not), whether the customer has a partner, whether they have dependents at home — and computes the churn rate for every group within each dimension. The findings are often surprising: gender turns out to be almost completely irrelevant. But being a senior citizen and being without a partner both show measurable effects on churn. Knowing which demographics matter helps focus retention conversations.

**Q7 — The Critical First Year**
Not all customers are at the same point in their relationship with the company. Someone who has been a customer for five years is very different from someone who joined last month. Q7 creates four life-stage groups based on how long each customer has been around — New (under a year), Growing (1–2 years), Mature (2–4 years), and Loyal (4–6 years) — and calculates the churn rate for each stage. The pattern is striking: new customers leave at a dramatically higher rate than any other group. The first twelve months are where the company has the most to gain from investing in retention.

**Q8 — The Exact Profile of a Customer Who Is About to Leave**
Q8 groups customers by three attributes at once — what contract they're on, how they pay, and what internet service they use — and ranks every combination by churn rate. The top of the list reveals the same profile almost every time: month-to-month contract, paying manually by electronic cheque, on fibre optic internet. Month-to-month means no commitment. Manual payment means no direct debit friction when cancelling. Fibre optic means high expectations that, when unmet, lead to quick decisions. Now the retention team has a name for this customer profile — and a reason to reach out before something goes wrong.

**Q9 — How Much Has Each Customer Actually Spent?**
There's a meaningful difference between a customer who has been paying £80 a month for three years and one who has been paying £80 a month for three months. Q9 calculates lifetime value for every customer — total charges since they joined — and compares this between customers who churned and those who stayed. The finding is consistent: churned customers have much lower lifetime values, not because they paid less per month, but because they left before the relationship had time to compound. This reframes the cost of early churn: it's not just losing next month's payment, it's losing the next three years.

**Q10 — Building a Simple Risk Scorecard**
The previous four questions gave us insight. Q10 turns that insight into something actionable: a score for every customer. Points are awarded for things that correlate with leaving — being on a flexible contract, being new, not having security or support add-ons. The higher the score, the more likely the customer is to cancel. Applied to all 7,000 customers, this produces a ranked call list. The retention team starts at the top. Every score can be explained in plain English, which matters when a customer asks why they were contacted.

---

### Module 3 — Putting a Number on the Problem (Q11–Q15)

**Q11 — What Is Churn Actually Costing?**
"We have a churn problem" is a statement. "We are losing £2.4 million per year" is a statement that gets a budget approved. Q11 converts the abstract problem into a concrete annual figure: take all the customers who have already left, add up their monthly charges, multiply by twelve. That's how much revenue would have been retained if those customers had stayed just one more year. This number exists to make the investment case for a retention programme — if it costs £100,000 to run one and the revenue at risk is twenty times that, the decision makes itself.

**Q12 — Are a Few Customers Carrying the Revenue?**
The 80/20 rule says that in many situations, 20% of inputs drive 80% of outputs. Q12 tests whether this holds for this telecom company by ranking customers from highest to lowest total spend and watching the cumulative revenue percentage climb. The question it answers: how many customers do you need before you have accounted for 80% of total revenue? If the answer is less than 20% of the customer base, the implication is immediate — those customers deserve a completely different tier of service and attention. Losing even one of them should trigger a priority escalation, not a form letter.

**Q13 — Who Actually Makes the Company the Most Money?**
Average Revenue Per User sounds dry, but Q13 uses it to tell a story that challenges intuition. Customers on two-year contracts often pay less per month than month-to-month customers — they accepted a small discount in exchange for committing. But when you account for how long they stay, they are dramatically more valuable in total. A customer paying £60 a month for three years contributes £2,160. One paying £80 a month but leaving after four months contributes £320. This reframes how the business should think about discount offers: a slightly cheaper annual plan is not giving money away, it's buying loyalty, and the return on that investment is strongly positive.

**Q14 — The Most Painful Churns**
Not all customers who leave hurt equally. Q14 finds the most painful ones: customers who were paying above-average amounts, had been around for a short time, and still chose to leave. These are the sharpest disappointments — high-value customers who came in, didn't find what they expected, and left before the relationship had a chance to develop. The output is a list of specific cases the company can investigate. What did these customers have in common? Was there a product gap? A bad onboarding experience? Those answers are worth finding.

**Q15 — Customers Paying Unusually High Amounts**
Q15 finds customers whose monthly charges sit well above the statistical norm. The instinct is to celebrate them. But high payers can also be at elevated churn risk — they may be on a complex or over-specified plan that nobody helped them optimise. A proactive call — "we noticed you're on our premium configuration, can we make sure you're getting everything out of it?" — is one of the cheapest and most effective retention moves available. Q15 produces the list of who those people are before they start looking for alternatives.

---

### Module 4 — The Services That Keep People Around (Q16–Q20)

**Q16 — The Safest Combination of Services**
Q16 asks a targeted question: if you look at internet service type and whether the customer has tech support, which combination has the lowest churn? DSL internet with tech support has among the lowest churn in the dataset. Fibre optic without tech support has among the highest. Fibre optic is the premium, highest-expectation product — when something goes wrong, and it does, customers without support have nobody to call. For the sales team, this means that selling fibre optic and tech support together is not upselling, it's responsible product design.

**Q17 — Which Add-On Service Saves the Most Customers?**
The company offers six optional extras. Q17 runs what is effectively a natural experiment: for each service, compare the churn rate of customers who have it versus those who don't. The gap tells you how protective that service is against cancellation. The ranking is illuminating: security and support services dramatically reduce churn. Entertainment services — TV and movies — also reduce churn, but far less so. People stay because they feel looked after, not primarily because they have access to content. That's a meaningful signal for where to focus onboarding conversations.

**Q18 — The More Services, the More They Stay**
Q17 looked at services one at a time. Q18 asks a broader question: does it matter how many services a customer has, regardless of which ones? Each customer gets a score from 0 to 6 based on how many add-ons they subscribe to. The relationship between this score and churn rate is one of the clearest findings in the phase: as adoption score increases, churn falls steadily and substantially. A customer with no add-ons is many times more likely to cancel than one with five. Getting new customers to adopt even one or two add-ons in their first month is one of the highest-leverage retention moves available.

**Q19 — The Highest-Risk Group in the Dataset**
Q19 defines the precise combination most likely to cancel: fibre optic internet — the fastest and most demanding service — with no tech support and no online security. This cohort churns at a rate far above the overall average. They signed up for a premium, high-expectation product, opted out of every layer of protection and support, and when something goes wrong — and with demanding technology it will — they have no reason to stay. For the retention team, this is not a segment to monitor over the next quarter. It is a segment to call this week.

**Q20 — Which Bundle of Services Makes the Most Money Over Time?**
Q20 approaches the problem from a revenue angle — not "who stays?" but "who generates the most lifetime revenue?" It creates a label for each customer describing their exact combination of active add-ons, then asks which bundle produces the highest average lifetime charges. The metric accounts for both what customers pay and how long they stay — because a slightly cheaper bundle that people keep for four years easily outperforms an expensive one with high churn. The output gives sales and product teams a concrete recommendation: guide new customers toward this specific bundle, because the data shows it generates the best long-term return.

---

### Module 5 — Building Tools That Go Beyond This Dataset (Q21–Q24)

**Q21 — Preparing the Data for a Machine Learning Model**
Everything in the previous modules was human-driven: we looked at the data, asked questions, and interpreted the answers. Q21 prepares for a different kind of analysis — one where a computer finds patterns too subtle for a human to see. A machine learning model cannot read text. It needs every piece of information expressed as a number. Q21 runs a systematic translation: Yes/No columns become 1 and 0, text categories like "Month-to-month" become a set of binary flags, numbers stay as numbers. The result is a clean numerical table — every row a customer, every column a feature — that any classification algorithm can immediately consume. This is the foundation Phase 3 builds on.

**Q23 — The Executive Dashboard in One DataFrame**
Imagine a screen that shows, at a glance, the key health numbers for the customer base: total customers, how many have churned, the overall churn rate, average tenure, average lifetime value — broken down by contract type. Q23 builds exactly that dataset. It assembles the most important summary statistics into a structured output that can be connected directly to a BI tool and refreshed on a schedule. It is not glamorous code. It is the code that transforms all the analysis done in this phase into something a leadership team can look at every morning without opening a single notebook.

**Q24 — A Risk Engine Anyone Can Read and Challenge**
The final question builds the most practically useful output of the phase: a risk level for every customer — Low, Medium, High, or Critical — based on a transparent, auditable set of rules. Points are awarded for being on a flexible contract, for being new, for having few add-ons, for paying manually. The logic is completely visible: any manager can read it, question it, and suggest changes. Applied to all 7,000 customers, it produces a prioritised call list. The retention team starts at Critical and works downward. The real value is the interpretability — every score can be explained in a customer conversation. That is the difference between analysis that sits in a notebook and analysis that changes behaviour in the real world.

---

*All of this was built in Jupyter notebooks — documents that run code and show results in the same place, making analysis transparent and reproducible.*
