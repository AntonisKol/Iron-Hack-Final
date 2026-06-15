# Phase 1 — SQL & Fraud Analytics
**Tool:** Snowflake SQL | **Dataset:** Bank Fraud | **Effort:** ~6 hours

## Dataset
`data/bank_fraud.csv` — bank transaction records with fraud labels.

## Folder Structure
```
phase1_sql/
├── data/
│   └── bank_fraud.csv          ← load this into Snowflake first
├── sql/
│   ├── 00_setup.sql            ← create database, schema, table, load data
│   ├── module1_exploration.sql ← Q1–Q5:  data profiling & aggregates
│   ├── module2_customer.sql    ← Q6–Q10: customer behavior analytics
│   ├── module3_fraud.sql       ← Q11–Q15: fraud pattern analysis
│   ├── module4_investigation.sql ← Q16–Q20: advanced fraud investigation
│   ├── module5_windows.sql     ← Q21–Q24: window functions
│   └── module6_capstone.sql    ← Q25–Q28: executive report + views
└── README.md
```

## How to Run
1. Open Snowflake worksheet
2. Run `00_setup.sql` first — creates the table and loads the CSV
3. Run each module file in order (module1 → module6)

---

## Module 1 — Data Exploration & Profiling (Q1–Q5)
| Q | Task |
|---|------|
| Q1 | Total transactions, total customers, total fraud, fraud % |
| Q2 | Top 10 countries by transaction volume |
| Q3 | Top 10 cities by transaction value |
| Q4 | Data profiling report: column name, distinct values, null count |
| Q5 | % distribution by payment method, device type, merchant category |

## Module 2 — Customer Behavior Analytics (Q6–Q10)
| Q | Task |
|---|------|
| Q6  | Top 20 customers by SUM(transaction_amount) |
| Q7  | Avg transaction amount by age group (18–25, 26–35, 36–50, 51–65, 65+) |
| Q8  | Which age group has the highest fraud rate |
| Q9  | Customers with high balance, low frequency — dormant accounts |
| Q10 | Avg account balance and credit score by country |

## Module 3 — Fraud Pattern Analysis (Q11–Q15)
| Q | Task |
|---|------|
| Q11 | Fraud rate by merchant category |
| Q12 | Fraud rate by payment method |
| Q13 | Fraud rate by device type |
| Q14 | Most common fraud type (fraud_type, count, percentage) |
| Q15 | Fraud during night vs day, weekend vs weekday |

## Module 4 — Advanced Fraud Investigation (Q16–Q20)
| Q | Task |
|---|------|
| Q16 | Transactions exceeding AVG + 3×STDDEV — anomaly detection |
| Q17 | Customers with >3 failed attempts AND fraud = 1 |
| Q18 | Does recent PIN change increase fraud probability? |
| Q19 | Fraud rate: international vs domestic transactions |
| Q20 | Customers with international + night + failed_attempts > 2, ranked by risk |

## Module 5 — Window Functions (Q21–Q24)
| Q | Task |
|---|------|
| Q21 | RANK() customers by transaction value within each country |
| Q22 | ROW_NUMBER() top 5 transactions per merchant category |
| Q23 | SUM() OVER() cumulative transaction amount per customer |
| Q24 | NTILE(10) customer deciles by total transaction value |

## Module 6 — Capstone Fraud Analytics Challenge (Q25–Q28)
| Q | Task |
|---|------|
| Q25 | Executive fraud report — CTEs + CASE + Window Functions + Aggregations in one query |
| Q26 | CREATE VIEW VW_HIGH_RISK_TRANSACTIONS (international + night + failed_attempts > 2) |
| Q27 | CREATE MATERIALIZED VIEW for daily fraud reporting |
| Q28 | Detect suspicious customers — above-avg amount + international + >2 failed + credit score < 600 |
