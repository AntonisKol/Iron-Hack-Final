# Phase 3 — Airflow & dbt Pipeline Orchestration

**Tools:** Apache Airflow 3.2 · dbt 1.11 · Snowflake · Python  
**Datasets:** Bank Fraud (1M rows) · Telecom Customer Churn (7,043 rows)

---

## What This Phase Does

Phase 3 builds automated data pipelines using two industry-standard tools:

- **Apache Airflow** — schedules and orchestrates tasks (the "when" and "in what order")
- **dbt** — transforms raw data into clean, structured tables inside Snowflake (the "how")

Together they form the backbone of a modern data engineering stack: Airflow triggers the pipeline, dbt does the transformation, Snowflake stores the result.

---

## Folder Structure

```
phase3_airflow_dbt/
├── dags/                        ← Airflow DAG files (Q1–Q10)
│   ├── q1_first_pipeline.py     ← Basic pipeline with 3 tasks
│   ├── q2_dynamic_mapping.py    ← Dynamic task generation per country
│   ├── q3_branching.py          ← Conditional execution (Monday vs other days)
│   ├── q4_email_notification.py ← Email alert on task failure
│   ├── q5_snowflake_etl.py      ← ETL pipeline: extract → transform → load in Snowflake
│   ├── q6_api_ingestion.py      ← REST API → parse JSON → load into Snowflake
│   ├── q8_airflow_dbt.py        ← Airflow triggers dbt run automatically
│   ├── q9_fraud_detection.py    ← Fraud detection pipeline on bank data
│   └── q10_enterprise_elt.py    ← Full enterprise ELT pipeline
└── churn_mart/                  ← dbt project (Q7)
    ├── models/
    │   ├── staging/             ← Raw data cleaned and standardized
    │   ├── intermediate/        ← Calculated features (CLV, risk score)
    │   └── marts/               ← Final tables for reporting
    └── dbt_project.yml          ← dbt configuration
```

---

## Airflow DAGs (Q1–Q6, Q8–Q10)

Each DAG is a pipeline that runs on a schedule. All DAGs run daily at 8:00 AM.

| DAG | Description | Key Concept |
|-----|-------------|-------------|
| Q1 `first_pipeline` | 3-task pipeline: start → wait 10s → complete | Basic DAG structure |
| Q2 `dynamic_mapping` | Processes 5 countries in parallel automatically | `PythonOperator.partial().expand()` |
| Q3 `branching_workflow` | Runs Full Load on Monday, Incremental Load otherwise | `BranchPythonOperator` |
| Q4 `email_notification` | Sends email to `antonisk@gmx.net` when a task fails | `on_failure_callback` + SMTP |
| Q5 `snowflake_etl` | Checks source data → creates `FRAUD_SUMMARY` table → reports row count | Snowflake connector + SQL transforms |
| Q6 `api_ingestion` | Calls public REST API → parses JSON → saves to `API_USERS` table | `requests` + XCom + Snowflake |
| Q8 `airflow_dbt` | Airflow triggers dbt models automatically | Airflow + dbt integration |
| Q9 `fraud_detection` | Runs fraud detection queries on bank transaction data | Complex SQL orchestration |
| Q10 `enterprise_elt` | Full ELT pipeline simulating a production environment | End-to-end orchestration |

### Key Airflow Concepts Used

- **XCom** — tasks communicate by pushing/pulling values through Airflow's shared storage
- **Dynamic Task Mapping** — one task definition generates N parallel tasks at runtime
- **BranchPythonOperator** — conditionally executes one branch and skips the other
- **trigger_rule** — controls when a task runs relative to upstream tasks
- **on_failure_callback** — runs a custom function (e.g. send email) when a task fails

---

## dbt Project — Customer Churn Data Mart (Q7)

The dbt project builds a **star schema** from the raw telecom churn dataset.  
Raw data lives in Snowflake (`FRAUD_DB.CHURN_RAW.RAW_CHURN` — 7,043 customers).

### Data Lineage

```
RAW_CHURN (Snowflake source)
    ↓
stg_churn           [view]   — clean column names, fix data types, encode Yes/No as 1/0
    ↓
int_customer_features [view] — add CLV, risk score (0–7), tenure segment
    ↓
┌──────────────────┬──────────────────┬──────────────────┐
dim_customers      dim_services       fct_churn          mart_churn_kpis
[table]            [table]            [table]            [table]
who the customer   what services      churn outcome +    KPI summary by
is (demographics)  they subscribe to  key metrics        contract type
```

### Models Explained

| Model | Type | Rows | Description |
|-------|------|------|-------------|
| `stg_churn` | view | 7,043 | Cleaned raw data — standardized names, fixed TotalCharges type |
| `int_customer_features` | view | 7,043 | Adds CLV (monthly × tenure), risk score, tenure segment |
| `dim_customers` | table | 7,043 | Customer dimension — demographics and account info |
| `dim_services` | table | 7,043 | Services dimension — phone, internet, add-ons |
| `fct_churn` | table | 7,043 | Fact table — churn outcome, charges, risk score |
| `mart_churn_kpis` | table | 3 | KPIs by contract type — churn rate, revenue lost, avg CLV |

### Key KPI Results (from `mart_churn_kpis`)

| Contract | Churn Rate | Annual Revenue Lost |
|----------|------------|----------------------|
| Month-to-month | ~43% | Highest |
| One year | ~11% | Medium |
| Two year | ~3% | Lowest |

### How to Run

```bash
cd phase3_airflow_dbt/churn_mart
dbt run          # build all models
dbt docs generate && dbt docs serve --port 8081   # open documentation site
```

---

## How to Run the Airflow DAGs

1. Copy DAG files to the Airflow dags folder:
```bash
cp dags/*.py ~/airflow/dags/
```

2. Start Airflow (in separate terminals):
```bash
airflow scheduler
airflow api-server --port 8080
```

3. Open `http://localhost:8080` and trigger any DAG manually.

---

## Credentials

All Snowflake credentials are stored in a `.env` file at the project root (not committed to git).  
The `.gitignore` excludes `.env` to prevent accidental credential exposure.
