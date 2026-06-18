# Phase 3 — Airflow & dbt Pipeline Orchestration
**Presentation Guide**

---

## What Was Asked (Technical Brief)

Build automated data pipelines using two industry-standard tools: **Apache Airflow** (for scheduling and orchestration) and **dbt** (for SQL-based data transformations inside Snowflake). The phase consists of 10 Airflow DAGs (one per question) and one dbt project. Each DAG must demonstrate a specific orchestration pattern. The dbt project must build a star schema from the telecom churn dataset, producing a customer data mart.

All credentials must be stored in a `.env` file and loaded at runtime — nothing hardcoded. Task failure must trigger an automated email notification.

---

## Technical Breakdown — File by File

---

### `dag_utils.py` — Shared Utilities (Not a DAG)

**Purpose:** Single source of truth for the `send_failure_email` function. Airflow's DAG loader skips this file because it contains no DAG objects.

- **Imports block:** `smtplib` (Python's built-in SMTP client), `MIMEText` (email body formatter), `dotenv`, `os`.
- **`load_dotenv()` call:** Loads the `.env` file from the project root — makes `EMAIL_ADDRESS` and `GMX_PASSWORD` available as environment variables.
- **`send_failure_email(context)` function:** Receives an Airflow context dict (contains the failed task's metadata). Builds an email body from `context['task_instance'].task_id` and `dag_id`. Connects to GMX's SMTP server (`mail.gmx.net:587`), authenticates, sends the email. Every DAG imports this function via `from dag_utils import send_failure_email` and passes it as `on_failure_callback=send_failure_email` on each task.

---

### `q1_first_pipeline.py` — Basic 3-Task DAG

**Purpose:** Introduce the core Airflow concepts: DAG definition, task creation, task dependency syntax.

- **`default_args` block:** Dict applied to every task in the DAG — sets owner, retry count (1), retry delay (5 minutes). Tasks inherit these unless they override individually.
- **`with DAG(...)` block:** Declares the DAG. `dag_id='first_pipeline'`, `schedule='0 8 * * *'` (daily at 08:00), `catchup=False` (don't re-run missed historical runs).
- **Three `PythonOperator` tasks:** `pipeline_started` (prints a message), `wait_10_seconds` (calls `time.sleep(10)`), `pipeline_completed` (prints done).
- **Dependency line:** `task_start >> task_wait >> task_end` — the `>>` operator is Airflow's syntax for "this must complete before the next one starts." Airflow reads this and builds the execution graph.

---

### `q2_dynamic_mapping.py` — Dynamic Task Mapping

**Purpose:** Generate N parallel tasks at runtime from a list, without writing N task definitions.

- **Country list:** `['US', 'DE', 'FR', 'GB', 'ES']` — five countries.
- **`process_country(country)` function:** Simulates loading and processing data for one country. Takes the country code as input, prints a log line.
- **`PythonOperator.partial(...).expand(op_kwargs=...)` block:** Airflow's dynamic mapping API. `partial()` fixes the task template. `expand()` provides the varying input — one dict per country. At runtime Airflow spawns five separate tasks, one per country, and runs them in parallel. This is the key concept: **one task definition, many runtime instances**.

---

### `q3_branching.py` — Conditional Execution

**Purpose:** Run different tasks depending on a condition evaluated at runtime — not at DAG definition time.

- **`BranchPythonOperator` task:** `choose_branch()` checks `datetime.now().weekday()`. If today is Monday (`== 0`) it returns `'full_load'` — the task_id of the next task to run. Otherwise returns `'incremental_load'`. Airflow uses this return value to skip all other branches.
- **Two downstream tasks:** `full_load` (prints "Running full data load") and `incremental_load` (prints "Running incremental load"). Only one runs per execution.
- **`trigger_rule='none_failed_min_one_success'` on the final task:** Allows the downstream `notify_complete` task to run even though one branch was skipped (skipped ≠ failed).

---

### `q4_email_notification.py` — Failure Email

**Purpose:** Demonstrate `on_failure_callback` — automatic email when any task fails.

- **`from dag_utils import send_failure_email`:** Imports the shared function.
- **`fail_task` PythonOperator:** Intentionally raises `ValueError('This task always fails')` to trigger the callback.
- **`on_failure_callback=send_failure_email` on every task:** Airflow calls this function automatically if the task exits with an exception, passing the context dict so the email knows which task failed and in which DAG.

---

### `q5_snowflake_etl.py` — ETL Inside Snowflake

**Purpose:** Three-task pipeline: check source data → create transformed table → verify row count.

- **`SNOWFLAKE_CONFIG` block:** Connection parameters loaded from environment variables. Passed with `**SNOWFLAKE_CONFIG` to `snowflake.connector.connect()`.
- **`extract_check` task:** Opens a Snowflake connection, runs `SELECT COUNT(*) FROM BANK_TRANSACTIONS`, prints the result. Fails the pipeline if the table is empty — data must exist before transforming it.
- **`transform_load` task:** Runs `CREATE OR REPLACE TABLE FRAUD_SUMMARY AS SELECT merchant_category, COUNT(*), ROUND(SUM(is_fraud)/COUNT(*)*100,2) AS fraud_rate FROM BANK_TRANSACTIONS GROUP BY merchant_category`. This is a Snowflake-side transformation — no data leaves the warehouse.
- **`verify` task:** Reads `FRAUD_SUMMARY` and prints the row count. Confirms the transform ran successfully.

---

### `q6_api_ingestion.py` — REST API → Snowflake

**Purpose:** Pull data from an external HTTP API, parse the JSON, and load it into Snowflake — a standard EL (extract-load) pattern.

- **`fetch_api_data` task:** Calls `requests.get('https://jsonplaceholder.typicode.com/users')`, parses the response as JSON (list of 10 user dicts). Pushes the raw list into Airflow's **XCom** storage with `task_instance.xcom_push(key='users', value=users)`. XCom is Airflow's lightweight message bus for passing small values between tasks.
- **`load_to_snowflake` task:** Pulls the data back with `task_instance.xcom_pull(task_ids='fetch_api_data', key='users')`. Creates `API_USERS` table in Snowflake with `id`, `name`, `email`, `phone`, `website` columns. Inserts all 10 rows with `executemany`.

---

### `q8_airflow_dbt.py` — Airflow Triggering dbt

**Purpose:** Show how Airflow and dbt work together — Airflow schedules the run, dbt executes the SQL transformations.

- **`DBT_PROJECT_DIR` constant:** Absolute path to the `churn_mart/` dbt project.
- **`dbt_deps` BashOperator:** Runs `dbt deps` in the project directory — installs any dbt packages declared in `packages.yml`. Must run before the model build.
- **`dbt_run` BashOperator:** Runs `dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}` — builds all dbt models in the correct dependency order.
- **`dbt_test` BashOperator:** Runs `dbt test` — executes all data quality tests defined in `schema.yml` (not-null, unique constraints). Fails the DAG if any test fails.
- **`on_failure_callback=send_failure_email`** on every task.

---

### `q9_fraud_detection.py` — Automated Fraud Detection Pipeline

**Purpose:** Full production-style pipeline. Four tasks running fraud detection rules on 1M bank transactions daily.

- **`load_check` task:** Connects to Snowflake, runs `SELECT COUNT(*) FROM BANK_TRANSACTIONS`. Verifies data is accessible before running expensive queries.
- **`apply_fraud_rules` task:** Runs a `CREATE OR REPLACE TABLE FRAUD_ALERTS AS SELECT ...` query. The WHERE clause catches four alert patterns: High Amount Anomaly (amount > avg+3σ), Multiple Failed Attempts (failed_attempts > 3), International Night Fraud (international + night + 3+ failures), High Risk International (international + credit_score < 600 + above-avg amount). A `CASE WHEN` block assigns the matching label to each row.
- **`generate_summary` task:** Aggregates `FRAUD_ALERTS` into `FRAUD_DAILY_SUMMARY` — one row per alert type with total count, confirmed fraud count, confirmation rate %, avg amount, total at-risk amount.
- **`fraud_report` task:** Reads `FRAUD_DAILY_SUMMARY` and formats it as a table in the Airflow logs. In production, this would be emailed to the fraud team.
- **Task chain:** `load_check >> apply_fraud_rules >> generate_summary >> fraud_report`. `on_failure_callback` on every step.

---

### `q10_enterprise_elt.py` — Enterprise ELT Pipeline

**Purpose:** Simulate a full enterprise Extract-Load-Transform flow with data quality validation.

- **`extract` task:** Reads source data into a staging table — simulates pulling from an operational system.
- **`validate` task:** Runs data quality checks (null counts, value range checks) on the staging table. Fails loudly if data doesn't meet expectations.
- **`load` task:** Loads validated data into the target schema.
- **`transform` task:** Applies business logic transformations inside Snowflake using SQL (ELT pattern — transform happens after loading, inside the warehouse, not in the pipeline).
- **`dbt_run` BashOperator:** Triggers the dbt project to build downstream models on top of the freshly loaded data.

---

### dbt Project — `churn_mart/`

**Purpose:** Build a star schema inside Snowflake from the raw telecom churn CSV.

#### `models/staging/stg_churn.sql` — Staging Layer
View over the raw table. Renames columns to snake_case, casts `TotalCharges` from string to float (`TRY_CAST`), converts `Yes`/`No` columns to `1`/`0` integers. No business logic — pure cleaning.

#### `models/intermediate/int_customer_features.sql` — Intermediate Layer
View over `stg_churn`. Adds derived columns:
- `CLV = monthly_charges × tenure` — customer lifetime value
- `risk_score` — 0–7 point score based on contract type, tech support, online security, senior citizen status
- `tenure_segment` — CASE WHEN buckets: New / Growing / Mature / Loyal

#### `models/marts/dim_customers.sql` — Customer Dimension
Materialised as a **table** (not a view). Selects from `int_customer_features`: `customer_id` (PK), demographics, `tenure_segment`, `risk_score`, `CLV`. This is the "who" dimension.

#### `models/marts/dim_services.sql` — Services Dimension
Materialised as a table. Selects the add-on columns: `OnlineSecurity`, `OnlineBackup`, `DeviceProtection`, `TechSupport`, `StreamingTV`, `StreamingMovies`, `InternetService`, `Contract`. This is the "what they subscribed to" dimension.

#### `models/marts/fct_churn.sql` — Churn Fact Table
Materialised as a table. One row per customer. Joins `dim_customers` and `dim_services` on `customer_id`. Adds: `is_churned` (1/0), `monthly_charges`, `total_charges`, `payment_method`. This is the central fact table.

#### `models/marts/mart_churn_kpis.sql` — KPI Summary
Three-row summary table. Groups by `contract` type. Computes: `churn_rate` (%), `avg_clv`, `total_monthly_revenue`, `customers_at_risk_count`. The output table that a BI tool connects to for the executive dashboard.

---

## For the Room (Non-Technical)

Picture a large factory. The factory runs 24 hours a day and needs to do specific jobs at specific times — some jobs need to run every morning before the workers arrive, some need to happen in a certain order, and if something breaks, a manager needs to know immediately.

**Airflow is the factory's alarm clock and supervisor.**

You set it up once and say: *"Every day at 8 AM, run these jobs in this order. If any job fails, send me an email."* From then on, Airflow handles it — no one has to remember to run anything manually. In this phase we built 9 of these automated routines. Some are simple (a three-step sequence), some are smart (checking what day it is before deciding what to do), and one is a full fraud-detection pipeline that checks a million bank transactions every morning.

**dbt is the recipe book for the data.**

Imagine you get raw vegetables delivered every day. You don't serve raw vegetables — you chop, cook, and plate them. dbt does that for data. It takes the raw, messy customer spreadsheet from Snowflake and turns it into clean, organised tables that a dashboard can actually use. We told dbt: *"Take the raw data, clean it, calculate each customer's lifetime value and risk score, then organise it into four separate tables."* dbt figures out the correct order to build them and handles dependencies automatically.

The two tools work together: **Airflow is the alarm clock, dbt is the recipe**. Airflow wakes up at 8 AM and says *"time to cook."* dbt runs the recipes and produces clean tables. By the time the business team opens their dashboards in the morning, everything is already fresh and ready.

One more thing worth highlighting: **if anything breaks, an email arrives automatically.** We wired every step to call a shared email function. If the database goes down at 3 AM, the on-call person knows within seconds — they didn't have to check anything themselves.
