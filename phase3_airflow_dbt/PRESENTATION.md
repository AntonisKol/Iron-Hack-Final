# Phase 3 — Airflow + dbt: Orchestrated ETL/ELT Pipelines
**Presentation Guide**

---

## What Was Asked (Technical Brief)

Build production-grade data pipelines using Apache Airflow 3.2 as the orchestrator and dbt as the transformation layer. Tasks covered: writing multi-step DAGs, dynamic task generation, conditional branching, failure email alerts, Snowflake ETL, REST API ingestion with XCom, Airflow-triggered dbt runs, a fraud detection pipeline with SQL rule-based alerting, and a capstone enterprise ELT pipeline combining all components.

All credentials must be stored in a `.env` file and loaded at runtime — nothing hardcoded. Task failure must trigger an automated email notification.

---

## Technical Breakdown — File by File

---

### `dag_utils.py` — Shared Utilities (Not a DAG)

Single source of truth for the `send_failure_email` function. Airflow's DAG loader skips this file because it contains no DAG objects. `load_dotenv()` runs at import time, making `EMAIL_ADDRESS` and `GMX_PASSWORD` available. `send_failure_email(context)` receives Airflow's context dict, extracts `task_id` and `dag_id`, connects to GMX SMTP on port 587, authenticates, and sends a notification email. Every DAG imports this and passes it as `on_failure_callback=send_failure_email` on each task.

---

### `q1_first_pipeline.py` — Basic 3-Task DAG

Introduces core Airflow concepts: DAG definition, task creation, and dependency syntax. `default_args` is defined once (owner, retries, retry_delay) and applied to every task automatically via inheritance. The DAG is declared with `with DAG(...)`, a cron schedule of `0 8 * * *` (daily at 08:00), a fixed start date, and `catchup=False` to prevent backfilling missed runs. Three `PythonOperator` tasks — `pipeline_started`, `wait_10_seconds`, `pipeline_completed` — are chained with `task_start >> task_wait >> task_end`. The `>>` operator is Airflow's syntax for "this must complete before the next one starts," and is how Airflow builds the execution graph.

---

### `q2_dynamic_mapping.py` — Dynamic Task Mapping

Instead of writing five separate tasks, one per country, this DAG uses `PythonOperator.partial(...).expand(...)`. `partial()` defines the task template — the callable and task ID that stay the same. `expand(op_args=[...])` provides the list of inputs — one item per country. Airflow spawns five task instances in parallel at runtime, one per country, without any additional code. This is Airflow's "dynamic task mapping" feature: one definition, many runtime instances, zero boilerplate.

---

### `q3_branching.py` — Conditional Execution

Uses `BranchPythonOperator` to route execution to one of two paths based on a runtime condition. `choose_branch()` calls `datetime.now().weekday()` — this returns 0 on Monday. If Monday, it returns the string `'full_load'`, which Airflow matches to the task with that ID and runs it, automatically skipping the other. On any other day, `'incremental_load'` runs instead. The final `task_end` task carries `trigger_rule='none_failed_min_one_success'` — without this, Airflow waits for both upstream tasks and gets stuck when one is intentionally skipped. The branch fans out to both tasks; only one executes; both reconnect to the final task.

---

### `q4_email_notification.py` — Failure Email

Demonstrates Airflow's `on_failure_callback` mechanism. When `task_failure` raises an exception, Airflow calls `send_failure_email` automatically, passing the full context dict. That function connects to GMX SMTP, reads the recipient address from `EMAIL_ADDRESS` in the environment, and sends an email naming the failed DAG, task, and execution time. Credentials are never hardcoded — they live in `.env` and are accessed with `os.getenv()`.

---

### `q5_snowflake_etl.py` — Snowflake ETL Pipeline

A three-task ETL DAG where all six Snowflake connection parameters are assembled into `SNOWFLAKE_CONFIG` at module load time, loaded from `.env`, and unpacked into `snowflake.connector.connect()` with `**`. Task 1 confirms source data exists with `SELECT COUNT(*)`. Task 2 runs `CREATE OR REPLACE TABLE FRAUD_SUMMARY AS SELECT ...` — groups transactions by country and merchant category, computing fraud rate and average amount entirely inside Snowflake (no data leaves the warehouse). Task 3 reads back the first five rows as a sanity check. The three tasks run sequentially via `>>`.

---

### `q6_api_ingestion.py` — REST API → Snowflake with XCom

A three-task DAG that pulls from an external HTTP API and stores results in Snowflake. `call_api()` hits a public REST endpoint with `requests.get()`, converts the response with `.json()`, serialises it to a string with `json.dumps()`, and pushes it to Airflow's XCom store — Airflow's built-in shared notepad where tasks write key-value pairs that other tasks in the same run can read. `parse_response()` pulls the raw string with `xcom_pull`, deserialises with `json.loads()`, extracts nested fields (city lives inside `address`, company inside `company`), and pushes the cleaned version back to XCom. `save_to_snowflake()` pulls the parsed data and inserts each row using `%s` parameterised placeholders — preventing SQL injection. `CREATE TABLE IF NOT EXISTS` makes the setup step idempotent.

---

### `q8_airflow_dbt.py` — Airflow Triggering dbt

Airflow orchestrates dbt via `BashOperator`, which executes shell commands. `task_dbt_run` runs `dbt run --project-dir .` from the `churn_mart/` directory — rebuilds all dbt models in dependency order. `task_dbt_test` runs `dbt test` — validates `unique` and `not_null` constraints defined in `schema.yml`. Both carry `on_failure_callback=send_failure_email`. `task_capture_logs` reads the last 50 lines of dbt's log file and prints them to the Airflow task log — making it possible to review exactly what dbt did without leaving the Airflow UI. Tasks run sequentially: tests only execute after the run succeeds, logs are captured only after tests pass.

---

### `q9_fraud_detection.py` — Banking Fraud Detection Pipeline

A four-task fraud alerting DAG. `load_check` verifies the source table has data before running expensive queries. `apply_fraud_rules` creates `FRAUD_ALERTS` using a `CASE WHEN` block that classifies each suspicious transaction into one of four types: High Amount Anomaly (amount > avg + 3σ), Multiple Failed Attempts (failed_attempts > 3), International Night Fraud (international + night + repeated failures), High Risk International (international + low credit score + above-average amount). The `WHERE` clause mirrors the same four conditions to filter only flagged rows. `generate_summary` aggregates into `FRAUD_DAILY_SUMMARY` by alert type, computing confirmation rates and total amount at risk. `fraud_report` prints the summary in a formatted column table to Airflow logs. Every task carries `on_failure_callback`.

---

### `q10_enterprise_elt.py` — Enterprise-Grade ELT Pipeline

The capstone DAG combining all Phase 3 components. `ingest_check` queries both source databases (fraud and churn) and raises an exception if either is empty — failing fast before running any expensive transformations. `dbt_run` rebuilds all churn models via BashOperator. `dbt_test` validates data quality. After tests pass, `fraud_business_report` and `churn_business_report` run in parallel — each queries its respective dataset, prints a formatted report, and pushes key metrics to XCom (total fraud count, average fraud amount, total churn revenue loss). `send_business_report` pulls those metrics from XCom and sends a combined daily BI email via GMX SMTP. The diamond pattern `dbt_test >> [fraud_report, churn_report] >> send_report` expresses parallelism and a merge point in a single line.

---

### dbt Project — `churn_mart/`

#### `models/staging/stg_churn.sql`
The entry point of the dbt pipeline. `{{ source('churn_raw', 'RAW_CHURN') }}` tells dbt where to find raw data and enables lineage tracking. All columns are renamed to snake_case. Yes/No string flags are converted to 1/0 integers via `CASE WHEN`. `TotalCharges` is cast from text to number using `try_to_number()` — the raw data stores it as a string because blank-space rows would break a strict numeric cast. No business logic lives here — purely cleaning and standardisation.

#### `models/intermediate/int_customer_features.sql`
Builds on staging via `{{ ref('stg_churn') }}`. The `ref()` call tells dbt to use the output of `stg_churn` as input and to run it first — this is how dbt builds its dependency graph automatically. Three calculated columns are added: `clv` (monthly_charges × tenure), `tenure_segment` (New / Developing / Established / Loyal), and `risk_score` (0–6 point sum: month-to-month contract adds 2, fiber optic adds 1, no security adds 1, no tech support adds 1, electronic check adds 1, tenure under 12 months adds 1).

#### `models/marts/mart_churn_kpis.sql`
The final reporting model, built on `fct_churn` via `{{ ref('fct_churn') }}`. Groups by contract type and computes: total customers, churned customers, churn rate as a percentage, average monthly charges, average CLV, annual revenue lost (churned customers × monthly_charges × 12), average risk score, and average tenure. Sorted by churn rate descending. This is the table that feeds dashboards and the Q10 enterprise email.

---

## For the Room — Plain-Language Walkthrough

---

### Q1 — Building Your First Automated Pipeline

Think of Airflow like a very organised personal assistant with a daily planner. You write the instructions once: "Every morning at 8, first confirm the data arrived, then pause for a moment, then write a note that the job is done." From that point forward, Airflow runs those three steps in exactly that order every single day without anyone pressing a button. If something breaks, it retries automatically. If it breaks twice, it gives up and sends an email. This is what data engineers mean by a "pipeline" — a set of steps that run in a fixed order on a schedule, reliably, without human intervention.

### Q2 — Running the Same Task Five Times at Once

Imagine you have a report that needs to run for five countries — USA, India, UK, Germany, France. The bad way to do this is to write five separate tasks, one per country, in your code. If the country list ever changes, you'd have to rewrite the file. The smart way — what we built here — is to write one task definition and hand it a list. Airflow stamps out five copies automatically and runs them all in parallel, simultaneously. You write the logic once. Airflow handles the multiplication. Adding a sixth country is one edit to a list, not a new task.

### Q3 — Taking a Different Road Depending on the Day

This pipeline works like a traffic roundabout with two exits. Every morning it checks what day of the week it is. On Mondays it takes the "full reload" road — rebuilding everything from scratch, like doing a full clean of the house. On every other day it takes the "incremental" road — only updating what changed since yesterday, like a quick tidy-up. Both roads merge back into a single final step when they're done. Airflow handles the routing automatically, and critically, it knows not to wait forever for a road that was intentionally skipped.

### Q4 — Getting an Email When Something Goes Wrong

The simplest idea in operations: when something fails, tell someone immediately. We wired every task in Airflow so that if it crashes — database is down, query fails, network times out — it immediately calls a function that sends an email naming the pipeline, the task, and when it happened. You don't need to monitor a dashboard all day. The system watches itself and tells you when it needs attention. Think of it as a smoke alarm for your data infrastructure: quiet when everything is fine, loud the moment it isn't.

### Q5 — Moving and Reshaping Data Inside Snowflake

This pipeline connects to our cloud data warehouse and does three things in order. First, it checks that there's actually data to work with — no point running an expensive transformation on an empty table. Then it runs a transformation: takes hundreds of thousands of raw transactions and summarises them into a clean table showing fraud rates by country and merchant type. Then it checks the result has the expected number of rows. All the database passwords live in a hidden configuration file, never in the code itself, so there's nothing sensitive exposed in the codebase.

### Q6 — Fetching Data from the Internet and Saving It

This pipeline goes out to a public website (a REST API), asks for a list of user records, receives a structured response — names, emails, cities, company names — cleans it, and saves it to Snowflake. The interesting engineering detail is how three separate tasks communicate with each other. Airflow has a built-in shared notepad called XCom. Task one writes the raw data to it. Task two reads that, processes it, and writes the cleaned version back. Task three picks that up and saves to the database. It is like a relay race where each runner improves what they are carrying before passing it on.

### Q7 — How Airflow Thinks (Theory)

This question covered the concepts behind the tool: how Airflow decides what to run, what states a task goes through (queued, running, success, failed, skipped), how scheduling works, and why preventing historical backfills matters when dealing with live production data. No code — just understanding the machinery before using it.

### Q8 — Letting Airflow Drive dbt

dbt is a tool that runs SQL transformations in the right order. Airflow is a scheduler that runs things on a timer. Here we connected them: every morning, Airflow tells dbt to rebuild all its models (refresh a set of structured reports from scratch), then run quality tests (checking that key columns have no nulls, no duplicates), then capture the log of what happened so you can see it in the Airflow dashboard without opening a terminal. If any step fails, an email goes out. It is like a production line: build, inspect, log — all automated, all auditable.

### Q9 — A Daily Fraud Watchlist

Every morning this pipeline checks a bank's transaction table and flags anything suspicious using four rules: an unusually large amount (statistically more than three standard deviations above normal), too many failed login attempts before the transaction, international transactions late at night with repeated failed attempts, or international transactions from customers with a low credit score above the average amount. Each flagged transaction gets labelled with the rule it triggered. Then it summarises: how many were flagged per rule, how many turned out to be confirmed fraud, how much money was at risk. A formatted daily report prints to the logs. Think of it as the briefing a fraud analyst would otherwise have to produce manually — now it runs itself every morning before anyone sits down at their desk.

### Q10 — The Full Picture: One Pipeline That Does Everything

This is the capstone — a single Airflow pipeline that touches everything built in this phase. It starts by verifying both databases have data. Then it rebuilds all dbt reporting models and runs their quality tests. Once that passes, it runs two business reports in parallel — one on fraud statistics, one on customer churn revenue risk — and passes the key numbers forward via Airflow's shared notepad. Finally it assembles a combined daily business intelligence email and sends it. A CFO could read that email over morning coffee and know the state of fraud exposure and customer retention risk without opening a single dashboard. That is the point: data engineering that disappears into the background and just works.

---

### dbt Models — Translating Raw Data into Business Intelligence

**Staging (`stg_churn.sql`) — Unpacking the Box**

Imagine the raw data arriving in a cardboard box — inconsistent naming, Yes/No strings instead of 0/1 flags, a numeric column stored as text. The staging model is the person who unpacks the box and puts everything neatly on labelled shelves. No calculations yet, just cleaning. Every column gets a proper name, every format gets standardised. Nothing is calculated here — this layer exists purely so that every downstream model is working with clean, trustworthy inputs from the start.

**Intermediate (`int_customer_features.sql`) — Adding Meaning**

Now that things are clean, we add insight. How much has each customer spent in total? How long have they been around? How risky are they likely to be? These are calculated scores — not raw facts, but derived signal. A customer on a month-to-month contract with fibre optic internet and no security add-on scores highest on the risk scale. This model is where raw data becomes business intelligence. The `ref()` function tells dbt to always run the staging model first, automatically building the correct order without manual coordination.

**Mart (`mart_churn_kpis.sql`) — The Dashboard Table**

The final output — one row per contract type, showing churn rate, average revenue, and how much money walks out the door each year per group. Month-to-month customers churn the most; two-year contract customers almost never leave. Those two numbers alone can change a pricing strategy. This is the table a BI tool like Tableau connects to — built automatically every morning by Airflow, always up to date, always correct.
