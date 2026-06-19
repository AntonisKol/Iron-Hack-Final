# Phase 3 — Airflow & dbt: Pipeline Orchestration & Data Transformation
**Presentation Guide**

---

## What Is This Phase?

This phase covers two tools working together: **Apache Airflow** for scheduling and orchestrating pipelines, and **dbt** for transforming raw data inside the warehouse.

### What Airflow Does

Airflow is a scheduler and workflow manager. You define a DAG (Directed Acyclic Graph) — a set of tasks and the order they must run in. Airflow then runs that DAG on a schedule (daily, hourly, etc.), retries failed tasks, sends alerts on failure, and shows you the full history of every run in a web UI.

Without Airflow, you would run scripts manually, forget to run them, not know when they failed, and have no audit trail. Airflow replaces all of that with a reliable automated system.

### What dbt Does — and What the Alternative Would Be

dbt (data build tool) runs SQL transformations inside your data warehouse in the correct dependency order. You write `SELECT` statements. dbt figures out which model depends on which other model, runs them in the right sequence, generates documentation, and runs data quality tests.

**Without dbt**, you would write raw SQL scripts and run them manually or via a Python script. You would have to manage the order yourself. If `mart_churn_kpis.sql` depends on `fct_churn.sql`, which depends on `int_customer_features.sql`, which depends on `stg_churn.sql` — you must know and maintain that chain yourself. There is no automatic testing, no lineage diagram, no documentation, and nothing enforcing the order. For one script, that is fine. For a project with many models across staging, intermediate, and mart layers, it breaks down fast.

**What dbt gives you that raw SQL scripts do not:**
- Automatic dependency resolution via `{{ ref() }}` — you declare what you depend on; dbt runs everything in the right order
- Built-in data quality tests — `unique`, `not_null`, `accepted_values` defined in `schema.yml`, run with one command
- Auto-generated documentation and lineage diagrams
- Idempotent builds — running `dbt run` twice produces the same result

**Why they work together:** Airflow handles *when* things run. dbt handles *what* transformations to run and in what order. Q8 and Q10 wire them together: Airflow triggers `dbt run` on a daily schedule.

---

## Technical Breakdown — Q by Q

---

### Q1 — First Airflow Pipeline (`q1_first_pipeline.py`)

**Objective:** Create a DAG that runs daily at 8:00 AM with three sequential tasks.

`default_args` is defined once (owner, retries, retry_delay) and inherited by every task automatically. The DAG is declared with `with DAG(...)`, cron schedule `0 8 * * *` (daily at 08:00), a fixed start date, and `catchup=False` to prevent backfilling missed runs. Three `PythonOperator` tasks are chained with `>>` — Airflow's dependency operator — forming the execution graph: `pipeline_started` → `wait_10_seconds` → `pipeline_completed`. Each task wraps a plain Python function. The `>>` operator means "this task must finish before the next one starts." Validated via the Airflow UI Grid view, which shows each run's status per task.

---

### Q2 — Dynamic Task Mapping (`q2_dynamic_mapping.py`)

**Objective:** Generate one task per country without writing them individually.

Instead of five separate task definitions, `PythonOperator.partial(...).expand(...)` is used. `partial()` defines the shared template — the callable and task ID that stay the same across all copies. `expand(op_args=[...])` provides the input list — one item per country. At runtime, Airflow spawns five independent task instances in parallel. Adding a sixth country is one list edit. This is Airflow 3's dynamic task mapping: write once, expand to many at runtime. Parallel execution is visible in the Airflow UI Grid view — all five tasks show as separate columns running simultaneously.

---

### Q3 — Branching Workflow (`q3_branching.py`)

**Objective:** Route execution to one of two tasks based on the day of the week.

`BranchPythonOperator` wraps `choose_branch()`, which calls `datetime.now().weekday()` — returns 0 on Monday. The function returns a string matching a task ID: `'full_load'` or `'incremental_load'`. Airflow routes execution to that task and automatically skips the other. `task_end` carries `trigger_rule='none_failed_min_one_success'` — without this, Airflow waits for both upstream tasks and stalls forever when one is intentionally skipped. The dependency graph is `task_branch >> [task_full, task_incremental] >> task_end`: fan out to two paths, merge back to one.

---

### Q4 — Email Notification (`q4_email_notification.py`)

**Objective:** Send an email automatically whenever a task fails.

`send_failure_email` is imported from `dag_utils.py` — the shared utility that connects to GMX SMTP on port 587, reads the recipient from `EMAIL_ADDRESS` env var, and sends a notification naming the failed DAG, task, and execution time. `on_failure_callback=send_failure_email` is passed as an argument to any task that should alert on failure. `task_fail` deliberately raises an `Exception` to demonstrate the mechanism. All credentials live in `.env`, read via `os.getenv()` — never hardcoded in the source file.

---

### Q5 — Snowflake ETL Pipeline (`q5_snowflake_etl.py`)

**Objective:** Load and transform data in Snowflake via an Airflow DAG.

All six Snowflake connection parameters are assembled into `SNOWFLAKE_CONFIG` at module load time from `.env` and unpacked into `snowflake.connector.connect(**SNOWFLAKE_CONFIG)`. Three sequential tasks: `check_source_data` runs `SELECT COUNT(*)` to verify the source table has rows before running anything expensive. `run_transformation` executes `CREATE OR REPLACE TABLE FRAUD_SUMMARY AS SELECT ...` — grouping by country and merchant category, computing fraud rate and average transaction amount, entirely inside Snowflake (no data leaves the warehouse). `row_count_report` reads back the first five rows as a sanity check. ETL pattern: check → transform → verify.

---

### Q6 — API Data Ingestion (`q6_api_ingestion.py`)

**Objective:** Fetch data from a REST API and store it in Snowflake, passing data between tasks using XCom.

Three tasks communicate via Airflow's XCom — a built-in key-value store where tasks write data that other tasks in the same DAG run can read. `call_api` hits a public REST endpoint with `requests.get()`, converts the response with `.json()`, serialises it with `json.dumps()`, and pushes to XCom via `context['ti'].xcom_push()`. `parse_response` pulls the raw string with `xcom_pull`, deserialises with `json.loads()`, extracts nested fields (city from `address.city`, company from `company.name`), and pushes the cleaned version back. `save_to_snowflake` pulls the parsed data and inserts each row using `%s` parameterised placeholders — preventing SQL injection. `CREATE TABLE IF NOT EXISTS` makes table setup idempotent.

---

### Q7 — Customer Churn Data Mart (`churn_mart/`)

**Objective:** Build a three-layer dbt project — staging → intermediate → mart.

#### `models/staging/stg_churn.sql`
Entry point of the dbt pipeline. `{{ source('churn_raw', 'RAW_CHURN') }}` tells dbt where to find raw data and registers it for lineage tracking — dbt will show this source in the generated documentation graph. All columns are renamed to snake_case. Yes/No strings are converted to 1/0 integers via `CASE WHEN`. `TotalCharges` is cast from text to number using `try_to_number()`. No business logic here — purely cleaning and standardisation.

#### `models/intermediate/int_customer_features.sql`
Builds on staging via `{{ ref('stg_churn') }}`. The `ref()` call tells dbt to use `stg_churn` as input and automatically ensures it runs first — this is how dbt builds its dependency graph without any manual coordination. Three calculated columns are added: `clv` (monthly_charges × tenure), `tenure_segment` (New / Developing / Established / Loyal based on months), and `risk_score` (0–6 point sum from contract type, internet service, add-on absence, and tenure bracket).

#### `models/marts/mart_churn_kpis.sql`
The final reporting layer, built on `fct_churn` via `{{ ref('fct_churn') }}`. Groups by contract type and computes: total customers, churned customers, churn rate as a percentage, average monthly charges, average CLV, annual revenue lost (churned × monthly_charges × 12), average risk score, average tenure. Sorted by churn rate descending. This is the table the Q10 enterprise email reads from and where any BI tool would connect.

---

### Q8 — Airflow + dbt Integration (`q8_airflow_dbt.py`)

**Objective:** Orchestrate dbt using Airflow — run models, test quality, and capture logs.

Airflow orchestrates dbt via `BashOperator`, which executes shell commands directly. `task_dbt_run` runs `dbt run --project-dir .` from `churn_mart/` — rebuilds all dbt models in dependency order (staging before intermediate before mart). `task_dbt_test` runs `dbt test` — validates `unique` and `not_null` constraints defined in `schema.yml`. Both carry `on_failure_callback=send_failure_email`. `task_capture_logs` reads the last 50 lines of dbt's log file and prints them to the Airflow task log — reviewable in the Airflow UI without opening a terminal. Sequential: run → test → logs.

---

### Q9 — Banking Fraud Detection Project (`q9_fraud_detection.py`)

**Objective:** Build a daily pipeline that flags suspicious transactions and generates a fraud summary.

Four sequential tasks. `load_check` verifies the source table has data before running expensive queries. `apply_fraud_rules` creates `FRAUD_ALERTS` using a `CASE WHEN` block classifying each flagged transaction into one of four types: High Amount Anomaly (amount > avg + 3σ), Multiple Failed Attempts (failed_attempts > 3), International Night Fraud (international + night + repeated failures), High Risk International (international + credit_score < 600 + above-average amount). The `WHERE` clause mirrors those four conditions to filter only the suspicious rows. `generate_summary` aggregates into `FRAUD_DAILY_SUMMARY` by alert type with confirmation rates and total amount at risk. `fraud_report` prints a formatted column-aligned table to Airflow logs. Every task carries `on_failure_callback`.

---

### Q10 — Enterprise-Grade ELT Pipeline (`q10_enterprise_elt.py`)

**Objective:** Combine data verification, dbt, parallel reporting, and a BI email in a single production-grade DAG.

`ingest_check` (Tasks 1 & 2) queries both source databases (fraud and churn) and raises an exception if either is empty — fail fast before any expensive work. `task_dbt_run` (Tasks 3 & 4) rebuilds all churn models via `BashOperator`. `task_dbt_test` (Task 5) validates data quality. After tests pass, `fraud_business_report` and `churn_business_report` run in **parallel** (Task 7) — each queries its dataset, prints a formatted report, and pushes key metrics to XCom. `send_business_report` (Task 7 — email) pulls those metrics and sends a combined daily BI email via GMX SMTP. Dependency: `ingest_check >> dbt_run >> dbt_test >> [fraud_report, churn_report] >> send_report` — the `[list]` notation expresses parallel execution and a merge point in a single line.

---

## How to Run — End to End

### Prerequisites
- Apache Airflow 3.2 installed and running
- dbt installed (`/opt/homebrew/bin/dbt` or on PATH)
- Snowflake account with `FRAUD_DB` and `FRAUD_SCHEMA` accessible
- `.env` file at project root with: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`, `EMAIL_ADDRESS`, `GMX_PASSWORD`

---

### Step 1 — Start Airflow (two terminals)

```bash
# Terminal 1
airflow scheduler

# Terminal 2
airflow webserver --port 8080
```

Open `http://localhost:8080` in the browser.

---

### Step 2 — Point Airflow to the DAGs

In `airflow.cfg`, set:
```
dags_folder = /path/to/phase3_airflow_dbt/dags
```
Or symlink the `dags/` folder into Airflow's default dags directory. Airflow auto-discovers all `.py` files in that folder.

---

### Step 3 — Set up the dbt project (needed for Q7, Q8, Q10)

```bash
cd phase3_airflow_dbt/churn_mart
dbt run      # builds all models in dependency order
dbt test     # validates unique / not_null constraints
```

---

### Step 4 — Trigger DAGs in the UI

1. Toggle any DAG to **ON** using the slider
2. Click the ▶ button to trigger a manual run immediately
3. Watch task statuses in the **Grid view** (colour per task per run)
4. Click any task box → **Log** to read the output

---

### Recommended demo order

| Order | DAG | What it shows |
|---|---|---|
| 1 | `q1_first_pipeline` | Airflow is up and running |
| 2 | `q2_dynamic_mapping` | Parallel task expansion |
| 3 | `q3_branching` | Conditional routing |
| 4 | `q4_email_notification` | Failure email fires |
| 5 | `q5_snowflake_etl` | Snowflake connection confirmed |
| 6 | `q6_api_ingestion` | XCom + API + Snowflake insert |
| 7 | `q8_airflow_dbt` | dbt run/test/logs via Airflow |
| 8 | `q9_fraud_detection` | Full fraud alerting pipeline |
| 9 | `q10_enterprise_elt` | Full capstone: dbt + fraud + churn + BI email |

---

## For the Room — Plain-Language Walkthrough

---

### Q1 — Building Your First Automated Pipeline

Think of Airflow like a very organised personal assistant with a daily planner. You write the instructions once: "Every morning at 8, first confirm the data arrived, then pause for a moment, then write a note that the job is done." From that point forward, Airflow runs those three steps in exactly that order every single day without anyone pressing a button. If something breaks, it retries automatically. If it breaks twice, it gives up and sends an email. This is what data engineers mean by a "pipeline" — a set of steps that run in a fixed order on a schedule, reliably, without human intervention.

---

### Q2 — Running the Same Task Five Times at Once

Imagine you need to run the same process for five countries. The slow way is to write five separate blocks of code — one per country. If you later add a sixth country, you rewrite the file. The smart way — what we built here — is to write the logic once and hand it a list. Airflow generates five copies automatically and runs them all in parallel at the same time. Adding a sixth country is one word in a list, not a new task. This is called Dynamic Task Mapping and it is how production pipelines handle scale without duplicate code.

---

### Q3 — Taking a Different Road Depending on the Day

Every morning this pipeline checks what day of the week it is. On Mondays it takes the "full reload" road — rebuilding everything from scratch, like doing a full clean of the house. Every other day it takes the "incremental" road — only updating what changed since yesterday, like a quick tidy-up. Both roads reconnect into a single final step. This branching pattern is how real pipelines avoid unnecessary work — a full reload every day would be expensive and slow; doing it only on Mondays is the right balance.

---

### Q4 — Getting an Email When Something Goes Wrong

The simplest idea in operations: when something breaks, tell someone immediately. We wired every task so that if it crashes — database is down, query fails, network times out — it immediately sends an email naming the pipeline, the task, and when it happened. You do not need to monitor a dashboard all day. The system watches itself and tells you when it needs attention. Think of it as a smoke alarm for your data infrastructure: silent when everything is fine, loud the moment it is not.

---

### Q5 — Moving and Reshaping Data Inside Snowflake

This pipeline connects to the cloud data warehouse and does three things in order. First it checks that there is actually data to work with — no point running an expensive transformation on an empty table. Then it runs the transformation: takes raw transactions and summarises them into a clean table showing fraud rates by country and merchant type. Then it reads back a sample to confirm the result looks right. All database passwords live in a hidden configuration file — never in the code itself, so nothing sensitive is exposed.

---

### Q6 — Fetching Data from the Internet and Saving It

This pipeline goes out to a public website, asks for a list of user records, receives names, emails, cities and company names, cleans them up, and saves them to Snowflake. The interesting engineering detail is how three separate tasks communicate with each other. Airflow has a built-in shared notepad called XCom. Task one writes the raw data to it. Task two reads it, cleans it, and writes the improved version back. Task three picks that up and saves it to the database. Think of it like a relay race where each runner improves what they are carrying before passing it on.

---

### Q7 — How dbt Builds a Data Warehouse Layer by Layer

This question built the transformation models that Airflow runs automatically in Q8 and Q10. Without dbt, you would write a folder of SQL scripts and have to remember which one to run first, which depends on which, and which tests to run afterwards. dbt handles all of that. You write `SELECT` statements. dbt figures out the order, runs them, and checks the results.

The three layers work like a production line. The staging layer is the goods-in dock — everything arrives, gets unpacked and labelled consistently, nothing is calculated yet. The intermediate layer is the workshop — raw materials are combined into useful parts: customer lifetime value, risk score, tenure segment. The mart layer is the showroom — finished products ready for a report or dashboard, showing churn rate and annual revenue lost by contract type. Each layer builds on the previous one, and dbt guarantees the order is always right.

---

### Q8 — Letting Airflow Drive dbt

dbt knows what SQL to run and in what order. Airflow knows when to run it and what to do if it fails. This question connects them: every morning, Airflow tells dbt to rebuild all its models, then run quality tests to confirm no nulls or duplicates slipped through, then save the last 50 lines of dbt's log so you can see exactly what happened without opening a terminal. If any step fails, an email goes out. Build, test, log — automated, auditable, and self-monitoring.

---

### Q9 — A Daily Fraud Watchlist

Every morning this pipeline checks the transaction table and flags anything suspicious using four rules: an unusually large amount (statistically more than three standard deviations above normal), too many failed login attempts, an international transaction late at night with repeated failures, or an international transaction from a low-credit-score customer above the average amount. Each flagged transaction gets labelled with the rule it triggered. Then it summarises: how many were flagged per rule, how many were confirmed fraud, how much money was at risk. A formatted daily report prints to the logs. Think of it as the morning briefing a fraud analyst would otherwise have to produce manually — automated so it is ready before anyone sits down at their desk.

---

### Q10 — The Full Picture: One Pipeline That Does Everything

This is the capstone — a single Airflow pipeline that combines everything built in this phase. It starts by checking both databases have data. Then it rebuilds all dbt reporting models and runs their quality tests. Once that passes, it runs two business reports in parallel — one on fraud statistics, one on customer churn revenue risk — and passes the key numbers forward via Airflow's shared notepad. Finally it assembles a combined business intelligence email and sends it. A manager could read that email over morning coffee and know the fraud exposure and customer retention risk without opening a single tool. That is the goal of data engineering: infrastructure that disappears into the background and just works.

---

### dbt — The Alternative Would Have Been

Without dbt, the Q7 models would be three standalone SQL scripts run in a specific order that you would have to manage yourself. If someone ran `mart_churn_kpis.sql` before `stg_churn.sql` had been run, it would fail — and nothing would stop them. There would be no `schema.yml` to define tests, so data quality issues like duplicate customer IDs or null values would only be discovered downstream, in a dashboard or a wrong number in a report. The `ref()` function — which looks like a small detail — is the thing that makes the whole dependency chain automatic and safe.
