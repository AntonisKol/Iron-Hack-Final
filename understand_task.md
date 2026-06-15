╔══════════════════════════════════════════════════════════════════════╗
║                    WHAT YOU ARE BUILDING                             ║
║           A Real-Time Data Engineering Platform                      ║
╚══════════════════════════════════════════════════════════════════════╝

RAW DATA                 TOOL YOU LEARN          WHAT IT PRODUCES
────────                 ──────────────          ────────────────

┌─────────────────┐
│  bank_fraud.csv │
│ (Phase 1 data)  │
└────────┬────────┘
         │
         ▼
╔════════════════════════════════════════════════════╗
║  PHASE 1 — Snowflake SQL                           ║
║                                                    ║
║  You learn:  SQL, Window Functions, CTEs,          ║
║              Views, Materialized Views             ║
║                                                    ║
║  You build:  • Fraud analytics queries             ║
║              • VW_HIGH_RISK_TRANSACTIONS (View)    ║
║              • Daily fraud report (Mat. View)      ║
║              • Snowflake tables + schema           ║  ← You now KNOW
╚══════════════════════╤═════════════════════════════╝    Snowflake cold.
                       │                                  Tables exist.
                       │  Snowflake knowledge +           Schema exists.
                       │  existing tables carry forward
                       │
                       ▼
┌─────────────────────────────────┐
│  Telecom Customers Churn.csv    │
│  (Phase 2 data — independent)   │
└────────────────┬────────────────┘
                 │
                 ▼
╔════════════════════════════════════════════════════╗
║  PHASE 2 — Pandas                                  ║
║                                                    ║
║  You learn:  Pandas, feature engineering,          ║
║              churn analytics, risk scoring         ║
║                                                    ║
║  You build:  • Clean churn dataset                 ║
║              • Customer risk scores                ║
║              • CLV, ARPU, revenue metrics          ║  ← You now have
║              • Churn prediction feature set        ║    a clean,
╚══════════════════════╤═════════════════════════════╝    structured
                       │                                  churn dataset.
                       │  Clean data + analytics logic
                       │  become dbt model definitions
                       │
                       ▼
╔════════════════════════════════════════════════════╗
║  PHASE 3 — Airflow + dbt                           ║
║                                                    ║
║  You learn:  Pipeline scheduling (Airflow),        ║
║              SQL transformations as code (dbt)     ║
║                                                    ║  Uses Phase 1:
║  You build:  • Airflow DAGs (daily schedules)      ║  Snowflake is the
║              • dbt staging models (from churn CSV) ║  database target
║              • dbt fact/dim tables (star schema)   ║
║              • Airflow DAG that runs dbt           ║  Uses Phase 2:
║              • Fraud pipeline DAG (bank_fraud)     ║  Churn analytics
║              • Enterprise ELT pipeline             ║  logic becomes
╚══════════════════════╤═════════════════════════════╝  dbt SQL models
                       │
                       │  You now know how to:
                       │  schedule pipelines,
                       │  move data, transform it
                       │
                       ▼
╔════════════════════════════════════════════════════╗
║  PHASE 4 — NiFi + Kafka + Spark                    ║
║                                                    ║
║  You learn:  Real-time streaming tools             ║
║              (vs. batch pipelines in Phase 3)      ║
║                                                    ║
║  You build:  • Kafka producers/consumers           ║
║              • Spark Streaming jobs                ║
║              • NiFi data flows                     ║  ← You now know
║              • Streaming → Snowflake writes        ║    real-time
╚══════════════════════╤═════════════════════════════╝    ingestion end
                       │                                  to end.
                       │  ALL skills combined:
                       │  Snowflake + Pandas logic +
                       │  Airflow scheduling +
                       │  Kafka + Spark streaming
                       │
                       ▼
╔════════════════════════════════════════════════════╗
║  PHASE 5 — End-to-End Capstone                     ║
║                                                    ║
║  You build ONE complete production platform:       ║
║                                                    ║
║  Python Simulator                                  ║
║       │  (Phase 2 skill: generate realistic data)  ║
║       ▼                                            ║
║  Kafka Topics                                      ║
║       │  (Phase 4 skill: Kafka producers)          ║
║       ▼                                            ║
║  PySpark Streaming                                 ║
║       │  (Phase 4 skill: Structured Streaming)     ║
║       ▼                                            ║
║  Snowflake RAW → CURATED → ANALYTICS              ║
║       │  (Phase 1 skill: SQL + Views)              ║
║       │  (Phase 3 skill: dbt transforms it)        ║
║       ▼                                            ║
║  Power BI Dashboard                                ║
║       (reads from Snowflake ANALYTICS layer)       ║
╚════════════════════════════════════════════════════╝
