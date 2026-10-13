# Iron Hack Final

Data engineering capstone for the Ironhack program. It walks the same two datasets through the stack a data team actually uses: SQL in Snowflake, Pandas exploration, scheduled ELT with Airflow and dbt, streaming with NiFi, Kafka, and Spark, and a real-time social media analytics platform that lands events in Snowflake for Tableau.

Two source datasets sit underneath the first four phases:

- **Bank fraud** — transaction records with a fraud label, loaded into Snowflake as `FRAUD_DB.FRAUD_SCHEMA.BANK_TRANSACTIONS`.
- **Telecom customer churn** — 7,043 customers with services, charges, and a churn flag. Pandas profiles it, and dbt turns it into a churn data mart.

Neither CSV is committed. Put `bank_fraud.csv` where Phase 1 expects it, and `telecom_churn.csv` at `phase2_pandas/data/telecom_churn.csv`.

Phase 5 does not use those files. A simulator publishes social events, Spark turns them into trending hashtags, viral posts, influencer rankings, and comment sentiment, and Tableau reads the Snowflake analytics tables.

Each phase has its own notes:

| Phase | Folder | What you get |
| --- | --- | --- |
| 1 | [`phase1_sql`](phase1_sql/README.md) | Snowflake SQL for fraud profiling, customer behavior, and an executive fraud report |
| 2 | [`phase2_pandas`](phase2_pandas/README.md) | Jupyter notebooks for churn data quality, segmentation, revenue, and a risk score |
| 3 | [`phase3_airflow_dbt`](phase3_airflow_dbt/README.md) | Airflow DAGs plus a dbt star schema (`churn_mart`) of the telecom data |
| 4 | [`phase4_nifi_kafka_spark`](phase4_nifi_kafka_spark/THEORY_QUESTIONS.md) | NiFi flows, Kafka producers, and Spark batch and streaming jobs |
| 5 | [`phase5_social_media`](phase5_social_media/README.md) | End-to-end platform: simulator, Kafka, Spark Structured Streaming, Snowflake, Tableau |

## Prerequisites

- Python 3 with `pandas`, `numpy`, `jupyter`, `python-dotenv`, `snowflake-connector-python`, `kafka-python`, `requests`, and `pyspark` (the Spark jobs pin the Kafka package `spark-sql-kafka-0-10_2.12:3.5.3`, so use Spark 3.5)
- A Snowflake account and a running warehouse
- Docker, for the Kafka broker in `phase5_social_media/docker-compose.yml` (Phase 4 producers use the same `localhost:9092` broker)
- [dbt](https://docs.getdbt.com/) 1.11, with a profile named `churn_mart` that can read `FRAUD_DB.CHURN_RAW.RAW_CHURN`
- Apache Airflow 3.2, if you want to run the DAGs (`airflow scheduler` and `airflow api-server`)
- Apache NiFi and HDFS only if you rebuild the Phase 4 UI flows. The Python jobs in that folder run without them.
- Tableau Desktop, to open the Phase 5 dashboards against the `ANALYTICS` schema

```bash
pip install pandas numpy jupyter python-dotenv snowflake-connector-python kafka-python requests pyspark
```

## Credentials

Snowflake (and the failure-email DAGs) read a `.env` file. It is gitignored.

```bash
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=FRAUD_DB
SNOWFLAKE_SCHEMA=FRAUD_SCHEMA
EMAIL_ADDRESS=
GMX_PASSWORD=
```

`phase1_sql/upload_to_stage.py` loads `.env` from the current working directory. Every other Python job calls `load_dotenv` on this absolute path from the original machine:

```text
/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env
```

Put the file at that path, or change those `load_dotenv(...)` calls to your own `.env`, before running Phase 3, 4, or 5. Phase 5 always connects to database `SOCIAL_MEDIA_DB`. The fraud and churn jobs use `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA`.

The Airflow + dbt DAG (`phase3_airflow_dbt/dags/q8_airflow_dbt.py`) also hardcodes the dbt project directory and the dbt binary (`/opt/homebrew/bin/dbt`). Point those at your machine before triggering that DAG.

## How to run

Run each phase from the repository root unless a command says otherwise. Kafka jobs expect a broker on `localhost:9092`. Start it once from Phase 5 and leave it up for Phase 4:

```bash
cd phase5_social_media
docker compose up -d
```

### Phase 1 — SQL fraud analytics

1. Copy the bank fraud CSV to `/tmp/bank_fraud.csv`.
2. In a Snowflake worksheet, run `phase1_sql/sql/00_setup.sql` through the `CREATE OR REPLACE STAGE` statement. That creates `FRAUD_DB`, the transactions table, the file format, and the stage.
3. Upload the file, then load it:

```bash
python phase1_sql/upload_to_stage.py
```

4. Run the `COPY INTO BANK_TRANSACTIONS` statement at the end of `00_setup.sql`.
5. Run `module1_exploration.sql` through `module6_capstone.sql` in order.

Question-by-question notes are in [`phase1_sql/README.md`](phase1_sql/README.md).

### Phase 2 — Pandas churn analysis

Place the telecom CSV at `phase2_pandas/data/telecom_churn.csv`, then open the notebooks. Later modules expect the `TotalCharges` fix from Module 1, so run them in filename order.

```bash
cd phase2_pandas
jupyter notebook
```

### Phase 3 — Airflow and dbt

**dbt churn mart** (Snowflake must already contain `FRAUD_DB.CHURN_RAW.RAW_CHURN`):

```bash
cd phase3_airflow_dbt/churn_mart
dbt run
dbt docs generate && dbt docs serve --port 8081
```

**Airflow DAGs** — copy them into the Airflow home, start the scheduler and API server, then open `http://localhost:8080` and trigger a DAG. They are scheduled daily at 08:00.

```bash
cp phase3_airflow_dbt/dags/*.py ~/airflow/dags/
airflow scheduler
airflow api-server --port 8080
```

Run `airflow scheduler` and `airflow api-server` in separate terminals. DAG behavior is listed in [`phase3_airflow_dbt/README.md`](phase3_airflow_dbt/README.md).

### Phase 4 — NiFi, Kafka, and Spark

Start the Phase 5 Kafka broker first. Then run a producer and its consumer in two terminals.

| Job | Terminal A | Terminal B |
| --- | --- | --- |
| NiFi ingest source | `python phase4_nifi_kafka_spark/nifi/mock_api.py` (serves `http://localhost:8888`) | Build the flow in NiFi. Processor settings are in `THEORY_QUESTIONS.md`. Input file: `nifi/input/sales.csv`. |
| Kafka to NiFi / HDFS | `python phase4_nifi_kafka_spark/kafka/q5_kafka_producer.py` (topic `nifi-transactions`) | NiFi `ConsumeKafka` → `MergeContent` → `PutHDFS` |
| CSV into Kafka | `python phase4_nifi_kafka_spark/kafka/q8_csv_producer.py` (topic `csv-transactions`) | |
| Running customer totals | `python phase4_nifi_kafka_spark/kafka/q9_test_producer.py` | `python phase4_nifi_kafka_spark/kafka/q9_streams_app.py` |
| Top customers per city | `python phase4_nifi_kafka_spark/spark/q11_top_customers.py` | |
| Risk classifier | `python phase4_nifi_kafka_spark/spark/q12_risk_classifier.py` | Writes classified rows to Snowflake. |
| Page views | `python phase4_nifi_kafka_spark/spark/q14_pageview_producer.py` | `python phase4_nifi_kafka_spark/spark/q14_page_views_stream.py` |
| Sensor anomalies | `python phase4_nifi_kafka_spark/spark/q16_sensor_producer.py` | `python phase4_nifi_kafka_spark/spark/q16_anomaly_detection.py` |
| Late sensor data | `python phase4_nifi_kafka_spark/spark/q19_late_data_stream.py` | `python phase4_nifi_kafka_spark/spark/q19_sensor_producer.py` (start the stream first; the producer waits so the stream can catch up) |

`q11` is a self-contained batch job. `q12`, `q16`, and `q19` need the `.env` file described above.

### Phase 5 — social media analytics platform

This is the piece that runs as one pipeline. The simulator emits about 1,000 events per minute (`POST_CREATED`, `LIKE`, `COMMENT`, `SHARE`, `FOLLOW`, `VIDEO_VIEW`, `PROFILE_VISIT`) for 100 users and 500 posts. Spark validates them into `RAW.RAW_EVENTS`, enriches them into `CURATED.CURATED_EVENTS`, and maintains:

- `ANALYTICS.TRENDING_HASHTAGS` — mention counts on 1-, 5-, and 15-minute windows
- `ANALYTICS.VIRAL_POSTS` — posts that pass 500 likes in 5 minutes
- `ANALYTICS.INFLUENCER_RANKING` — engagement score per user on a 15-minute window (`likes×1 + comments×3 + shares×5 + video_views×0.5 + follows×2`)
- `ANALYTICS.COMMENT_SENTIMENT` — keyword classification of comments as positive, neutral, or negative

Architecture, KPI targets, and a file-by-file map are in [`phase5_social_media/README.md`](phase5_social_media/README.md).

From `phase5_social_media`, after Kafka is up and `snowflake/setup.sql` has been run once in a Snowflake worksheet:

```bash
python3 kafka/create_topics.py          # topic social-events, 3 partitions

# separate terminals
python3 simulator/event_simulator.py    # publishes events and seeds USER_DIM / POST_DIM
python3 spark/s4_stream_processor.py    # RAW.RAW_EVENTS
python3 spark/s6_enrichment.py          # CURATED.CURATED_EVENTS
python3 spark/s7_trending_hashtags.py
python3 spark/s8_viral_detection.py
python3 spark/s9_influencer_ranking.py
python3 spark/s10_sentiment.py

python3 kafka/validate_consumer.py      # optional: prints a live count per event type
```

Connect Tableau to `SOCIAL_MEDIA_DB.ANALYTICS` for the trending, viral, and influencer dashboards. Stop the Spark jobs with Ctrl+C. Stop Kafka with `docker compose down` from `phase5_social_media`.
