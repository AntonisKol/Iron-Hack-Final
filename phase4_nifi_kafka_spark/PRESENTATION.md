# Phase 4 — NiFi, Kafka & Spark
**Presentation Guide**

---

## What Was Asked (Technical Brief)

This phase combines theoretical understanding (written answers to architecture questions) with practical coding (working implementations of Kafka producers, a stateful stream processing app, and Spark batch and streaming jobs). The questions cover three technologies: Apache NiFi (data routing), Apache Kafka (distributed message streaming), and Apache Spark (large-scale data processing). Practical files live in `nifi/`, `kafka/`, and `spark/` subfolders.

---

## Technical Breakdown — File by File

---

### `nifi/mock_api.py` — HTTP Source for NiFi (Q2)

Simulates an external REST API that NiFi's `InvokeHTTP` processor calls to pull data. Five hardcoded transaction dicts are stored in a `RECORDS` list. `MockAPIHandler.do_GET()` responds to every GET request with HTTP 200 and the full records list as a JSON body. The server binds to `localhost:8888` and runs forever. NiFi is configured to hit this endpoint via its `InvokeHTTP` processor, passing each response downstream as a FlowFile through the flow: `InvokeHTTP → EvaluateJsonPath → RouteOnAttribute → PutFile`. Records with `status=ERROR` are routed to an alert folder; all others go to a processed folder.

---

### `kafka/q5_kafka_producer.py` — Basic Kafka Producer (Q5)

Sends 2,500 synthetic transaction events to Kafka topic `nifi-transactions`. `KafkaProducer` connects to `localhost:9092` with a `value_serializer` that converts each Python dict to UTF-8 JSON bytes — Kafka only stores raw bytes, so serialisation happens on the producer side. The event generation loop creates a random record per iteration and calls `producer.send(TOPIC, value=record)`, which is non-blocking: it queues the message in an internal buffer that the background I/O thread batches and sends to the broker. The loop prints progress every 500 records. `producer.flush()` at the end blocks until all in-flight messages are acknowledged by the broker — without it, the script might exit before all messages are delivered.

---

### `kafka/q8_csv_producer.py` — CSV-to-Kafka Producer with Guaranteed Delivery (Q8)

Reads a CSV file row by row and publishes each row as a Kafka message, simulating a legacy system feeding a real-time pipeline. `KafkaProducer` is configured with `acks='all'` (strongest delivery guarantee — all in-sync replicas must confirm), `retries=5`, and `retry_backoff_ms=200`. The key concept here is delivery callbacks: `on_success` and `on_error` are registered on the Future returned by each `send()` call and run in a background thread when the broker responds. `on_success` logs the partition number and offset — the position within the partition — confirming exactly where the message landed. `on_error` logs the exception if all retries are exhausted. `csv.DictReader` turns each CSV row into a Python dict keyed by column headers. `producer.flush()` waits for all pending callbacks before the script exits.

---

### `kafka/q9_streams_app.py` — Stateful Stream Processing / KTable Pattern (Q9)

Simulates Kafka Streams' KTable pattern in Python. Consumes from topic `transactions`, maintains a running total per customer in an in-memory dict (`running_totals = {customer_id: total}`), and produces an update to `high-value-customers` whenever a customer's accumulated total exceeds $10,000. The dict is the state store — in real Kafka Streams this is backed by RocksDB on disk for fault tolerance. For each incoming message: the customer's existing total is retrieved from the dict, the new amount is added, and the dict is updated. This is stateful processing — the output depends on all previous messages, not just the current one. When the threshold is crossed, a summary message is sent to the output topic with the customer ID, running total, and latest transaction details. `acks='all'` on the output producer ensures the alert is not lost.

---

### `kafka/q9_test_producer.py` — Test Data Seeder

Feeds `q9_streams_app.py` with a controlled mix of transactions to verify the threshold logic. The `transactions` list is hardcoded with a mix of customers — some have multiple entries that sum above $10,000 (should trigger the alert to `high-value-customers`), others have totals below the threshold (should be silently accumulated). `acks='all'` ensures all test data is durably written to Kafka before the streams app starts consuming.

---

### `spark/q11_top_customers.py` — Batch Spark: Top Customers Per City (Q11)

Demonstrates Spark's core batch processing concepts. `SparkSession.builder.appName(...).master('local[*]')` initialises a local session using all available CPU cores — in production this would point to a cluster. Two Python lists of tuples are converted to Spark DataFrames with `spark.createDataFrame(data, schema=[...])`. The processing pipeline is built as a series of transformations (lazy — nothing runs yet): an inner join on `customer_id`, a `groupBy` aggregation summing order amounts per customer, a window function `Window.partitionBy('city').orderBy(desc('total_spend'))` that assigns `rank()` within each city restarting per partition, and a filter keeping only `rank <= 5`. The first action — `top5_df.write.mode('overwrite').partitionBy('city').parquet(output_path)` — triggers execution of all transformations in one optimised pass. `partitionBy('city')` creates one subfolder per city so that a query filtered on London only reads the London folder.

---

### `spark/q12_risk_classifier.py` — Batch Spark: UDF Risk Classifier (Q12)

Shows how to apply custom Python logic to every row in a Spark DataFrame via a User-Defined Function. `spark.read.json(JSON_PATH)` reads newline-delimited JSON with automatic schema inference. `classify_risk(amount, credit_score, failed_attempts)` implements a three-tier rule: HIGH if amount > 10,000 OR credit_score < 500 OR failed_attempts > 3; MEDIUM if amount > 5,000 OR credit_score < 650; LOW otherwise. `udf(classify_risk, StringType())` wraps the function so Spark can call it in parallel across all executors. `df.withColumn('risk_level', classify_udf(...))` adds the classification column as a transformation. `df_classified.cache()` stores the result in memory so the UDF doesn't re-execute for each of the three downstream filters. `write_to_snowflake()` calls `.toPandas()` (an action, triggering Spark), then uses `cursor.executemany()` to bulk-insert rows into `RISK_HIGH`, `RISK_MEDIUM`, and `RISK_LOW` tables.

---

### `spark/q14_page_views_stream.py` — Structured Streaming: Page View Counter (Q14)

Reads a real-time stream of page-view events from Kafka topic `page-views`, applies event-time windowing, and counts views per page per 5-minute window. The SparkSession includes `spark.jars.packages = org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` so the Kafka connector JAR is downloaded automatically. The schema defines `user_id`, `page`, and `ts` as `StringType` — timestamp is parsed as a string first to avoid `from_json` timezone issues, then cast to timestamp with `.cast('timestamp')`. `withWatermark('ts', '10 minutes')` tells Spark to accept events up to 10 minutes late and drop anything older. `groupBy(window(col('ts'), '5 minutes'), col('page')).count()` groups events into non-overlapping 5-minute buckets per page. `outputMode('update')` re-emits a window row whenever it changes — including when a late event updates an already-computed window.

**Companion: `q14_pageview_producer.py`** sends 30 synthetic events to `page-views`, spread randomly across the last 10 minutes to create a natural late-data demonstration.

---

### `spark/q16_anomaly_detection.py` — Structured Streaming: IoT Anomaly Detection (Q16)

Detects temperature anomalies in real-time IoT sensor data from Kafka topic `sensor-readings`. Events are decoded with `from_json` using a `StructType` schema. The anomaly detection logic uses `foreachBatch` rather than Spark's built-in window aggregation because each incoming reading must be compared against a rolling 1-hour average from Snowflake history — Spark's window function produces the aggregate but loses the raw events. Each micro-batch: persists readings to `SENSOR_READINGS` in Snowflake, queries `AVG + STDDEV_POP` over the last hour per sensor (requiring at least 10 readings to avoid cold-start false positives), then flags any reading where `temperature > avg + 3×std`. Confirmed anomalies are written to both Kafka topic `sensor-alerts` (with a graceful fallback if the broker is unreachable) and Snowflake `SENSOR_ANOMALY_ALERTS`.

---

### `spark/q19_late_data_stream.py` — Structured Streaming: Late-Data Handling with Upsert (Q19)

Full implementation of the late-data architecture described in the Q19 theory answer. Reads JSON sensor files from a local directory (written by `q19_sensor_producer.py`). `withWatermark('ts', '10 minutes')` accepts events up to 10 minutes late. `groupBy(window(col('ts'), '5 minutes'), col('sensor_id')).agg(avg('temperature'), count('*'))` computes per-sensor averages in 5-minute tumbling windows. `outputMode('update')` re-emits a window whenever it changes. In `upsert_to_snowflake`: for each batch row, a `SELECT version FROM SENSOR_AGGREGATES` check determines whether the window already exists. If yes — this is a late-data correction: the version number is incremented, `is_correction=True`, and an `UPDATE` is applied. If no — first arrival, version=1, `INSERT`. Every batch also appends an immutable row to `SENSOR_CORRECTIONS_HISTORY` — a full audit trail of every version of every window.

**Companion: `q19_sensor_producer.py`** writes two phases of events: 60 on-time readings, then a 35-second pause, then 15 deliberately late readings with timestamps 7–9 minutes in the past. These fall into windows Spark has already processed, triggering the correction path.

---

## For the Room — Plain-Language Walkthrough

---

### Q2 — NiFi: The Smart Conveyor Belt

Apache NiFi is a tool for moving data from one place to another — automatically, on a schedule, with rules about what to do if something goes wrong. You draw a flowchart on screen (drag and drop, no code), set the rules, and NiFi runs it around the clock. Think of a mail-sorting machine: letters arrive on one belt, the machine reads the label, and routes them to different bins — priority post here, regular post there, undeliverable ones to the side. Our mock API here plays the role of the letter sender: it responds with transaction records, and NiFi decides what to do with each one based on its status.

### Q5 — Kafka Producer: Dropping Messages into the Pipeline

Kafka is like a very fast, very reliable postal system. You drop a message into a labelled mailbox (a "topic"), and anyone who subscribes to that mailbox receives it — at their own pace, in their own time, without you having to wait for them. Q5 builds the "mail sender" side of this: a Python script that generates 2,500 fake bank transactions and drops them into a Kafka topic one by one. The key engineering detail is that sending is non-blocking — the script doesn't wait for each message to be confirmed before sending the next one. It fires them off, and at the end it pauses just long enough to confirm they all arrived safely before shutting down.

### Q8 — Reliable Kafka: What Happens When Something Goes Wrong?

Q8 takes the basic producer from Q5 and makes it production-grade. The key addition is delivery callbacks — for every message sent, the producer registers two functions: one to call if the message was successfully stored ("it arrived at partition 3, position 142"), and one to call if all retries were exhausted and it still failed ("this message was lost"). This is the difference between fire-and-forget and guaranteed delivery. The producer also reads from a CSV file rather than generating random data, simulating a real-world scenario where a legacy database export feeds into a modern streaming pipeline.

### Q9 — Kafka Streams: Keeping Score Across Messages

This is where Kafka gets interesting. Q9 builds a stream processor that does something more than just forwarding messages — it keeps a running total. Imagine a tally counter. Every time a transaction arrives for a customer, the counter increments. When the counter crosses $10,000 for any one customer, the app sends an alert to a separate output channel. The trick is that this counter must survive across messages — message 5 from a customer must know about messages 1 through 4. This is called stateful processing, and it's the foundation of real-time fraud detection, loyalty scoring, and risk monitoring in production banking systems.

### Q11 — Spark Batch: Ranking the Best Customers

Spark is a framework for processing large amounts of data very quickly by splitting the work across multiple computers (or multiple CPU cores). Q11 uses Spark to answer a question that sounds simple but gets expensive at scale: "Who are the top five customers by total spend, broken down by city?" The interesting technical detail is how Spark approaches this: it builds a plan (join the orders to the customer list, sum their spending, rank them within each city) but doesn't actually run anything until the last step — when you ask it to save the results. At that point it executes everything in one optimised pass, like a kitchen that preps all the ingredients before cooking everything at once.

### Q12 — Spark UDF: Teaching Spark Your Own Rules

Spark knows how to count, sum, and group. But what if you want to apply a custom business rule — one that takes three inputs and returns a category? Q12 does exactly this: it defines a Python function that labels each transaction as High, Medium, or Low risk based on the transaction amount, the customer's credit score, and how many times they failed authentication. That function is then registered as a "User-Defined Function" and applied to every row in the dataset in parallel. Spark farms out the work across all available cores simultaneously. The results are then split into three tables in Snowflake — one per risk level — for the fraud team to act on.

### Q14 — Spark Streaming: Counting Page Views in Real Time

Q14 connects Spark directly to Kafka so it processes data continuously — not in batches, but as a rolling stream. The use case is counting how many times each page on a website was viewed, in 5-minute windows. Every 5 minutes, Spark publishes the latest counts: "Page A had 47 views between 14:00 and 14:05." The engineering challenge here is late data: what if a page-view event arrives at 14:07 but its timestamp says 14:03? Q14 handles this gracefully — Spark accepts events up to 10 minutes late, updates the window if needed, and re-publishes the corrected count. Nothing is silently discarded.

### Q16 — Spark Streaming: Catching the Hot Sensor

Q16 is a real-time alarm system for IoT sensors. Temperature readings arrive from Kafka, and for each one, the system asks: is this reading unusual given what this sensor has done in the last hour? If the temperature is more than three standard deviations above that sensor's recent average — the statistical definition of an outlier — an alert fires immediately. The alert goes to two places: back into Kafka (so any other system monitoring alerts can react) and into Snowflake (for the long-term audit trail). A reading must wait until at least ten prior readings exist before it can trigger an alert — preventing false alarms during the first few seconds after a sensor comes online.

### Q19 — Spark Streaming: Fixing the Past When Data Arrives Late

This is the most sophisticated piece of the phase. Imagine a temperature sensor in a factory that sends readings every few seconds. One reading gets delayed in transit and arrives 8 minutes after it was taken. The system has already published the average temperature for that time window — but that average is now wrong. Q19 handles this by keeping the door open: when a late reading arrives within the 10-minute window, Spark recalculates the affected time period's average, updates the stored result, increments a version counter, and writes a new row to a corrections history table so auditors can see every version of every result. The companion script demonstrates this deliberately — it sends 60 on-time readings, pauses, then sends 15 readings with timestamps from 8 minutes ago. The correction logs appear in Snowflake exactly as expected.

---

## How to Run — End to End

### Prerequisites

- Kafka running on `localhost:9092` (no docker-compose in Phase 4 — use Phase 5's docker-compose or a local Kafka install)
- PySpark installed with the Kafka connector JAR (`spark-sql-kafka-0-10_2.12:3.5.3`)
- `kafka-python` installed: `pip install kafka-python`
- `.env` file at project root with Snowflake credentials (needed for Q12, Q16)
- A CSV file for Q8 (any CSV with a header row will work; the producer reads it row by row)

---

### Step 1 — Start Kafka

Using Phase 5's docker-compose (simplest option):

```bash
cd ../phase5_social_media
docker-compose up -d
```

Wait ~15 seconds for Zookeeper and the Kafka broker to be ready.

---

### Step 2 — Create the Kafka topics needed by each exercise

```bash
# Q5 and Q8 — basic producer and CSV producer
kafka-topics.sh --create --topic nifi-transactions \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Q9 — stateful streams app (input and output topics)
kafka-topics.sh --create --topic transactions \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic high-value-customers \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Q14 — Spark page-view streaming
kafka-topics.sh --create --topic page-views \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Q16 — IoT anomaly detection
kafka-topics.sh --create --topic sensor-readings \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic sensor-alerts \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

### Step 3 — NiFi mock API (Q2)

```bash
# Terminal 1: start the mock REST API on localhost:8888
python phase4_nifi_kafka_spark/nifi/mock_api.py
```

In the NiFi UI (`http://localhost:8443/nifi`), wire the processors:
`InvokeHTTP` (GET http://localhost:8888) → `EvaluateJsonPath` → `RouteOnAttribute` → `PutFile`

---

### Step 4 — Kafka exercises (Q5, Q8, Q9)

```bash
# Q5 — basic producer (sends 2,500 synthetic transactions)
python phase4_nifi_kafka_spark/kafka/q5_kafka_producer.py

# Q8 — CSV producer with delivery callbacks
python phase4_nifi_kafka_spark/kafka/q8_csv_producer.py

# Q9 — stateful streams (run the seeder first, then the streams app)
# Terminal 1: seed the transactions topic with test events
python phase4_nifi_kafka_spark/kafka/q9_test_producer.py
# Terminal 2: streams app reads from 'transactions', writes alerts to 'high-value-customers'
python phase4_nifi_kafka_spark/kafka/q9_streams_app.py
```

---

### Step 5 — Spark batch jobs (Q11, Q12)

```bash
# Q11 — top customers per city
spark-submit phase4_nifi_kafka_spark/spark/q11_top_customers.py

# Q12 — UDF risk classifier (writes three Snowflake tables: HIGH / MEDIUM / LOW risk)
spark-submit phase4_nifi_kafka_spark/spark/q12_risk_classifier.py
```

---

### Step 6 — Spark Structured Streaming (Q14, Q16, Q19)

Each streaming job runs in one terminal; the companion producer runs in a second terminal.

```bash
# Q14 — page-view counter in 5-minute windows
# Terminal 1: start the stream processor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase4_nifi_kafka_spark/spark/q14_page_views_stream.py
# Terminal 2: generate page-view events into Kafka
python phase4_nifi_kafka_spark/spark/q14_pageview_producer.py

# Q16 — IoT anomaly detection
# Terminal 1: stream processor (reads sensor-readings, writes alerts to sensor-alerts + Snowflake)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  phase4_nifi_kafka_spark/spark/q16_anomaly_detection.py
# (send sensor events to the sensor-readings topic manually or via a producer)

# Q19 — late-data upsert
# Terminal 1: stream processor
spark-submit phase4_nifi_kafka_spark/spark/q19_late_data_stream.py
# Terminal 2: companion producer (sends 60 on-time readings, then 15 late ones)
python phase4_nifi_kafka_spark/spark/q19_sensor_producer.py
```

---

### Shutdown

```bash
# Ctrl+C all running Spark and producer terminals
# Then stop Kafka
cd ../phase5_social_media
docker-compose down
```
