# Phase 4 — NiFi, Kafka & Spark
**Presentation Guide**

---

## What Was Asked (Technical Brief)

This phase combines **theoretical understanding** (written answers to architecture questions) with **practical coding** (working implementations of Kafka producers, a stateful stream processing app, and Spark batch and streaming jobs). The questions cover three technologies: Apache NiFi (data routing and transformation flows), Apache Kafka (distributed message streaming), and Apache Spark (large-scale data processing). Practical files sit in `nifi/`, `kafka/`, and `spark/` subfolders.

---

## Technical Breakdown — File by File

---

### `nifi/mock_api.py` — HTTP Source for NiFi (Q2)

**Purpose:** Simulate an external REST API that NiFi's `InvokeHTTP` processor can call to pull data.

- **`RECORDS` list:** Five hardcoded transaction dicts — each with `id`, `name`, `amount`, and `status`.
- **`MockAPIHandler.do_GET()` method:** Responds to every GET request with HTTP 200, `Content-Type: application/json`, and the full records list as a JSON body.
- **`HTTPServer` startup block:** Binds to `localhost:8888`, enters `serve_forever()`. NiFi is configured to hit `http://localhost:8888/` via its `InvokeHTTP` processor, which passes each response FlowFile downstream for processing.

The NiFi flow this supports: `InvokeHTTP → EvaluateJsonPath → RouteOnAttribute → PutFile` — fetches the API response, extracts fields, routes ERROR status records to an alert folder and SUCCESS records to a processed folder.

---

### `kafka/q5_kafka_producer.py` — Basic Kafka Producer (Q5)

**Purpose:** Send 2,500 synthetic transaction events to Kafka topic `nifi-transactions`. NiFi's `ConsumeKafka` processor on the other end reads these and demonstrates the MergeContent batching flow (Q5 theory question).

- **`KafkaProducer` init block:** Connects to `localhost:9092`. `value_serializer` converts each Python dict to UTF-8 JSON bytes before sending — Kafka only stores raw bytes.
- **Event generation loop:** Creates `{'transaction_id': ..., 'customer_id': ..., 'amount': ..., 'country': ..., 'merchant': ...}` with `random` values. Calls `producer.send(TOPIC, value=event)`. Runs 2,500 times to produce two full MergeContent batches (each triggers at 1,000 records) plus one partial batch (triggers at the 10-second timeout).
- **`producer.flush()` at end:** Blocks until all in-flight messages are acknowledged by the Kafka broker. Without this, the script might exit before all messages are delivered.

---

### `kafka/q8_csv_producer.py` — CSV-to-Kafka Producer (Q8)

**Purpose:** Read a CSV file row by row and publish each row as a Kafka message — simulating a legacy system feeding a real-time pipeline.

- **Delivery callbacks block (`on_send_success`, `on_send_error`):** Functions passed to the Kafka send Future. `on_send_success` logs the topic, partition, and offset of the delivered message. `on_send_error` logs the exception. These demonstrate **guaranteed delivery** — the producer knows whether each message arrived.
- **CSV reading block:** Opens `transactions.csv` with `csv.DictReader` — each row becomes a Python dict keyed by column headers. `float()` cast on `amount` ensures the numeric value serialises cleanly as JSON.
- **`producer.send(TOPIC, value=row).add_callbacks(success=..., error=...)` block:** Non-blocking send. The Future callback fires asynchronously when the broker responds.
- **`producer.flush()` at end:** Waits for all pending callbacks before the script exits.

---

### `kafka/q9_streams_app.py` — Stateful Stream Processing (Q9)

**Purpose:** Simulate Kafka Streams' KTable pattern in Python. Consumes `transactions`, maintains a running total per customer in memory, and produces an alert to `high-value-customers` when any customer's total crosses $10,000.

- **`running_totals` dict block:** `{customer_id: running_total}` — the in-memory state store. In real Kafka Streams this is backed by RocksDB on disk for fault tolerance.
- **Consumer loop:** `KafkaConsumer` reads from `transactions` topic. For each message: parses JSON, looks up the customer's existing total in the dict, adds the new amount, updates the dict. This is the **stateful** part — the decision depends on all previous messages, not just the current one.
- **Threshold check block:** `if running_totals[customer_id] >= FILTER_THRESHOLD (10,000)`: sends a summary message to `high-value-customers` topic with customer ID, total amount, and number of transactions. Does this on every update once the threshold is crossed — so the downstream consumer gets the latest running total each time.

---

### `kafka/q9_test_producer.py` — Test Data Seeder

**Purpose:** Feed `q9_streams_app.py` with a realistic mix of transactions to exercise the threshold logic.

- **`transactions` list:** Hardcoded mix — some customers have multiple entries that sum above $10,000 (should trigger the alert), others have totals below the threshold (should be silently accumulated). Used to verify the streams app behaves correctly.
- **`producer.send` loop + `acks='all'` config:** `acks='all'` tells Kafka to wait for all in-sync replicas to acknowledge the message — strongest delivery guarantee. Used here so test data is definitely in Kafka before the streams app starts consuming.

---

### `spark/q11_top_customers.py` — Batch Spark: Top Customers Per City (Q11)

**Purpose:** Demonstrate Spark's core batch processing concepts: SparkSession, DataFrame creation, transformations, window functions, and writing Parquet output.

- **SparkSession block:** `SparkSession.builder.appName(...).master('local[*]')` — `local[*]` means use all available CPU cores. In production this would be `spark://master:7077` pointing to a cluster.
- **Sample data block:** Two Python lists of tuples (`orders_data`, `customers_data`). `spark.createDataFrame(data, schema=[...])` converts them into Spark DataFrames with explicit column names.
- **STEP 1 — JOIN transformation:** `orders_df.join(customers_df, on='customer_id', how='inner')` — standard inner join. This is a **transformation** — Spark builds the plan but nothing executes yet (lazy evaluation).
- **STEP 2 — Aggregation transformation:** `groupBy('customer_id', 'name', 'city').agg(_sum('amount').alias('total_spend'))` — sum all orders per customer.
- **STEP 3 — Window function transformation:** `Window.partitionBy('city').orderBy(desc('total_spend'))` defines the window. `rank().over(window)` assigns rank 1 to the highest spender in each city, restarting for each new city. Still no execution.
- **STEP 4 — Filter transformation:** `filter(col('rank') <= 5)` — keep only the top 5 per city.
- **STEP 5 — Parquet write (ACTION):** `top5_df.write.mode('overwrite').partitionBy('city').parquet(output_path)` — this is the first **action**, triggering execution of all four transformations in one optimised pass. `partitionBy('city')` creates separate subfolders per city, so a query for London only reads the London folder.

---

### `spark/q12_risk_classifier.py` — Batch Spark: UDF Risk Classifier (Q12)

**Purpose:** Show how to apply custom Python logic (a User-Defined Function) to every row in a Spark DataFrame, then write the results to Snowflake.

- **`SNOWFLAKE_CONFIG` block:** Credentials from `.env`, passed to `snowflake.connector.connect()`.
- **STEP 1 — Read JSON:** `spark.read.json(JSON_PATH)` — reads `risk_records.json` (newline-delimited JSON, one object per line). Spark infers the schema automatically.
- **STEP 2 — Define UDF:** `classify_risk(amount, credit_score, failed_attempts)` — three-tier rule: HIGH if amount > 10,000 OR credit_score < 500 OR failed_attempts > 3; MEDIUM if amount > 5,000 OR credit_score < 650; LOW otherwise. `udf(classify_risk, StringType())` wraps it so Spark can call it in parallel across all executors.
- **STEP 3 — Apply UDF:** `df.withColumn('risk_level', classify_udf(col(...), col(...), col(...)))` — adds a new column by calling the UDF on three existing columns per row.
- **STEP 4 — Split + cache:** `df_classified.cache()` stores the result in memory so the UDF doesn't re-execute for each of the three filter operations that follow.
- **STEP 5 — Write to Snowflake:** `write_to_snowflake()` calls `.toPandas()` (an ACTION, triggers Spark), then uses `cursor.executemany()` for batch insert into `RISK_HIGH`, `RISK_MEDIUM`, `RISK_LOW` tables.

---

### `spark/q14_page_views_stream.py` — Structured Streaming: Page Views (Q14)

**Purpose:** Read a real-time stream of page-view events from Kafka, apply event-time windowing with watermark, and count views per page per 5-minute window.

- **SparkSession block:** Includes `spark.jars.packages = org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` — the Kafka connector JAR that Spark downloads automatically.
- **Schema block:** `StructType` with `user_id`, `page`, `timestamp` — all `StringType`. Timestamp is parsed as string first to avoid `from_json` timezone issues, then cast to timestamp.
- **READ FROM KAFKA block:** `readStream.format('kafka').option('subscribe', 'page-views').load()` — reads incoming Kafka messages. `.select(from_json(col('value').cast('string'), schema).alias('d')).select('d.*')` — decodes the JSON bytes into structured columns.
- **Watermark + Window block:** `.withWatermark('ts', '10 minutes')` — accept events up to 10 minutes late; drop anything older. `groupBy(window(col('ts'), '5 minutes'), col('page')).count()` — counts views per page in non-overlapping 5-minute buckets.
- **Write block:** `writeStream.outputMode('update').format('console')` — re-prints every window row when it changes (as new events arrive or late events update a closed window).

**Companion file: `q14_pageview_producer.py`** — sends 30 synthetic page-view events to `page-views` topic, spread randomly across the last 10 minutes to create a natural late-data scenario.

---

### `spark/q16_anomaly_detection.py` — Structured Streaming: IoT Anomaly Detection (Q16)

**Purpose:** Detect temperature anomalies in real-time IoT sensor data. Alert when a reading exceeds AVG + 3×STDDEV computed over the past hour for that sensor.

- **SNOWFLAKE + KAFKA CONFIG block:** Credentials from `.env`. Bootstrap servers `localhost:9092`.
- **Schema block:** `StructType` with `sensor_id`, `temperature` (DoubleType), `timestamp` (TimestampType).
- **READ FROM KAFKA block:** Subscribes to `sensor-readings`. Parses JSON bytes with `from_json`. Casts timestamp string to proper timestamp type.
- **Rolling statistics block (foreachBatch):** Spark's window aggregation only computes forward-looking windows. To compare each incoming reading against a 1-hour rolling average, we use `foreachBatch` — each micro-batch triggers a Python function that queries Snowflake for the historical stats of that sensor, then applies the threshold.
- **Anomaly detection logic:** If `temperature > avg_temp + 3 × std_temp` (and at least 10 readings exist for that sensor, to avoid cold-start false positives) — the reading is flagged.
- **Dual output:** Anomalies are written to Snowflake `SENSOR_ANOMALY_ALERTS` AND produced as messages to Kafka topic `sensor-alerts` — so downstream consumers (dashboards, paging systems) can react immediately.

---

### `spark/q19_late_data_stream.py` — Structured Streaming: Late-Data Handling (Q19)

**Purpose:** Full demonstration of the late-data handling architecture described in the Q19 theory answer — Kafka → Spark → Snowflake with MERGE upsert and a corrections history table.

- **Schema block:** `sensor_id`, `temperature`, `ts` (timestamp).
- **Watermark + window aggregation block:** `withWatermark('ts', '10 minutes')` + `groupBy(window(col('ts'), '5 minutes'), col('sensor_id'))` + `agg(avg('temperature'), count('*'))`.
- **`outputMode('update')`:** Re-emits a window every time it changes — including when a late event updates an already-closed window. Each re-emission triggers `foreachBatch`.
- **`upsert_to_snowflake` function:** For each batch row, checks if the window already exists in `SENSOR_AGGREGATES`. If yes — this is a correction: increments the version number, sets `is_correction=True`. Runs a Snowflake `MERGE` (upsert) on `SENSOR_AGGREGATES` (keeps the latest value) and an `INSERT` on `SENSOR_CORRECTIONS_HISTORY` (immutable audit trail). If no — first write, version=1.

---

## For the Room (Non-Technical)

Let me explain what these three technologies do using everyday analogies.

---

**NiFi — the conveyor belt**

Imagine a factory where packages arrive on one conveyor belt and need to be sorted onto different belts — big packages to shipping, broken ones to repair, everything else to storage. Apache NiFi does exactly this, but for data. You draw the flow visually (drag and drop), set the rules, and NiFi runs it 24 hours a day. If something breaks, the package doesn't disappear — it waits in a queue until the problem is fixed.

In this phase: NiFi picks up files from one folder, calls our fake bank API, and routes the records based on their status.

---

**Kafka — the world's fastest post office**

Imagine a post office that handles a million letters per second, never loses a single one, and lets multiple teams pick up their own copy of every letter independently. That's Kafka.

A producer drops messages into a topic (like a mailbox labelled "transactions"). Any number of consumers can read from that mailbox — each getting their own copy, at their own pace. If a consumer goes down, Kafka keeps the messages. When it comes back, it picks up where it left off.

We built a producer that reads a CSV file and puts each row into Kafka. We built a stream processor that reads those rows and keeps a running score per customer. When any customer's total crosses $10,000, it fires an alert.

---

**Spark — the factory floor**

If Kafka is the post office, Spark is the factory that processes everything inside. It splits the work across all available computers (or all CPU cores on your laptop) and processes everything in parallel.

For batch processing: we gave Spark a list of customer orders, asked it to join with the customer list, add up each person's total spend, rank them within each city, and save the top 5 per city to files. All of that in one optimised pass.

For streaming: we connected Spark directly to Kafka, so it continuously processes new messages as they arrive — like an assembly line that never stops. We used it to count website page views per 5-minute window, detect sensor temperature spikes in real time, and automatically correct past summaries when late-arriving sensor data showed up.

---

**The key insight from this whole phase:**

In most systems, if a message arrives late — say a sensor reading that got delayed in transit — it just gets ignored or causes an error. We built something smarter: the system *accepts* late data, updates the past result, keeps a history of every correction, and records which version each number came from. That's the kind of resilience you need in a real production system.

---

**One more analogy for the room:**

Imagine you're counting votes in an election. You have a result at 11 PM. But some mail-in ballots arrive the next morning — they're valid, just late. Our system doesn't throw those away. It reopens the count for that voting window, updates the total, records the original count AND the corrected one, and notes that a correction happened. That's exactly what we implemented for sensor readings.
