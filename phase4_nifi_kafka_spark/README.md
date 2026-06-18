# Phase 4 — NiFi, Kafka & Spark
**Real-Time Streaming & Data Processing**

---

## Coding Tasks

| Q | File | What it does |
| --- | ------ | ------------- |
| Q2 | `nifi/mock_api.py` | Flask API serving JSON sales records — used as the HTTP source in a NiFi InvokeHTTP flow |
| Q5 | `kafka/q5_kafka_producer.py` | Produces 50 JSON transaction events to topic `nifi-transactions` (1 msg/s) |
| Q8 | `kafka/q8_csv_producer.py` | Reads `transactions.csv`, streams each row as a JSON message to topic `transactions` |
| Q9 | `kafka/q9_streams_app.py` | Kafka Streams app: consumes `transactions`, filters high-value (>500) rows, produces to `high-value-transactions`; `q9_test_producer.py` seeds test data |
| Q11 | `spark/q11_top_customers.py` | Batch Spark job: reads `transactions.csv`, groups by `customer_id`, sums `amount`, returns top 10 by total spend |
| Q12 | `spark/q12_risk_classifier.py` | Batch Spark job: applies rule-based risk classification (Low / Medium / High) using `amount`, `country`, `device_type`; writes results to `risk_records.json` |
| Q14 | `spark/q14_page_views_stream.py` | Structured Streaming: reads page-view events from Kafka topic `page-views`, applies a 10-minute watermark and 5-minute tumbling window, counts views per page per window; `q14_pageview_producer.py` seeds the topic |
| Q16 | `spark/q16_anomaly_detection.py` | Structured Streaming: reads IoT sensor readings from Kafka topic `sensor-readings`, computes rolling 1-hour AVG + STDDEV per sensor, flags temperature > avg + 3σ as anomalies, writes alerts to Kafka topic `sensor-alerts` and Snowflake |
| Q19 | `spark/q19_late_data_stream.py` | Structured Streaming: reads from Kafka topic `sensor-readings` using `outputMode('update')` + `foreachBatch` MERGE-upsert into Snowflake, demonstrating how re-emitted windows are handled when late data arrives; `q19_sensor_producer.py` seeds the topic |

---

## Theory Questions & Answers

---

### Q1 — What is a FlowFile in Apache NiFi?

A FlowFile is the basic unit of data in NiFi — think of it like an envelope moving through a conveyor belt. Every piece of data that travels through a NiFi flow is wrapped in a FlowFile.

**Two main components:**

1. **Attributes** — metadata about the data, stored as key-value pairs
   - Examples: `filename=sales.csv`, `fileSize=2048`, `uuid=abc-123`
   - Like the label on the outside of the envelope — tells you what's inside without opening it

2. **Content** — the actual data payload
   - The bytes of the file, JSON, CSV row, etc.
   - Like the letter inside the envelope

**What happens when a processor fails:**

When a processor fails, NiFi does NOT delete the data. Instead:
- The FlowFile is routed to the **failure** relationship of that processor
- It sits in the failure queue and can be retried, redirected, or inspected
- NiFi guarantees **data provenance** — every FlowFile has a full history of where it's been
- This is the **guaranteed delivery** model — data is never silently lost

---

### Q3 — Back Pressure Object Threshold vs Back Pressure Data Size Threshold

Both settings live on a NiFi **connection** (the arrow between two processors). They control when NiFi stops sending more data into that connection to prevent overload.

**Back Pressure Object Threshold**
- Limits the **number of FlowFiles** queued in a connection
- Example: set to 10,000 → when 10,000 FlowFiles are waiting, the upstream processor pauses
- Use when: you care about the count of messages (e.g. API calls where each message = 1 request)

**Back Pressure Data Size Threshold**
- Limits the **total size in bytes** of all FlowFiles in the connection
- Example: set to 1 GB → when 1 GB of data is queued, the upstream processor pauses
- Use when: you care about memory/disk usage (e.g. large binary files or video)

**When to tune each:**
- Tune **object threshold** when processing many small messages (logs, JSON events)
- Tune **data size threshold** when processing large files (CSVs, images, Parquet files)
- In practice, tune both — whichever limit is hit first triggers the back pressure

---

### Q5 — NiFi Kafka Ingest with MergeContent

**Goal:** Consume a Kafka topic in NiFi, batch every 1,000 records (or 10 seconds, whichever comes first) into a single JSON array, and write to HDFS.

**Flow:**
```
ConsumeKafka → MergeContent → PutHDFS
```

**Processor settings:**

| Processor | Key Property | Value |
|-----------|-------------|-------|
| ConsumeKafka | Bootstrap Servers | `localhost:9092` |
| ConsumeKafka | Topic Name | `nifi-transactions` |
| ConsumeKafka | Group ID | `nifi-consumer` |
| ConsumeKafka | Offset Reset | `earliest` |
| MergeContent | Merge Strategy | Bin-Packing Algorithm |
| MergeContent | Minimum Number of Entries | `1000` |
| MergeContent | Maximum Number of Entries | `1000` |
| MergeContent | Maximum Bin Age | `10 sec` |
| MergeContent | Header | `[` |
| MergeContent | Demarcator | `,` |
| MergeContent | Footer | `]` |
| PutHDFS | Directory | `/data/transactions` |

**How MergeContent batching works:**

MergeContent holds FlowFiles in a bin and releases the bin when EITHER condition is met first:
- **1,000 records collected** — full batch, write immediately
- **10 seconds elapsed** — partial batch, write whatever arrived (prevents data being stuck if Kafka traffic is low)

This is called a **time-or-count trigger** — it guarantees both throughput and latency bounds.

**Failure handling strategies:**

1. **ConsumeKafka failure**
   - Kafka offsets are only committed after NiFi acknowledges processing — if NiFi crashes, messages are re-consumed from the last committed offset. No data loss.
   - Route `parse.failure` to a dead-letter folder for inspection.

2. **MergeContent failure**
   - Route `failure` relationship to a PutFile in an error folder — partial batches are saved, not dropped.
   - Use LogMessage processor to log the failure with FlowFile attributes for debugging.

3. **PutHDFS failure**
   - HDFS is unavailable → route `failure` to a local staging folder. A separate flow retries writing once HDFS recovers.
   - Use RetryFlowFile processor for automatic retries with exponential back-off.

4. **Back pressure**
   - Set Object Threshold on the ConsumeKafka → MergeContent connection to pause consumption if MergeContent falls behind. Kafka retains the messages — they are not lost, just delayed.

5. **Provenance**
   - Every FlowFile has a full audit trail in NiFi Data Provenance — you can trace exactly which Kafka offset produced which file.

**Local implementation note:**
In this project, PutHDFS is replaced with PutFile writing to `kafka/output/` since HDFS is not running locally. The flow logic is identical — only the destination processor changes.

---

### Q6 — Kafka Broker, Topic, Partition, and Consumer Group

**Broker**
A Kafka broker is a server that stores messages and handles read/write requests. A Kafka cluster is made up of multiple brokers for redundancy and scale. Think of it as the post office building.

**Topic**
A topic is a named category where messages are published. Producers write to topics, consumers read from them. Think of it as a mailbox label (e.g. "transactions", "fraud-alerts").

**Partition**
A topic is split into partitions — ordered, immutable sequences of messages. Each partition lives on one broker. Partitions enable parallelism: multiple consumers can read from different partitions simultaneously. Think of it as separate lanes on a highway.

**Consumer Group**
A consumer group is a set of consumers that together read all the partitions of a topic. Each partition is assigned to exactly one consumer in the group at a time. Think of it as a team of workers — each worker handles a different lane.

**How they relate:**

```
Topic: "transactions"
├── Partition 0  →  Consumer A (in Group 1)
├── Partition 1  →  Consumer B (in Group 1)
└── Partition 2  →  Consumer C (in Group 1)

A second Consumer Group reads ALL partitions independently.
```

Key rule: **more consumers than partitions = idle consumers**. Partitions are the unit of parallelism.

---

### Q7 — 5 Consumers, 3 Partitions — What Happens?

**Initial state: 5 consumers, 3 partitions**

Kafka assigns one consumer per partition maximum:
- Consumer 1 → Partition 0
- Consumer 2 → Partition 1
- Consumer 3 → Partition 2
- Consumer 4 → **idle** (no partition to read)
- Consumer 5 → **idle** (no partition to read)

Two consumers do nothing. This is wasted resource. The rule: you can never have more active consumers than partitions in a group.

**After adding 2 more partitions (now 5 partitions):**

A **rebalance** is triggered automatically. The group coordinator (a broker) pauses all consumers and reassigns partitions:

1. All 5 consumers stop reading — brief pause in consumption
2. Kafka reassigns:
   - Consumer 1 → Partition 0
   - Consumer 2 → Partition 1
   - Consumer 3 → Partition 2
   - Consumer 4 → Partition 3 *(now active)*
   - Consumer 5 → Partition 4 *(now active)*
3. All consumers resume reading from their assigned partition

Rebalancing is automatic but causes a short lag window. In production, it's triggered by: adding/removing consumers, adding partitions, consumer crashes, or timeouts.

---

### Q10 — Spark Transformation vs Action, and Lazy Evaluation

**Transformation**
A transformation creates a new RDD/DataFrame from an existing one. Transformations are **lazy** — they don't execute immediately, they just build a plan.

Examples:
- `.filter(col("amount") > 1000)` — keeps only rows above 1000
- `.groupBy("customer_id").agg(sum("amount"))` — groups and aggregates

**Action**
An action triggers the actual computation and returns a result (to the driver or to storage). Without an action, nothing runs.

Examples:
- `.count()` — counts rows and returns a number
- `.write.parquet("output/")` — writes data to disk

**Why lazy evaluation?**

Spark builds a **DAG (directed acyclic graph)** of all transformations before running anything. When an action is called, Spark optimizes the entire plan at once:

- It can combine multiple filters into one pass over the data (predicate pushdown)
- It can skip reading columns that aren't needed (column pruning)
- It avoids unnecessary shuffles

Think of it like planning your whole road trip before driving — you don't drive to every gas station individually, you plan the optimal route first.

---

### Q13 — Spark Streaming (DStreams) vs Structured Streaming

**DStreams (Discretized Streams) — the old way**
- Introduced in Spark 1.x
- Treats a stream as a sequence of small RDD batches (micro-batches)
- Low-level API — you work with RDDs directly
- No built-in support for event-time, watermarking, or exactly-once semantics
- Being phased out — not recommended for new projects

**Structured Streaming — the modern way**
- Introduced in Spark 2.x, stable in Spark 2.2+
- Treats a stream as an **unbounded DataFrame** — same API as batch Spark SQL
- Built-in support for event-time windows, watermarking (late data handling), and exactly-once semantics
- Optimized by the Catalyst query optimizer (same engine as Spark SQL)
- Supports multiple output modes: append, update, complete

**Key difference in one sentence:**
DStreams = RDD-based micro-batches with manual everything. Structured Streaming = SQL-style API with automatic optimization, late data handling, and fault tolerance built in.

---

### Q15 — Late Data Handling and Watermarking in Spark Structured Streaming

**The problem:**
In real-time streaming, events don't always arrive in order. A sensor might send a reading at 10:00 AM but due to network delays it arrives at Spark at 10:07 AM. If your window closed at 10:05, you've missed it.

**Watermarking — the solution:**
A watermark tells Spark: *"accept events that arrive up to X minutes late, then discard anything older."*

```python
df.withWatermark("timestamp", "10 minutes")
  .groupBy(window("timestamp", "5 minutes"))
  .count()
```

**What this means:**
- Spark tracks the maximum event timestamp it has seen
- It subtracts the watermark delay (10 min) to get the watermark line
- Events older than the watermark line are dropped
- Windows are only finalized (and written to output) once the watermark passes their end time

**Example:**
- Latest event seen: 10:15
- Watermark = 10:15 - 10 min = 10:05
- Window 10:00–10:05 is now finalized — no more late data accepted for it
- An event with timestamp 10:03 arriving now → dropped

**Trade-off:**
Larger watermark = more late data accepted = more state held in memory = higher latency.
Smaller watermark = less memory = results faster = more data dropped.

---

### Q19 — Late-Data Handling: IoT → Kafka → Spark → Snowflake → Power BI

**The problem:**
IoT sensors send readings continuously, but network delays mean events can arrive up to 10 minutes after their real timestamp. If Spark already aggregated and closed a 5-minute window, where does the late reading go? And if it changes the result, how does Power BI show both the corrected number AND the fact that a correction happened?

---

**Full architecture:**

```
IoT Sensors
    │  (MQTT / HTTP push)
    ▼
Kafka topic: "sensor-readings"
    │  key=sensor_id, value=JSON {sensor_id, temperature, ts}
    ▼
Spark Structured Streaming
    │  withWatermark("ts", "10 minutes")
    │  groupBy(window("ts", "5 minutes"), sensor_id)
    │  agg(avg(temperature), count())
    │  outputMode("update")
    │  writeStream.foreachBatch(upsert_to_snowflake)
    ▼
Snowflake
    ├── SENSOR_AGGREGATES       ← latest result per window per sensor
    └── SENSOR_CORRECTIONS_HISTORY  ← every version of every window
    ▼
Power BI (DirectQuery to Snowflake)
    ├── Live tile  → SENSOR_AGGREGATES
    └── History chart → SENSOR_CORRECTIONS_HISTORY
```

---

**Step 1 — Kafka layer**

Each IoT event is published to the `sensor-readings` topic with `sensor_id` as the message key.
Using the sensor ID as a key ensures all readings from the same sensor land on the same partition — important for ordered processing and efficient state management.

---

**Step 2 — Spark watermark and output mode**

```python
aggregates = (
    stream_df
    .withWatermark("ts", "10 minutes")          # accept late events up to 10 min
    .groupBy(
        window(col("ts"), "5 minutes"),          # 5-min tumbling windows
        col("sensor_id")
    )
    .agg(avg("temperature").alias("avg_temp"),
         count("*").alias("reading_count"))
)

query = aggregates.writeStream \
    .outputMode("update") \                      # emit a window every time it changes
    .foreachBatch(upsert_to_snowflake) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "output/q19_checkpoint") \
    .start()
```

**Why `update` mode (not `append`)?**

- `append` mode: a window is emitted only ONCE, after the watermark passes its end time. This means results are delayed by up to 10 minutes. No corrections are ever needed — but you wait longer.
- `update` mode: a window is emitted every time it changes — including when late data arrives and updates a closed window. Results appear faster, but the same window can be emitted multiple times.

For this question the teacher explicitly asks for a corrections history — so we use `update` mode and handle the multiple emissions in Snowflake.

---

**Step 3 — Snowflake schema**

Two tables work together:

```sql
-- Latest aggregate per window (what Power BI shows as "current")
CREATE TABLE SENSOR_AGGREGATES (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    sensor_id     VARCHAR,
    avg_temp      FLOAT,
    reading_count INTEGER,
    last_updated  TIMESTAMP,
    version       INTEGER,   -- increments each time late data changes this window
    PRIMARY KEY (window_start, sensor_id)
);

-- Full history of every version of every window
CREATE TABLE SENSOR_CORRECTIONS_HISTORY (
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    sensor_id       VARCHAR,
    avg_temp        FLOAT,
    reading_count   INTEGER,
    recorded_at     TIMESTAMP,  -- when this version was written
    version         INTEGER,    -- 1 = original, 2+ = corrections
    is_correction   BOOLEAN     -- FALSE for first write, TRUE for late-data updates
);
```

---

**Step 4 — The `foreachBatch` upsert function**

This is where late data is handled correctly:

```python
def upsert_to_snowflake(batch_df, batch_id):
    pdf = batch_df.toPandas()
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()
    now  = datetime.utcnow()

    for _, row in pdf.iterrows():
        w_start    = row["window"]["start"]
        w_end      = row["window"]["end"]
        sensor_id  = row["sensor_id"]
        avg_temp   = float(row["avg_temp"])
        count      = int(row["reading_count"])

        # check if this window already exists (i.e., this is a correction)
        cur.execute("""
            SELECT version FROM SENSOR_AGGREGATES
            WHERE window_start = %s AND sensor_id = %s
        """, (w_start, sensor_id))
        existing = cur.fetchone()

        if existing:
            new_version  = existing[0] + 1
            is_correction = True
        else:
            new_version  = 1
            is_correction = False

        # upsert into SENSOR_AGGREGATES (latest values only)
        cur.execute("""
            MERGE INTO SENSOR_AGGREGATES AS t
            USING (SELECT %s ws, %s we, %s sid, %s at, %s rc, %s lu, %s v) AS s
              ON (t.window_start = s.ws AND t.sensor_id = s.sid)
            WHEN MATCHED THEN
              UPDATE SET avg_temp=%s, reading_count=%s, last_updated=%s, version=%s
            WHEN NOT MATCHED THEN
              INSERT (window_start, window_end, sensor_id, avg_temp, reading_count,
                      last_updated, version)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (w_start, w_end, sensor_id, avg_temp, count, now, new_version,
              avg_temp, count, now, new_version,
              w_start, w_end, sensor_id, avg_temp, count, now, new_version))

        # always append to history — original write AND every correction
        cur.execute("""
            INSERT INTO SENSOR_CORRECTIONS_HISTORY
              (window_start, window_end, sensor_id, avg_temp, reading_count,
               recorded_at, version, is_correction)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (w_start, w_end, sensor_id, avg_temp, count,
              now, new_version, is_correction))

    conn.close()
```

---

**Step 5 — Power BI**

Connect Power BI to Snowflake using the Snowflake connector (Get Data → Snowflake). Use **DirectQuery** so the dashboard always reflects the latest data without needing manual refreshes.

Two visuals on the dashboard:

| Visual | Source table | What it shows |
|--------|-------------|---------------|
| Line / bar chart | `SENSOR_AGGREGATES` | Current avg temperature per sensor per 5-min window |
| Table / audit view | `SENSOR_CORRECTIONS_HISTORY WHERE is_correction = TRUE` | Every window that was updated by late data, with old vs new values |

For the corrections view, add a calculated column in Power BI:
```
Correction Label = "v" & [version] & " — " & FORMAT([recorded_at], "HH:MM:SS")
```
This lets users see exactly when a past window was revised and by how much.

---

**Key implementation decisions — summary**

| Decision | Choice | Reason |
|----------|--------|--------|
| Output mode | `update` | We need corrections to be emitted, not just final values |
| Watermark | 10 minutes | Matches the stated maximum late-arrival delay |
| Snowflake write pattern | MERGE (upsert) | Idempotent — safe to re-run if Spark restarts |
| History tracking | Separate `CORRECTIONS_HISTORY` table | Keeps `SENSOR_AGGREGATES` clean (one row per window) while preserving full audit trail |
| Power BI refresh | DirectQuery | Real-time view — no scheduled refresh needed |
| Kafka key | `sensor_id` | Keeps all readings for one sensor on one partition → correct ordering |
