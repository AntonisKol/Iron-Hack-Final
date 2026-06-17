"""
Q8 — Kafka Producer: CSV to JSON messages with at-least-once delivery.

WHAT THIS SCRIPT DOES:
  Reads every row from transactions.csv and sends it as a JSON message
  to the 'csv-transactions' Kafka topic.

AT-LEAST-ONCE DELIVERY:
  A message is "at-least-once" when the producer guarantees it will never
  be lost, but may be delivered more than once (if the broker crashes after
  receiving the message but before sending the ack back, the producer retries
  and the broker stores it again). The consumer must handle duplicates.

  This is achieved with two settings:
    acks='all'  → broker waits for ALL in-sync replicas to confirm the write
                  before sending an ack. Even if the leader broker dies, the
                  message survives on the followers.
    retries=5   → on any transient failure (network blip, leader election),
                  retry up to 5 times before giving up.

ASYNC VS SYNC SENDING:
  send() is non-blocking — it returns immediately and queues the message in
  an internal buffer. A background thread handles the actual network I/O and
  fires the callbacks when the broker responds. This allows the producer to
  send thousands of messages without waiting for each ack individually.
  flush() at the end blocks until all pending messages are fully delivered.

Run with:
    python3 phase4_nifi_kafka_spark/kafka/q8_csv_producer.py
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError   # base class for all Kafka-related exceptions
import json
import csv
import os

TOPIC = 'csv-transactions'

# __file__ is the path of this script — dirname gives its folder
# this makes the CSV path work regardless of where you run the script from
CSV_FILE = os.path.join(os.path.dirname(__file__), 'transactions.csv')


# ── DELIVERY CALLBACKS ────────────────────────────────────────────────────────
# These two functions are passed to the Future returned by send().
# They run in a background thread — not in the main loop — so they don't
# slow down message sending.

def on_success(record_metadata, row_id):
    """Called when the broker confirms the message was written."""
    # record_metadata.partition → which partition received this message (0, 1, or 2)
    # record_metadata.offset    → position within that partition (ever-increasing number)
    # The offset is how Kafka tracks "how far has each consumer read"
    print(f'  [OK] {row_id} → partition {record_metadata.partition}, offset {record_metadata.offset}')

def on_error(exc, row_id):
    """Called when all retries are exhausted and the message still failed."""
    # At this point the message is lost — log it so we can investigate
    print(f'  [FAIL] {row_id} → {exc}')


# ── PRODUCER SETUP ────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',   # Kafka broker address — the entry point to the cluster

    # acks='all': strongest delivery guarantee
    # acks=0 → fire and forget (fastest, no guarantee)
    # acks=1 → only the leader confirms (message lost if leader crashes before replicating)
    # acks='all' → all in-sync replicas confirm (message survives even if leader crashes)
    acks='all',

    # retry up to 5 times on transient failures (network blip, broker restart)
    # combined with acks='all' this gives at-least-once: message may arrive twice
    # but will never be lost
    retries=5,

    # wait 200ms between retries — gives the broker time to recover
    retry_backoff_ms=200,

    # value_serializer runs on every message before it's sent
    # converts Python dict → JSON string → UTF-8 bytes
    # Kafka only stores bytes — the format (JSON, Avro, Protobuf) is our choice
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),

    # key_serializer converts the string key to bytes
    # the key is hashed to decide which partition the message goes to
    # same key always → same partition → messages for same transaction stay ordered
    key_serializer=lambda k: k.encode('utf-8') if k else None,
)


# ── READ CSV AND SEND ─────────────────────────────────────────────────────────
sent = 0    # count of messages successfully queued
failed = 0  # count of messages that failed before even reaching the broker

print(f'Reading: {CSV_FILE}')
print(f'Topic:   {TOPIC}\n')

try:
    with open(CSV_FILE, newline='') as f:

        # DictReader reads the first row as column headers
        # each subsequent row becomes a dict: {'transaction_id': 'TXN001', 'amount': '250.00', ...}
        # this is what gets serialized to JSON and sent to Kafka
        reader = csv.DictReader(f)

        for row in reader:
            row_id = row['transaction_id']  # used as the Kafka message key

            try:
                # send() is non-blocking — queues the message in the internal buffer
                # the background I/O thread will batch and send it to the broker
                future = producer.send(
                    TOPIC,
                    key=row_id,   # routes this message to a consistent partition
                    value=row,    # full CSV row as dict → serialized to JSON bytes
                )

                # add_callback fires on_success when the broker sends an ack
                # add_errback fires on_error if all retries are exhausted
                # both run in the background thread, not blocking this loop
                future.add_callback(on_success, row_id)
                future.add_errback(on_error, row_id)

                sent += 1

            except KafkaError as e:
                # this catches errors that happen before the message reaches the broker
                # e.g. the producer's internal buffer is full, or it's already closed
                print(f'  [ERROR] Could not queue {row_id}: {e}')
                failed += 1

except FileNotFoundError:
    print(f'CSV file not found: {CSV_FILE}')
    exit(1)


# flush() blocks the main thread until all queued messages are delivered
# (or retries exhausted). Without this, the script could exit while the
# background thread still has unsent messages — data loss.
print('Flushing — waiting for all broker acks...')
producer.flush()

# close() shuts down the background I/O thread cleanly
producer.close()

print(f'\nDone.  Sent: {sent}  |  Failed: {failed}')
