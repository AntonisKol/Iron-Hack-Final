# Q8: CSV-to-Kafka Producer with Guaranteed Delivery
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import csv
import os

TOPIC = 'csv-transactions'

CSV_FILE = os.path.join(os.path.dirname(__file__), 'transactions.csv')

# ── DELIVERY CALLBACKS ────────────────────────────────────────────────────────
# Called by the background I/O thread when the broker confirms or rejects a message.
# This is the difference between fire-and-forget (Q5) and guaranteed delivery (Q8).
def on_success(record_metadata, row_id):
    # partition: which Kafka partition stored this message
    # offset: its position within that partition — a unique, auditable address
    print(f'  [OK] {row_id} → partition {record_metadata.partition}, offset {record_metadata.offset}')


def on_error(exc, row_id):
    print(f'  [FAIL] {row_id} → {exc}')


# ── PRODUCER CONFIG ───────────────────────────────────────────────────────────
# acks='all': broker waits for all in-sync replicas to confirm — strongest delivery guarantee.
# retries=5 + retry_backoff_ms=200: retry up to 5 times with increasing delay before on_error fires.
# key_serializer: the transaction_id is used as the Kafka message key (bytes).
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=5,
    retry_backoff_ms=200,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
)

sent = 0
failed = 0

print(f'Reading: {CSV_FILE}')
print(f'Topic:   {TOPIC}\n')

# ── CSV READ + SEND LOOP ──────────────────────────────────────────────────────
# DictReader turns each CSV row into a dict keyed by the header row column names.
# add_callback / add_errback: register delivery functions on the Future returned by send().
try:
    with open(CSV_FILE, newline='') as f:
        reader = csv.DictReader(f)

        for row in reader:
            row_id = row['transaction_id']

            try:
                future = producer.send(
                    TOPIC,
                    key=row_id,
                    value=row,
                )

                future.add_callback(on_success, row_id)
                future.add_errback(on_error, row_id)

                sent += 1

            except KafkaError as e:
                print(f'  [ERROR] Could not queue {row_id}: {e}')
                failed += 1

except FileNotFoundError:
    print(f'CSV file not found: {CSV_FILE}')
    exit(1)

# ── FLUSH & CLOSE ─────────────────────────────────────────────────────────────
# flush() blocks until all pending callbacks have fired — nothing is lost on exit.
print('Flushing — waiting for all broker acks...')
producer.flush()

producer.close()

print(f'\nDone.  Sent: {sent}  |  Failed: {failed}')
