# Q8: CSV-to-Kafka Producer with Guaranteed Delivery
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import csv
import os

TOPIC = 'csv-transactions'

CSV_FILE = os.path.join(os.path.dirname(__file__), 'transactions.csv')

def on_success(record_metadata, row_id):
    # partition: which Kafka partition stored this message
    print(f'  [OK] {row_id} → partition {record_metadata.partition}, offset {record_metadata.offset}')


def on_error(exc, row_id):
    print(f'  [FAIL] {row_id} → {exc}')


# PRODUCER CONFIG 
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all', # acks='all': broker waits for all in-sync replicas to confirm — strongest delivery guarantee.

    retries=5,
    retry_backoff_ms=200,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None, # key_serializer: the transaction_id is used as the Kafka message key (bytes).

)

sent = 0
failed = 0

print(f'Reading: {CSV_FILE}')
print(f'Topic:   {TOPIC}\n')


try:
    with open(CSV_FILE, newline='') as f:
        reader = csv.DictReader(f) # DictReader turns each CSV row into a dict keyed by the header row column names.
        for row in reader:
            row_id = row['transaction_id']
            try:
                future = producer.send(
                    TOPIC,
                    key=row_id,
                    value=row,
                )
                future.add_callback(on_success, row_id) # add_callback / add_errback: register delivery functions on the Future returned by send().
                future.add_errback(on_error, row_id) # Future callbacks are executed by the producer's background I/O thread when the broker responds.
                sent += 1
            except KafkaError as e:
                print(f'  [ERROR] Could not queue {row_id}: {e}')
                failed += 1
except FileNotFoundError:
    print(f'CSV file not found: {CSV_FILE}')
    exit(1)


# flush() blocks until all pending callbacks have fired and all messages have been sent to the broker (or failed after retries).
print('Flushing — waiting for all broker acks...')
producer.flush()
producer.close()

print(f'\nDone.  Sent: {sent}  |  Failed: {failed}')
