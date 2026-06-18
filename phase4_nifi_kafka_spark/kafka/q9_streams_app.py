from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json

INPUT_TOPIC = 'transactions'
OUTPUT_TOPIC = 'high-value-customers'
FILTER_THRESHOLD = 10_000

running_totals = {}

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers='localhost:9092',
    group_id='streams-app',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    retries=3,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
)

print(f'Kafka Streams app started.')
print(f'  Reading from : {INPUT_TOPIC}')
print(f'  Writing to   : {OUTPUT_TOPIC}')
print(f'  Filter       : amount > ${FILTER_THRESHOLD:,}')
print(f'  Waiting for messages... (Ctrl+C to stop)\n')

processed = 0
filtered = 0

try:
    for message in consumer:
        record = message.value
        processed += 1

        customer_id = record.get('customer_id', 'UNKNOWN')
        amount = float(record.get('amount', 0))
        transaction_id = record.get('transaction_id', '?')

        if amount <= FILTER_THRESHOLD:
            print(f'  SKIP  {transaction_id} | {customer_id} | ${amount:,.2f} (below threshold)')
            continue

        previous_total = running_totals.get(customer_id, 0.0)
        new_total = previous_total + amount
        running_totals[customer_id] = new_total
        filtered += 1

        print(f'  PASS  {transaction_id} | {customer_id} | ${amount:,.2f} → running total: ${new_total:,.2f}')

        output = {
            'customer_id': customer_id,
            'running_total': round(new_total, 2),
            'latest_transaction_id': transaction_id,
            'latest_amount': amount,
            'transactions_counted': sum(1 for k in running_totals if k == customer_id),
        }

        try:
            producer.send(OUTPUT_TOPIC, key=customer_id, value=output)
            producer.flush()
        except KafkaError as e:
            print(f'  [ERROR] Failed to write result for {customer_id}: {e}')

except KeyboardInterrupt:
    print(f'\nStopped.')
    print(f'  Total processed : {processed}')
    print(f'  Passed filter   : {filtered}')
    print(f'  Final KTable    :')
    for cid, total in sorted(running_totals.items()):
        print(f'    {cid} → ${total:,.2f}')

finally:
    producer.flush()
    producer.close()
    consumer.close()
