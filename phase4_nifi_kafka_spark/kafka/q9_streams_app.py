from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json

INPUT_TOPIC = 'transactions'
OUTPUT_TOPIC = 'high-value-customers'
FILTER_THRESHOLD = 10_000  # only process transactions above this amount

# ── KTABLE: RUNNING TOTALS ────────────────────────────────────────────────────
# In Kafka Streams this is a KTable backed by a RocksDB state store.
# Here it's a Python dict: { customer_id → running_total }
# Key insight: this is what makes the operation STATEFUL — we remember
# the accumulated total across many messages, not just the current one.
running_totals = {}

# ── CONSUMER (KStream input) ──────────────────────────────────────────────────
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers='localhost:9092',
    group_id='streams-app',          # consumer group — Kafka tracks our read position
    auto_offset_reset='earliest',    # start from the beginning if no committed offset exists
    enable_auto_commit=True,         # commit offset after each poll (at-least-once)
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # JSON bytes → dict
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
)

# ── PRODUCER (output to high-value-customers) ─────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',       # at-least-once delivery on the output side too
    retries=3,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
)

print(f'Kafka Streams app started.')
print(f'  Reading from : {INPUT_TOPIC}')
print(f'  Writing to   : {OUTPUT_TOPIC}')
print(f'  Filter       : amount > ${FILTER_THRESHOLD:,}')
print(f'  Waiting for messages... (Ctrl+C to stop)\n')

processed = 0   # total messages read from input topic
filtered = 0    # messages that passed the $10,000 filter

try:
    # consumer.poll() loop — equivalent to Kafka Streams' internal processing loop
    # each iteration processes one message from the input topic
    for message in consumer:
        record = message.value   # already deserialized to dict by value_deserializer
        processed += 1

        # safely extract fields — use .get() with defaults to avoid KeyError
        customer_id = record.get('customer_id', 'UNKNOWN')
        amount = float(record.get('amount', 0))
        transaction_id = record.get('transaction_id', '?')

        # ── FILTER (stateless) ────────────────────────────────────────────────
        # Equivalent to KStream.filter((key, value) -> value.getAmount() > 10000)
        # Messages below the threshold are simply skipped — not forwarded
        if amount <= FILTER_THRESHOLD:
            print(f'  SKIP  {transaction_id} | {customer_id} | ${amount:,.2f} (below threshold)')
            continue

        # ── GROUP BY + AGGREGATE (stateful) ──────────────────────────────────
        # Equivalent to .groupBy(customer_id).aggregate(runningTotal + amount)
        # We accumulate the total for this customer across ALL messages seen so far
        previous_total = running_totals.get(customer_id, 0.0)
        new_total = previous_total + amount
        running_totals[customer_id] = new_total   # update the KTable
        filtered += 1

        print(f'  PASS  {transaction_id} | {customer_id} | ${amount:,.2f} → running total: ${new_total:,.2f}')

        # ── WRITE TO OUTPUT TOPIC ─────────────────────────────────────────────
        # Equivalent to KTable.toStream().to("high-value-customers")
        # The key is customer_id so all updates for the same customer go to
        # the same partition, preserving order of updates per customer
        output = {
            'customer_id': customer_id,
            'running_total': round(new_total, 2),
            'latest_transaction_id': transaction_id,
            'latest_amount': amount,
            'transactions_counted': sum(
                1 for k in running_totals if k == customer_id
            ),
        }

        try:
            producer.send(OUTPUT_TOPIC, key=customer_id, value=output)
            producer.flush()   # flush after each message so output is visible immediately
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
