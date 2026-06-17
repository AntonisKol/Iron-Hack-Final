from kafka import KafkaProducer
import json

TOPIC = 'transactions'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
)

# mix of transactions — some below $10,000 (should be skipped)
# some above (should be aggregated per customer)
transactions = [
    {'transaction_id': 'T001', 'customer_id': 'C100', 'amount': 500.00},     # SKIP
    {'transaction_id': 'T002', 'customer_id': 'C200', 'amount': 15000.00},   # PASS
    {'transaction_id': 'T003', 'customer_id': 'C100', 'amount': 12000.00},   # PASS
    {'transaction_id': 'T004', 'customer_id': 'C300', 'amount': 9999.99},    # SKIP
    {'transaction_id': 'T005', 'customer_id': 'C200', 'amount': 22000.00},   # PASS — C200 total = 37,000
    {'transaction_id': 'T006', 'customer_id': 'C100', 'amount': 300.00},     # SKIP
    {'transaction_id': 'T007', 'customer_id': 'C100', 'amount': 18000.00},   # PASS — C100 total = 30,000
    {'transaction_id': 'T008', 'customer_id': 'C300', 'amount': 50000.00},   # PASS
    {'transaction_id': 'T009', 'customer_id': 'C200', 'amount': 11000.00},   # PASS — C200 total = 48,000
    {'transaction_id': 'T010', 'customer_id': 'C300', 'amount': 7500.00},    # SKIP
]

for t in transactions:
    producer.send(TOPIC, key=t['transaction_id'], value=t)
    print(f"Sent {t['transaction_id']} | {t['customer_id']} | ${t['amount']:,.2f}")

producer.flush()
producer.close()
print('\nAll transactions sent.')
