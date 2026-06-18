from kafka import KafkaConsumer
import json

TOPIC  = 'social-events'
BROKER = 'localhost:9092'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id='validator',
    auto_offset_reset='earliest',      # read from the beginning on first run
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)

print(f'Consuming from topic: {TOPIC}')
print(f'Press Ctrl+C to stop.\n')
print(f'{"#":>6}  {"EventType":<16}  {"User":<8}  {"Post/Target":<12}  Timestamp')
print('-' * 70)

counts  = {}
total   = 0

try:
    for msg in consumer:
        event      = msg.value
        etype      = event.get('event_type', 'UNKNOWN')
        counts[etype] = counts.get(etype, 0) + 1
        total += 1

        if total % 50 == 0:
            print(f'  [{total:>5}]  {etype:<16}  {event.get("user_id","—"):<8}  '
                  f'{event.get("post_id", event.get("target_user_id", "—")):<12}  '
                  f'{event.get("timestamp","—")}')

except KeyboardInterrupt:
    print(f'\n=== Validation Summary ===')
    print(f'Total events consumed: {total}')
    print(f'\nBreakdown by type:')
    for etype, cnt in sorted(counts.items(), key=lambda x: -x[1]):
        pct = cnt / total * 100 if total > 0 else 0
        print(f'  {etype:<16} {cnt:>6}  ({pct:.1f}%)')

finally:
    consumer.close()
