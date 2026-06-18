# Step 3: Kafka Topic Setup
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BROKER = 'localhost:9092'

TOPICS = [
    NewTopic(name='social-events', num_partitions=3, replication_factor=1),
]

admin = KafkaAdminClient(bootstrap_servers=BROKER)

for topic in TOPICS:
    try:
        admin.create_topics([topic])
        print(f'Created topic: {topic.name}  (partitions={topic.num_partitions})')
    except TopicAlreadyExistsError:
        print(f'Already exists: {topic.name}')

admin.close()
print('\nDone. Verify with: kafka-topics.sh --list --bootstrap-server localhost:9092')
