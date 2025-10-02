from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'buzzline',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='test_group',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Polling up to 10 messages from 'buzzline'...")
messages = consumer.poll(timeout_ms=5000, max_records=10)
for tp, records in messages.items():
    for record in records:
        print(record.value)

consumer.close()
