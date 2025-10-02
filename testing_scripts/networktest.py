from kafka import KafkaConsumer

try:
    consumer = KafkaConsumer(
        bootstrap_servers="127.0.0.1:9092",  # or WSL IP
        consumer_timeout_ms=5000
    )
    print("Connected!")
except Exception as e:
    print("Cannot connect:", e)