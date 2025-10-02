from kafka import KafkaConsumer
try:
    consumer = KafkaConsumer(
        bootstrap_servers="127.0.0.1:9092",
        auto_offset_reset='earliest'
    )
    print("Connected to Kafka broker!")
except Exception as e:
    print("Cannot connect:", e)