from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers="localhost:9092")
producer.send("buzzline", b'{"test":"hello"}')
producer.flush()