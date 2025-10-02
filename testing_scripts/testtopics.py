from kafka.admin import KafkaAdminClient

admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
print(admin.list_topics())
