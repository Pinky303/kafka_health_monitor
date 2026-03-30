from kafka.admin import KafkaAdminClient, NewTopic
admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
topics = ["patient-vitals","patient-alerts","patient-analytics"]
admin.create_topics([NewTopic(name=t, num_partitions=3, replication_factor=1) for t in topics])
