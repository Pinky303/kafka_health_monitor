from kafka import KafkaConsumer
import json
consumer = KafkaConsumer("patient-analytics", bootstrap_servers="localhost:9092",
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for msg in consumer:
    print("Analytics:", msg.value)
