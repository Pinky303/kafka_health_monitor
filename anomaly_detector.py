from kafka import KafkaConsumer, KafkaProducer
import json
consumer = KafkaConsumer("patient-vitals", bootstrap_servers="localhost:9092",
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for msg in consumer:
    vitals = msg.value
    if vitals["heart_rate"] > 110:
        alert = {"patient_id": vitals["patient_id"], "alert":"High heart rate"}
        producer.send("patient-alerts", alert)
