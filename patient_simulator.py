import time, json, random
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
patients = [f"patient_{i}" for i in range(10)]
while True:
    for p in patients:
        vitals = {"patient_id": p,
                  "heart_rate": random.randint(60,120),
                  "bp": f"{random.randint(100,140)}/{random.randint(70,90)}",
                  "timestamp": time.time()}
        producer.send("patient-vitals", vitals)
    time.sleep(1)
