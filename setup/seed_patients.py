"""
Optional: Seeds Kafka with historical patient data for testing consumers.
Sends 500 messages across all patients instantly.
"""
import json
import random
import logging
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Seeder")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def seed(num_patients=10, messages_per_patient=50):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode(),
    )

    now = datetime.now(timezone.utc)
    total = 0

    for p in range(num_patients):
        pid = f"P{p+1:03d}"
        for i in range(messages_per_patient):
            ts = now - timedelta(minutes=messages_per_patient - i)
            msg = {
                "patient_id": pid,
                "name": f"Patient {p+1}",
                "age": random.randint(30, 80),
                "ward": random.choice(["ICU-A", "ICU-B", "CARDIAC"]),
                "risk_level": "MEDIUM",
                "timestamp": ts.isoformat(),
                "vitals": {
                    "heart_rate": round(random.gauss(75, 8), 2),
                    "bp_systolic": round(random.gauss(120, 10), 2),
                    "bp_diastolic": round(random.gauss(80, 6), 2),
                    "spo2": round(min(100, random.gauss(97, 1)), 2),
                    "temperature": round(random.gauss(36.8, 0.3), 2),
                    "resp_rate": round(random.gauss(16, 2), 2),
                },
                "device_id": f"MONITOR-{pid}",
            }
            producer.send("patient-vitals", key=pid, value=msg)
            total += 1

    producer.flush()
    producer.close()
    logger.info(f"✅ Seeded {total} historical messages into patient-vitals")


if __name__ == "__main__":
    seed()
