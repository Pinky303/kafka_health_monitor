"""
Patient Vitals Producer
=======================
Simulates NUM_PATIENTS ICU patients each streaming vitals every second.
Each patient runs in its own thread. Messages are keyed by patient_id
so Kafka guarantees ordering per partition per patient.

Run: python producer/patient_simulator.py
"""

import json
import time
import random
import threading
import logging
import os
from datetime import datetime, timezone
from dataclasses import dataclass, field
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("VitalsProducer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = "patient-vitals"
NUM_PATIENTS    = int(os.getenv("NUM_PATIENTS", 10))
INTERVAL        = float(os.getenv("VITALS_INTERVAL_SECONDS", 1.0))


# ── Patient profile ───────────────────────────────────────────────────────────

@dataclass
class PatientProfile:
    patient_id:   str
    name:         str
    age:          int
    ward:         str
    risk_level:   str
    base_hr:      float   # baseline heart rate (bpm)
    base_bp_sys:  float   # baseline systolic BP (mmHg)
    base_spo2:    float   # baseline SpO2 (%)
    base_temp:    float   # baseline temperature (°C)
    base_rr:      float   # baseline respiratory rate (/min)
    msg_count:    int = field(default=0, repr=False)


WARDS      = ["ICU-A", "ICU-B", "CARDIAC", "NEURO", "GENERAL"]
RISK_DIST  = ["LOW"] * 2 + ["MEDIUM"] * 2 + ["HIGH"]
NAMES      = [
    "Arjun Sharma", "Priya Patel", "Rahul Mehta", "Sunita Singh",
    "Vikram Reddy", "Kavya Nair", "Amit Joshi", "Pooja Gupta",
    "Suresh Kumar", "Anita Verma", "Rajesh Iyer", "Neha Agarwal",
]


def generate_patients(n: int) -> list[PatientProfile]:
    return [
        PatientProfile(
            patient_id  = f"P{i+1:03d}",
            name        = NAMES[i % len(NAMES)],
            age         = random.randint(28, 82),
            ward        = random.choice(WARDS),
            risk_level  = random.choice(RISK_DIST),
            base_hr     = random.uniform(62, 92),
            base_bp_sys = random.uniform(100, 132),
            base_spo2   = random.uniform(95.5, 99.5),
            base_temp   = random.uniform(36.2, 37.2),
            base_rr     = random.uniform(13, 19),
        )
        for i in range(n)
    ]


# ── Vital simulation ──────────────────────────────────────────────────────────

def simulate_vital(
    base: float,
    noise_std: float,
    spike_chance: float = 0.015,
    spike_scale: float = 5.0,
) -> float:
    """
    Adds Gaussian noise to a baseline value.
    Occasionally applies a sudden spike to simulate patient deterioration.
    """
    value = base + random.gauss(0, noise_std)
    if random.random() < spike_chance:
        direction = random.choice([-1, 1])
        value += direction * random.uniform(noise_std * spike_scale, noise_std * spike_scale * 2)
    return round(value, 2)


def build_vitals_message(patient: PatientProfile) -> dict:
    patient.msg_count += 1
    return {
        "patient_id":  patient.patient_id,
        "name":        patient.name,
        "age":         patient.age,
        "ward":        patient.ward,
        "risk_level":  patient.risk_level,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "sequence":    patient.msg_count,
        "vitals": {
            "heart_rate":   simulate_vital(patient.base_hr,     noise_std=4.0),
            "bp_systolic":  simulate_vital(patient.base_bp_sys, noise_std=8.0),
            "bp_diastolic": simulate_vital(patient.base_bp_sys * 0.65, noise_std=5.0),
            "spo2":         min(100.0, simulate_vital(patient.base_spo2, noise_std=0.8)),
            "temperature":  simulate_vital(patient.base_temp,   noise_std=0.15),
            "resp_rate":    simulate_vital(patient.base_rr,     noise_std=1.5),
        },
        "device_id": f"MONITOR-{patient.ward}-{patient.patient_id}",
        "schema_version": "1.0",
    }


# ── Producer thread ───────────────────────────────────────────────────────────

class PatientThread(threading.Thread):
    def __init__(
        self,
        patient: PatientProfile,
        producer: KafkaProducer,
        interval: float = 1.0,
    ):
        super().__init__(daemon=True, name=f"Thread-{patient.patient_id}")
        self.patient  = patient
        self.producer = producer
        self.interval = interval
        self._stop    = threading.Event()

    def run(self):
        logger.info(
            f"🟢 [{self.patient.patient_id}] Started — "
            f"{self.patient.name} | {self.patient.ward} | Risk: {self.patient.risk_level}"
        )
        while not self._stop.is_set():
            try:
                msg = build_vitals_message(self.patient)
                future = self.producer.send(
                    KAFKA_TOPIC,
                    key   = self.patient.patient_id.encode(),
                    value = json.dumps(msg).encode(),
                )
                future.add_errback(lambda e: logger.error(f"Send failed: {e}"))
            except Exception as e:
                logger.error(f"[{self.patient.patient_id}] Error: {e}")
            self._stop.wait(self.interval)

    def stop(self):
        self._stop.set()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP,
        acks              = "all",
        retries           = 5,
        linger_ms         = 5,
        batch_size        = 16384,
        compression_type  = "snappy",
    )

    patients = generate_patients(NUM_PATIENTS)
    threads  = [PatientThread(p, producer, INTERVAL) for p in patients]

    logger.info(f"\n{'='*50}")
    logger.info(f"  🏥 Health Monitor Producer Starting")
    logger.info(f"  Patients : {NUM_PATIENTS}")
    logger.info(f"  Interval : {INTERVAL}s per patient")
    logger.info(f"  Topic    : {KAFKA_TOPIC}")
    logger.info(f"  Brokers  : {KAFKA_BOOTSTRAP}")
    logger.info(f"{'='*50}\n")

    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(10)
            total = sum(p.msg_count for p in patients)
            logger.info(f"📊 Total messages sent: {total}")
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping all patient simulators...")
        for t in threads:
            t.stop()
        for t in threads:
            t.join(timeout=3)
        producer.flush(timeout=10)
        producer.close()
        logger.info("✅ Producer shut down cleanly")


if __name__ == "__main__":
    main()
