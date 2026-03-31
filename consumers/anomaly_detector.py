"""
Anomaly Detector Consumer
=========================
Reads from patient-vitals, checks each reading against clinical thresholds.
On critical breach → publishes to patient-alerts topic.
Cooldown per patient prevents alert storms.

Run: python consumers/anomaly_detector.py
"""

import json
import time
import logging
import os
import boto3
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ANOMALY] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("AnomalyDetector")

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SOURCE_TOPIC     = "patient-vitals"
ALERT_TOPIC      = "patient-alerts"
GROUP_ID         = "anomaly-detector-group-v1"
ALERT_COOLDOWN   = int(os.getenv("ALERT_COOLDOWN_SECONDS", 60))

SQS_QUEUE_URL    = os.getenv("SQS_ALERT_QUEUE_URL")
AWS_REGION       = os.getenv("AWS_REGION", "ap-south-1")


# ── Clinical thresholds ───────────────────────────────────────────────────────

THRESHOLDS = {
    "heart_rate": {
        "unit": "bpm",
        "warning_low":  50,   "warning_high":  110,
        "critical_low": 40,   "critical_high": 130,
    },
    "bp_systolic": {
        "unit": "mmHg",
        "warning_low":  85,   "warning_high":  150,
        "critical_low": 80,   "critical_high": 180,
    },
    "spo2": {
        "unit": "%",
        "warning_low":  92,   "warning_high":  101,
        "critical_low": 90,   "critical_high": 101,
    },
    "temperature": {
        "unit": "°C",
        "warning_low":  35.5, "warning_high":  38.5,
        "critical_low": 35.0, "critical_high": 39.5,
    },
    "resp_rate": {
        "unit": "/min",
        "warning_low":  11,   "warning_high":  25,
        "critical_low": 10,   "critical_high": 30,
    },
}


def classify_vital(key: str, value: float) -> dict | None:
    """Returns alert dict if vital is out of range, else None."""
    if key not in THRESHOLDS:
        return None
    t = THRESHOLDS[key]
    label = key.replace("_", " ").title()

    if value < t["critical_low"]:
        return {
            "vital":     key,
            "label":     label,
            "value":     value,
            "unit":      t["unit"],
            "severity":  "CRITICAL",
            "direction": "LOW",
            "threshold": t["critical_low"],
            "message":   f"{label} critically low: {value} {t['unit']} (threshold: {t['critical_low']})",
        }
    elif value > t["critical_high"] and t["critical_high"] != 101:
        return {
            "vital":     key,
            "label":     label,
            "value":     value,
            "unit":      t["unit"],
            "severity":  "CRITICAL",
            "direction": "HIGH",
            "threshold": t["critical_high"],
            "message":   f"{label} critically high: {value} {t['unit']} (threshold: {t['critical_high']})",
        }
    elif value < t["warning_low"]:
        return {
            "vital":     key,
            "label":     label,
            "value":     value,
            "unit":      t["unit"],
            "severity":  "WARNING",
            "direction": "LOW",
            "threshold": t["warning_low"],
            "message":   f"{label} warning — low: {value} {t['unit']}",
        }
    elif value > t["warning_high"] and t["warning_high"] != 101:
        return {
            "vital":     key,
            "label":     label,
            "value":     value,
            "unit":      t["unit"],
            "severity":  "WARNING",
            "direction": "HIGH",
            "threshold": t["warning_high"],
            "message":   f"{label} warning — high: {value} {t['unit']}",
        }
    return None


def check_all_vitals(vitals: dict) -> list[dict]:
    return [a for k, v in vitals.items() if (a := classify_vital(k, v)) is not None]


# ── SQS publisher (triggers Lambda) ──────────────────────────────────────────

def push_to_sqs(sqs_client, alert_msg: dict):
    if not SQS_QUEUE_URL:
        return
    try:
        sqs_client.send_message(
            QueueUrl    = SQS_QUEUE_URL,
            MessageBody = json.dumps(alert_msg),
            MessageAttributes={
                "patient_id": {
                    "DataType":    "String",
                    "StringValue": alert_msg["patient_id"],
                },
                "severity": {
                    "DataType":    "String",
                    "StringValue": "CRITICAL",
                },
            },
        )
        logger.info(f"📤 Pushed to SQS → Lambda will notify doctor")
    except Exception as e:
        logger.error(f"SQS push failed: {e}")


# ── Main consumer loop ────────────────────────────────────────────────────────

def main():
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers  = KAFKA_BOOTSTRAP,
        group_id           = GROUP_ID,
        auto_offset_reset  = "latest",
        enable_auto_commit = True,
        value_deserializer = lambda m: json.loads(m.decode()),
        max_poll_records   = 50,
        session_timeout_ms = 30000,
    )
    alert_producer = KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP,
        value_serializer  = lambda v: json.dumps(v).encode(),
        key_serializer    = lambda k: k.encode(),
        acks              = "all",
    )
    sqs = boto3.client("sqs", region_name=AWS_REGION) if SQS_QUEUE_URL else None

    last_alerted: dict[str, float] = defaultdict(lambda: 0.0)
    processed = 0
    alerted   = 0

    logger.info(f"🔴 Anomaly Detector active — consuming {SOURCE_TOPIC}")
    if sqs:
        logger.info(f"📤 SQS push enabled → {SQS_QUEUE_URL}")

    for message in consumer:
        data       = message.value
        patient_id = data["patient_id"]
        vitals     = data["vitals"]
        processed += 1

        alerts = check_all_vitals(vitals)
        if not alerts:
            continue

        # Only critical alerts trigger SQS/Lambda
        critical = [a for a in alerts if a["severity"] == "CRITICAL"]
        if not critical:
            continue

        now = time.time()
        if now - last_alerted[patient_id] < ALERT_COOLDOWN:
            continue   # suppress duplicate alerts

        last_alerted[patient_id] = now
        alerted += 1

        alert_msg = {
            "alert_id":    f"ALERT-{patient_id}-{int(now)}",
            "patient_id":  patient_id,
            "name":        data.get("name"),
            "ward":        data.get("ward"),
            "risk_level":  data.get("risk_level"),
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "alerts":      critical,
            "all_alerts":  alerts,
            "alert_count": len(critical),
            "vitals":      vitals,
            "source_partition": message.partition,
            "source_offset":    message.offset,
        }

        # 1. Publish to Kafka patient-alerts topic
        alert_producer.send(ALERT_TOPIC, key=patient_id, value=alert_msg)

        # 2. Push to SQS → triggers Lambda → SMS + Email
        if sqs:
            push_to_sqs(sqs, alert_msg)

        logger.warning(
            f"🚨 ALERT #{alerted} [{patient_id}] {data.get('ward')} — "
            f"{len(critical)} critical: {' | '.join(a['message'] for a in critical)}"
        )

        if processed % 500 == 0:
            logger.info(f"📊 Stats — processed: {processed} | alerts fired: {alerted}")


if __name__ == "__main__":
    main()
