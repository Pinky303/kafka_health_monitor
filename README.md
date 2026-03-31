# 🏥 Real-Time Patient Health Monitoring System

> Apache Kafka + AWS — Production-grade ICU vitals streaming pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-black?logo=apachekafka)](https://kafka.apache.org)
[![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20SNS-orange?logo=amazonaws)](https://aws.amazon.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 📐 Architecture

```
[ 10 Patient Simulators ] — vitals every 1 second
           │
           ▼
  ┌─────────────────────────────────┐
  │     KAFKA BROKER (Docker)       │
  │  Topics:                        │
  │   • patient-vitals  (4 parts)  │
  │   • patient-alerts  (2 parts)  │
  │   • patient-analytics          │
  └─────────────────────────────────┘
           │
    ┌──────┼────────────┬─────────────┐
    ▼      ▼            ▼             ▼
[Anomaly] [S3 Archiver] [WS Dashboard] [Kafka UI]
Detector      │              │         :8080
    │       AWS S3       Browser UI
    │                    localhost:8000
    ▼
[AWS SQS] → [Lambda] → [SNS] → SMS + Email
```

---

## ⚡ Quickstart

```bash
# 1. Clone
git clone https://github.com/yourusername/health-monitor.git
cd health-monitor

# 2. Setup environment
cp .env.example .env
# Fill in your AWS credentials in .env

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start Kafka
docker-compose up -d

# 5. Wait ~15s then create topics
python setup/create_topics.py

# 6. Open 4 terminals and run:
python consumers/dashboard_server.py   # Terminal 1
python consumers/s3_archiver.py        # Terminal 2
python consumers/anomaly_detector.py   # Terminal 3
python producer/patient_simulator.py   # Terminal 4

# 7. Open browser → http://localhost:8000
```

---

## 📁 Project Structure

```
health-monitor/
├── producer/
│   └── patient_simulator.py     # Simulates 10 ICU patients
├── consumers/
│   ├── anomaly_detector.py      # Detects critical vitals → alerts
│   ├── s3_archiver.py           # Batches & uploads to AWS S3
│   └── dashboard_server.py      # FastAPI + WebSocket server
├── lambda/
│   └── alert_handler.py         # AWS Lambda: SMS + Email alerts
├── dashboard/
│   └── index.html               # Live browser dashboard
├── setup/
│   ├── create_topics.py         # Kafka topic initializer
│   └── seed_patients.py         # Optional: seed test data
├── infra/
│   └── terraform/               # AWS infra as code
│       ├── main.tf
│       ├── s3.tf
│       ├── lambda.tf
│       └── sns_sqs.tf
├── tests/
│   ├── test_producer.py
│   ├── test_anomaly.py
│   └── test_archiver.py
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🩺 Kafka Topics

| Topic | Partitions | Retention | Description |
|---|---|---|---|
| `patient-vitals` | 4 | 7 days | Raw vitals stream (key=patient_id) |
| `patient-alerts` | 2 | 30 days | Critical alert events |
| `patient-analytics` | 1 | 1 day | Aggregated windowed stats |

---

## 🚨 Alert Thresholds

| Vital | Normal | Critical Low | Critical High |
|---|---|---|---|
| Heart Rate | 60–100 bpm | < 40 | > 130 |
| Systolic BP | 90–120 mmHg | < 80 | > 180 |
| SpO2 | 95–100% | < 90% | — |
| Temperature | 36.1–37.2°C | < 35°C | > 39.5°C |
| Resp. Rate | 12–20 /min | < 10 | > 30 |

---

## ☁️ AWS Services Used

| Service | Purpose |
|---|---|
| **S3** | Archive all vitals as partitioned JSON |
| **Lambda** | Serverless alert handler |
| **SNS** | SMS + email notifications |
| **SQS** | Alert queue between detector & Lambda |
| **SES** | Rich HTML email to doctors |
| **CloudWatch** | Lambda logs + custom metrics |

---

## 🛠 Tech Stack

- **Python 3.11** — all services
- **Apache Kafka** (Confluent 7.5) — via Docker
- **FastAPI + WebSockets** — live dashboard server
- **boto3** — AWS SDK
- **Docker Compose** — local Kafka + Zookeeper + Kafka UI

---

## 🔮 What to Build Next

- [ ] Replace rule-based detection with ML model (Isolation Forest / LSTM)
- [ ] AWS Athena + QuickSight for historical analytics
- [ ] Multi-hospital Kafka topic isolation
- [ ] Mobile push notifications via Firebase FCM
- [ ] Kubernetes (EKS) deployment

---

## 📄 License

MIT — free to use, modify, and learn from.
