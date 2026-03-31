"""
S3 Archiver Consumer
====================
Reads from patient-vitals and batches records into S3.
Flush trigger: every BATCH_SIZE messages OR every FLUSH_INTERVAL seconds.
S3 path: vitals/year=YYYY/month=MM/day=DD/patient_PXXX_timestamp.json

Run: python consumers/s3_archiver.py
"""

import json
import time
import logging
import os
import boto3
from datetime import datetime, timezone
from collections import defaultdict
from io import BytesIO
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [S3-ARCHIVER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("S3Archiver")

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SOURCE_TOPIC     = "patient-vitals"
GROUP_ID         = "s3-archiver-group-v1"
S3_BUCKET        = os.getenv("S3_BUCKET_NAME")
AWS_REGION       = os.getenv("AWS_REGION", "ap-south-1")
BATCH_SIZE       = int(os.getenv("S3_BATCH_SIZE", 100))
FLUSH_INTERVAL   = int(os.getenv("S3_FLUSH_INTERVAL_SECONDS", 30))


# ── S3 key strategy ───────────────────────────────────────────────────────────

def make_s3_key(patient_id: str, timestamp: str, batch_id: int) -> str:
    """
    Hive-partitioned path for efficient Athena queries:
    vitals/year=2025/month=03/day=28/patient_P001_1234567890_batch42.json
    """
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return (
        f"vitals/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"patient_{patient_id}_{int(dt.timestamp())}_b{batch_id}.json"
    )


# ── Archiver class ────────────────────────────────────────────────────────────

class S3Archiver:
    def __init__(self):
        self.s3          = boto3.client("s3", region_name=AWS_REGION)
        self.buffer:     list[dict] = []
        self.last_flush  = time.time()
        self.total_msgs  = 0
        self.total_files = 0
        self._batch_id   = 0

    def add(self, record: dict):
        self.buffer.append(record)
        self.total_msgs += 1

        should_flush = (
            len(self.buffer) >= BATCH_SIZE
            or (time.time() - self.last_flush) >= FLUSH_INTERVAL
        )
        if should_flush:
            self.flush()

    def flush(self):
        if not self.buffer:
            return

        batch = self.buffer[:]
        self.buffer.clear()
        self.last_flush = time.time()
        self._batch_id  += 1

        # Group by patient for cleaner S3 layout + easier Athena partitioning
        by_patient: dict[str, list] = defaultdict(list)
        for r in batch:
            by_patient[r["patient_id"]].append(r)

        for patient_id, records in by_patient.items():
            key  = make_s3_key(patient_id, records[0]["timestamp"], self._batch_id)
            body = json.dumps(
                {
                    "patient_id":    patient_id,
                    "record_count":  len(records),
                    "batch_id":      self._batch_id,
                    "archived_at":   datetime.now(timezone.utc).isoformat(),
                    "schema":        "vitals-v1",
                    "records":       records,
                },
                indent=2,
            ).encode()

            if S3_BUCKET:
                self.s3.put_object(
                    Bucket      = S3_BUCKET,
                    Key         = key,
                    Body        = BytesIO(body),
                    ContentType = "application/json",
                    Metadata    = {
                        "patient_id":   patient_id,
                        "record_count": str(len(records)),
                        "source":       "kafka-patient-vitals",
                        "batch_id":     str(self._batch_id),
                    },
                )
                logger.info(
                    f"✅ s3://{S3_BUCKET}/{key}  "
                    f"({len(records)} records, {len(body):,} bytes)"
                )
            else:
                # Dry run — log what would be uploaded
                logger.info(f"[DRY RUN] Would upload → {key}  ({len(records)} records)")

            self.total_files += 1

        logger.info(
            f"📊 Batch #{self._batch_id} done — "
            f"total archived: {self.total_msgs} msgs / {self.total_files} files"
        )

    def stats(self) -> dict:
        return {
            "total_messages": self.total_msgs,
            "total_files":    self.total_files,
            "buffer_size":    len(self.buffer),
            "batch_id":       self._batch_id,
        }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers  = KAFKA_BOOTSTRAP,
        group_id           = GROUP_ID,
        auto_offset_reset  = "earliest",   # archive from beginning
        enable_auto_commit = True,
        value_deserializer = lambda m: json.loads(m.decode()),
        max_poll_records   = BATCH_SIZE,
        fetch_max_bytes    = 52428800,      # 50 MB
    )

    archiver = S3Archiver()

    if S3_BUCKET:
        logger.info(f"📦 S3 Archiver → s3://{S3_BUCKET}/vitals/")
    else:
        logger.warning("⚠️  S3_BUCKET_NAME not set — running in DRY RUN mode")

    logger.info(
        f"   Batch size:     {BATCH_SIZE} messages\n"
        f"   Flush interval: {FLUSH_INTERVAL}s\n"
        f"   Group ID:       {GROUP_ID}"
    )

    try:
        for message in consumer:
            archiver.add(message.value)
    except KeyboardInterrupt:
        archiver.flush()
        logger.info(f"🛑 Archiver stopped. Final stats: {archiver.stats()}")


if __name__ == "__main__":
    main()
