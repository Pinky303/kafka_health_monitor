"""
Creates all required Kafka topics for the health monitoring system.
Run once after docker-compose up.
"""
import logging
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("TopicSetup")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = [
    NewTopic(
        name="patient-vitals",
        num_partitions=4,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "compression.type": "snappy",
        },
    ),
    NewTopic(
        name="patient-alerts",
        num_partitions=2,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
        },
    ),
    NewTopic(
        name="patient-analytics",
        num_partitions=1,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(1 * 24 * 60 * 60 * 1000),  # 1 day
        },
    ),
]


def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id="setup-admin")
    try:
        admin.create_topics(new_topics=TOPICS, validate_only=False)
        logger.info("✅ All topics created successfully:")
        for t in TOPICS:
            logger.info(f"   → {t.name}  ({t.num_partitions} partitions)")
    except TopicAlreadyExistsError:
        logger.warning("⚠️  Topics already exist — skipping creation")
    except Exception as e:
        logger.error(f"❌ Topic creation error: {e}")
        raise
    finally:
        admin.close()


def list_topics():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    topics = admin.list_topics()
    logger.info(f"📋 Existing topics: {sorted(topics)}")
    admin.close()


if __name__ == "__main__":
    create_topics()
    list_topics()
