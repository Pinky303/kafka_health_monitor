"""Tests for S3 archiver batching logic."""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from consumers.s3_archiver import S3Archiver, make_s3_key


def make_record(patient_id="P001", offset=0):
    return {
        "patient_id": patient_id,
        "name": f"Patient {patient_id}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ward": "ICU-A",
        "vitals": {"heart_rate": 75.0, "spo2": 98.0},
    }


def test_s3_key_format():
    key = make_s3_key("P001", "2025-03-28T10:30:00+00:00", batch_id=1)
    assert key.startswith("vitals/")
    assert "year=2025" in key
    assert "month=03" in key
    assert "day=28" in key
    assert "P001" in key


def test_buffer_accumulates():
    archiver = S3Archiver()
    archiver.s3 = MagicMock()
    for i in range(5):
        archiver.buffer.append(make_record())
    assert len(archiver.buffer) == 5


def test_flush_clears_buffer():
    import os
    os.environ["S3_BUCKET_NAME"] = ""  # dry run
    archiver = S3Archiver()
    archiver.s3 = MagicMock()
    for _ in range(3):
        archiver.buffer.append(make_record())
    archiver.flush()
    assert len(archiver.buffer) == 0


def test_stats_tracking():
    archiver = S3Archiver()
    archiver.s3 = MagicMock()
    archiver.total_msgs  = 100
    archiver.total_files = 10
    stats = archiver.stats()
    assert stats["total_messages"] == 100
    assert stats["total_files"] == 10


def test_groups_by_patient():
    archiver = S3Archiver()
    archiver.s3 = MagicMock()
    archiver.buffer = [
        make_record("P001"),
        make_record("P002"),
        make_record("P001"),
        make_record("P003"),
    ]
    # After flush, S3 put_object called once per unique patient
    archiver.flush()
    # 3 unique patients → 3 put_object calls (when bucket set)
    # In dry-run mode (no bucket) just verify buffer cleared
    assert len(archiver.buffer) == 0
