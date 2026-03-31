"""Tests for patient vitals producer."""
import pytest
from producer.patient_simulator import (
    generate_patients,
    build_vitals_message,
    simulate_vital,
    PatientProfile,
)


def test_generate_patients():
    patients = generate_patients(5)
    assert len(patients) == 5
    for p in patients:
        assert p.patient_id.startswith("P")
        assert 28 <= p.age <= 82
        assert p.risk_level in ("LOW", "MEDIUM", "HIGH")


def test_build_vitals_message_schema():
    patients = generate_patients(1)
    msg = build_vitals_message(patients[0])
    required_keys = {"patient_id", "name", "ward", "timestamp", "vitals", "device_id"}
    assert required_keys.issubset(msg.keys())
    vital_keys = {"heart_rate", "bp_systolic", "bp_diastolic", "spo2", "temperature", "resp_rate"}
    assert vital_keys.issubset(msg["vitals"].keys())


def test_spo2_never_exceeds_100():
    patients = generate_patients(1)
    for _ in range(1000):
        msg = build_vitals_message(patients[0])
        assert msg["vitals"]["spo2"] <= 100.0


def test_simulate_vital_range():
    base, noise = 75.0, 5.0
    values = [simulate_vital(base, noise) for _ in range(10000)]
    assert all(isinstance(v, float) for v in values)
    # Most values should be within 5 std devs of base (with spike allowance)
    within_range = sum(1 for v in values if base - noise * 20 < v < base + noise * 20)
    assert within_range > 9900


def test_message_sequence_increments():
    patients = generate_patients(1)
    p = patients[0]
    m1 = build_vitals_message(p)
    m2 = build_vitals_message(p)
    assert m2["sequence"] == m1["sequence"] + 1
