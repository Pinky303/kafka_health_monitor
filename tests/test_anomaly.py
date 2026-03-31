"""Tests for anomaly detection logic."""
import pytest
from consumers.anomaly_detector import check_all_vitals, classify_vital


def test_normal_vitals_no_alerts():
    vitals = {
        "heart_rate":   75.0,
        "bp_systolic":  120.0,
        "spo2":         98.0,
        "temperature":  36.8,
        "resp_rate":    16.0,
    }
    alerts = check_all_vitals(vitals)
    assert len(alerts) == 0


def test_critical_low_heart_rate():
    alerts = check_all_vitals({"heart_rate": 35.0})
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "CRITICAL"
    assert alerts[0]["direction"] == "LOW"


def test_critical_high_bp():
    alerts = check_all_vitals({"bp_systolic": 185.0})
    assert len(alerts) == 1
    assert alerts[0]["vital"] == "bp_systolic"
    assert alerts[0]["severity"] == "CRITICAL"


def test_critical_low_spo2():
    alerts = check_all_vitals({"spo2": 88.0})
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "CRITICAL"


def test_multiple_critical_vitals():
    vitals = {
        "heart_rate":  35.0,   # critical low
        "bp_systolic": 190.0,  # critical high
        "spo2":        87.0,   # critical low
    }
    alerts = check_all_vitals(vitals)
    critical = [a for a in alerts if a["severity"] == "CRITICAL"]
    assert len(critical) == 3


def test_warning_vitals():
    alerts = check_all_vitals({"heart_rate": 52.0})  # warning low range
    assert len(alerts) >= 1
    severities = {a["severity"] for a in alerts}
    assert "WARNING" in severities


def test_unknown_vital_ignored():
    alerts = check_all_vitals({"unknown_metric": 999.0})
    assert len(alerts) == 0
