"""
AWS Lambda — Critical Alert Handler
=====================================
Triggered by SQS queue when Anomaly Detector publishes a critical alert.
Sends SMS via SNS and rich HTML email via SES to on-call doctors.

Deploy:
  zip alert_handler.zip alert_handler.py
  aws lambda create-function --function-name patient-alert-handler \
    --runtime python3.11 --handler alert_handler.lambda_handler \
    --zip-file fileb://alert_handler.zip --role arn:aws:iam::ACCOUNT:role/LambdaRole \
    --environment Variables='{...}'
"""

import json
import os
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clients (initialized outside handler for Lambda warm start performance)
sns = boto3.client("sns")
ses = boto3.client("ses")

# ── Config from Lambda env vars ───────────────────────────────────────────────
DOCTOR_PHONE   = os.environ["DOCTOR_PHONE"]           # +91XXXXXXXXXX
DOCTOR_EMAIL   = os.environ["DOCTOR_EMAIL"]           # doctor@hospital.com
SNS_TOPIC_ARN  = os.environ["SNS_ALERT_TOPIC_ARN"]
HOSPITAL_NAME  = os.environ.get("HOSPITAL_NAME", "City Hospital")
SENDER_EMAIL   = os.environ.get("SENDER_EMAIL", f"alerts@hospital.com")


# ── Message formatters ────────────────────────────────────────────────────────

def format_sms(alert: dict) -> str:
    ts     = datetime.fromisoformat(alert["timestamp"]).strftime("%H:%M:%S UTC")
    issues = " | ".join(a["message"] for a in alert["alerts"])
    return (
        f"🚨 CRITICAL — {HOSPITAL_NAME}\n"
        f"Patient: {alert['name']} | Ward: {alert['ward']}\n"
        f"Time: {ts}\n"
        f"{issues}\n"
        f"CHECK PATIENT IMMEDIATELY"
    )


def format_email_html(alert: dict) -> str:
    ts      = datetime.fromisoformat(alert["timestamp"]).strftime("%Y-%m-%d %H:%M:%S UTC")
    vitals  = alert.get("vitals", {})

    rows = "".join(
        f"""<tr>
          <td style="padding:10px 16px;color:#f87171;font-weight:600">{a['label']}</td>
          <td style="padding:10px 16px;color:#fbbf24;font-size:18px;font-weight:bold">{a['value']} {a['unit']}</td>
          <td style="padding:10px 16px;color:#f87171">{a['message']}</td>
        </tr>"""
        for a in alert["alerts"]
    )

    vital_rows = "".join(
        f"<tr><td style='padding:6px 12px;color:#6b7280'>{k.replace('_',' ').title()}</td>"
        f"<td style='padding:6px 12px;color:#d1d5db;font-family:monospace'>{v}</td></tr>"
        for k, v in vitals.items()
    )

    return f"""
    <html>
    <body style="font-family:-apple-system,sans-serif;background:#0a0a0f;color:#cdd9e5;margin:0;padding:24px">
      <div style="max-width:600px;margin:0 auto">

        <div style="background:#1a0a0a;border:2px solid #f87171;border-radius:12px;padding:24px;margin-bottom:20px">
          <h1 style="color:#f87171;margin:0 0 8px;font-size:24px">
            🚨 Critical Patient Alert
          </h1>
          <p style="color:#6b7280;margin:0;font-size:14px">{HOSPITAL_NAME} · {ts}</p>
        </div>

        <div style="background:#111820;border:1px solid #1e2d3d;border-radius:10px;padding:20px;margin-bottom:16px">
          <table style="width:100%">
            <tr>
              <td style="color:#6b7280;font-size:13px">Patient</td>
              <td style="color:white;font-weight:600;font-size:16px">{alert.get('name')}</td>
            </tr>
            <tr>
              <td style="color:#6b7280;font-size:13px">ID</td>
              <td style="color:#38bdf8;font-family:monospace">{alert.get('patient_id')}</td>
            </tr>
            <tr>
              <td style="color:#6b7280;font-size:13px">Ward</td>
              <td style="color:#e5e7eb">{alert.get('ward')}</td>
            </tr>
            <tr>
              <td style="color:#6b7280;font-size:13px">Risk Level</td>
              <td style="color:#fbbf24">{alert.get('risk_level')}</td>
            </tr>
          </table>
        </div>

        <h2 style="color:#f87171;font-size:16px;margin:0 0 10px">Critical Readings</h2>
        <table style="width:100%;border-collapse:collapse;background:#111820;border-radius:10px;overflow:hidden;margin-bottom:16px">
          <thead>
            <tr style="background:#1e2d3d">
              <th style="padding:10px 16px;text-align:left;color:#6b7280;font-size:12px;text-transform:uppercase">Vital</th>
              <th style="padding:10px 16px;text-align:left;color:#6b7280;font-size:12px;text-transform:uppercase">Value</th>
              <th style="padding:10px 16px;text-align:left;color:#6b7280;font-size:12px;text-transform:uppercase">Alert</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>

        <h2 style="color:#38bdf8;font-size:16px;margin:0 0 10px">Full Vitals Snapshot</h2>
        <table style="width:100%;border-collapse:collapse;background:#111820;border-radius:8px;margin-bottom:20px">
          <tbody>{vital_rows}</tbody>
        </table>

        <p style="color:#374151;font-size:12px;text-align:center;margin-top:24px">
          Auto-generated by {HOSPITAL_NAME} Health Monitor · Kafka + AWS
        </p>
      </div>
    </body>
    </html>
    """


def format_email_text(alert: dict) -> str:
    issues = "\n".join(f"  - {a['message']}" for a in alert["alerts"])
    return (
        f"CRITICAL ALERT — {HOSPITAL_NAME}\n"
        f"Patient: {alert.get('name')} | Ward: {alert.get('ward')}\n"
        f"Time: {alert.get('timestamp')}\n\n"
        f"Critical Issues:\n{issues}\n\n"
        f"Alert ID: {alert.get('alert_id')}"
    )


# ── Lambda handler ────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Processes SQS messages. Each record is a critical patient alert.
    Returns 200 on success; Lambda retries on unhandled exceptions.
    """
    results = {"processed": 0, "failed": 0}

    for record in event.get("Records", []):
        try:
            alert = json.loads(record["body"])
            pid   = alert.get("patient_id", "UNKNOWN")

            logger.info(f"Processing alert for {pid}: {alert.get('alert_id')}")

            # 1. SMS via SNS
            sns.publish(
                PhoneNumber = DOCTOR_PHONE,
                Message     = format_sms(alert),
                MessageAttributes={
                    "AWS.SNS.SMS.SMSType": {
                        "DataType":    "String",
                        "StringValue": "Transactional",
                    },
                    "AWS.SNS.SMS.SenderID": {
                        "DataType":    "String",
                        "StringValue": "HOSPITAL",
                    },
                },
            )
            logger.info(f"📱 SMS sent for {pid}")

            # 2. Email via SES
            ses.send_email(
                Source      = SENDER_EMAIL,
                Destination = {"ToAddresses": [DOCTOR_EMAIL]},
                Message     = {
                    "Subject": {
                        "Data":    f"🚨 CRITICAL: {alert.get('name')} — {alert.get('ward')} | {HOSPITAL_NAME}",
                        "Charset": "UTF-8",
                    },
                    "Body": {
                        "Text": {"Data": format_email_text(alert), "Charset": "UTF-8"},
                        "Html": {"Data": format_email_html(alert), "Charset": "UTF-8"},
                    },
                },
            )
            logger.info(f"📧 Email sent for {pid}")

            # 3. Also publish to SNS topic (for fan-out to other subscribers)
            sns.publish(
                TopicArn  = SNS_TOPIC_ARN,
                Subject   = f"Critical Alert: {pid}",
                Message   = json.dumps(alert),
                MessageAttributes={
                    "patient_id": {"DataType": "String", "StringValue": pid},
                    "ward":       {"DataType": "String", "StringValue": alert.get("ward", "")},
                },
            )

            logger.warning(
                f"ALERT_FIRED "
                f"patient_id={pid} "
                f"ward={alert.get('ward')} "
                f"alert_count={alert.get('alert_count')} "
                f"alert_id={alert.get('alert_id')}"
            )

            results["processed"] += 1

        except Exception as e:
            results["failed"] += 1
            logger.error(f"Failed to process record: {e}", exc_info=True)
            raise   # re-raise so SQS retries this message

    logger.info(f"Batch complete: {results}")
    return {"statusCode": 200, "body": json.dumps(results)}
