"""
Alerting utilities for Slack/Teams integration.

Usage:
    from utils.alerting import send_slack_alert

    send_slack_alert(
        title="Pipeline Failed",
        message="Orders ETL failed with error...",
        severity="error",
        details={"job_id": 123, "run_id": 456}
    )
"""

import requests
import json
from datetime import datetime
import uuid
from typing import Optional


def send_slack_alert(
    title: str,
    message: str,
    severity: str = "warning",
    details: Optional[dict] = None,
    channel_override: Optional[str] = None
) -> bool:
    """
    Send alert to Slack channel.

    Args:
        title: Alert title
        message: Alert message body
        severity: 'info', 'warning', or 'error'
        details: Optional dict of additional details
        channel_override: Override default webhook (use secret key name)

    Returns:
        True if alert sent successfully, False otherwise
    """
    color_map = {
        "info": "#36a64f",    # Green
        "warning": "#ff9900", # Orange
        "error": "#ff0000"   # Red
    }

    emoji_map = {
        "info": "information_source",
        "warning": "warning",
        "error": "rotating_light"
    }

    color = color_map.get(severity, "#808080")
    emoji = emoji_map.get(severity, "bell")

    # Build message blocks
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":{emoji}: {title}",
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*Severity:* {severity.upper()} | *Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                }
            ]
        }
    ]

    # Add details if provided
    if details:
        details_text = json.dumps(details, indent=2, default=str)
        if len(details_text) > 2900:  # Slack limit
            details_text = details_text[:2900] + "\n... (truncated)"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```{details_text}```"
            }
        })

    payload = {
        "attachments": [{
            "color": color,
            "blocks": blocks
        }]
    }

    # Get webhook URL from secrets
    try:
        # Import dbutils only when running in Databricks
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        webhook_key = channel_override or "slack_webhook_default"
        webhook_url = dbutils.secrets.get("alerts", webhook_key)
    except Exception as e:
        print(f"Failed to get webhook secret: {e}")
        return False

    # Send alert
    alert_id = str(uuid.uuid4())
    success = False

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )
        response.raise_for_status()
        success = True
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

    # Log alert to monitoring table
    try:
        spark = SparkSession.builder.getOrCreate()
        alert_record = spark.createDataFrame([{
            "alert_id": alert_id,
            "alert_timestamp": datetime.now(),
            "alert_type": "slack",
            "severity": severity,
            "title": title,
            "message": message,
            "details_json": json.dumps(details) if details else None,
            "destination": webhook_key if 'webhook_key' in dir() else "unknown"
        }])
        alert_record.write.mode("append").saveAsTable("production.monitoring.alerts_sent")
    except Exception as e:
        print(f"Failed to log alert: {e}")

    return success


def alert_job_failure(job_name: str, job_id: int, run_id: int, error: str):
    """Send alert for job failure."""
    send_slack_alert(
        title=f"Job Failed: {job_name}",
        message=f"Job `{job_name}` (ID: {job_id}) failed on run {run_id}",
        severity="error",
        details={"job_id": job_id, "run_id": run_id, "error": error}
    )


def alert_dq_failure(table_name: str, failed_checks: list):
    """Send alert for data quality failures."""
    send_slack_alert(
        title=f"Data Quality Failed: {table_name}",
        message=f"Data quality checks failed for `{table_name}`",
        severity="error",
        details={"table": table_name, "failed_checks": failed_checks}
    )


def alert_sla_breach(table_name: str, hours_stale: float, sla_hours: float):
    """Send alert for SLA breach."""
    send_slack_alert(
        title=f"SLA Breach: {table_name}",
        message=f"Table `{table_name}` is {hours_stale:.1f} hours stale (SLA: {sla_hours} hours)",
        severity="warning",
        details={"table": table_name, "hours_stale": hours_stale, "sla_hours": sla_hours}
    )


def send_teams_alert(
    title: str,
    message: str,
    severity: str = "warning",
    details: Optional[dict] = None,
    webhook_key: str = "teams_webhook_default"
) -> bool:
    """
    Send alert to Microsoft Teams channel.

    Args:
        title: Alert title
        message: Alert message body
        severity: 'info', 'warning', or 'error'
        details: Optional dict of additional details
        webhook_key: Secret key for webhook URL

    Returns:
        True if alert sent successfully
    """
    color_map = {
        "info": "00FF00",
        "warning": "FFA500",
        "error": "FF0000"
    }

    color = color_map.get(severity, "808080")

    # Build adaptive card
    card = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": title,
        "sections": [{
            "activityTitle": title,
            "facts": [
                {"name": "Severity", "value": severity.upper()},
                {"name": "Time", "value": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')},
            ],
            "text": message
        }]
    }

    if details:
        card["sections"][0]["facts"].append({
            "name": "Details",
            "value": json.dumps(details, indent=2)[:1000]
        })

    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        webhook_url = dbutils.secrets.get("alerts", webhook_key)
        response = requests.post(webhook_url, json=card, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Failed to send Teams alert: {e}")
        return False
