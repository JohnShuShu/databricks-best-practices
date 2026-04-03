# Phase 1: Foundation - Kanban Tickets

> **Phase Goal:** Establish core infrastructure that all other improvements depend on
> **Timeline:** Recommended 2-3 sprints
> **Dependencies:** None (this is the foundation)

---

## Ticket 1: Set Up Unity Catalog Hierarchy

**Title:** Set up Unity Catalog hierarchy with environment separation

**Type:** Infrastructure

**Priority:** Critical

**Story Points:** 5

**Description:**
Create a proper Unity Catalog hierarchy with separate catalogs per environment and schemas per domain. This establishes the foundation for all data governance and access control.

**Acceptance Criteria:**
- [ ] Create `production`, `staging`, and `development` catalogs
- [ ] Create domain schemas in each catalog (e.g., `sales`, `marketing`, `finance`, `bronze`, `silver`, `gold`)
- [ ] Create `monitoring` schema for platform observability tables
- [ ] Document the catalog/schema naming conventions
- [ ] Verify all catalogs are accessible from appropriate workspaces

**Technical Details:**
```sql
-- Production catalog
CREATE CATALOG IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS production.bronze;
CREATE SCHEMA IF NOT EXISTS production.silver;
CREATE SCHEMA IF NOT EXISTS production.gold;
CREATE SCHEMA IF NOT EXISTS production.sales;
CREATE SCHEMA IF NOT EXISTS production.marketing;
CREATE SCHEMA IF NOT EXISTS production.finance;

-- Staging catalog
CREATE CATALOG IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS staging.bronze;
CREATE SCHEMA IF NOT EXISTS staging.silver;
CREATE SCHEMA IF NOT EXISTS staging.gold;

-- Development catalog
CREATE CATALOG IF NOT EXISTS development;
CREATE SCHEMA IF NOT EXISTS development.bronze;
CREATE SCHEMA IF NOT EXISTS development.silver;
CREATE SCHEMA IF NOT EXISTS development.gold;

-- Monitoring (can be in production or separate)
CREATE SCHEMA IF NOT EXISTS production.monitoring;
```

**Blocked By:** None

**Blocks:** Tickets 4, 5 (monitoring schema needed)

**Labels:** `infrastructure`, `unity-catalog`, `critical`

---

## Ticket 2: Create and Enforce Cluster Policies

**Title:** Create cluster policies to enforce cost controls and standards

**Type:** Infrastructure

**Priority:** Critical

**Story Points:** 3

**Description:**
Create cluster policies that enforce auto-termination, approved instance types, worker limits, and required tagging. Apply policies to all user groups to prevent cost waste from over-provisioned or zombie clusters.

**Acceptance Criteria:**
- [ ] Create "Standard Interactive" policy for data engineers
- [ ] Create "Analyst" policy with stricter limits
- [ ] Create "Job Cluster" policy for automated workloads
- [ ] All policies enforce auto-termination (max 2 hours)
- [ ] All policies require `CostCenter` and `Owner` tags
- [ ] All policies limit worker count (e.g., max 20)
- [ ] All policies restrict to approved node types
- [ ] Apply policies to appropriate groups
- [ ] Document policy usage in onboarding docs

**Technical Details:**
```json
{
  "name": "Standard Interactive",
  "definition": {
    "spark_version": {
      "type": "regex",
      "pattern": "1[3-5]\\.[0-9]+\\.x-scala.*",
      "defaultValue": "14.3.x-scala2.12"
    },
    "autotermination_minutes": {
      "type": "range",
      "minValue": 10,
      "maxValue": 120,
      "defaultValue": 60
    },
    "custom_tags.CostCenter": {
      "type": "fixed",
      "value": "data-platform"
    },
    "custom_tags.Owner": {
      "type": "regex",
      "pattern": ".*@company\\.com"
    },
    "num_workers": {
      "type": "range",
      "minValue": 1,
      "maxValue": 20
    },
    "node_type_id": {
      "type": "allowlist",
      "values": [
        "Standard_D4ds_v5",
        "Standard_D8ds_v5",
        "Standard_D16ds_v5"
      ],
      "defaultValue": "Standard_D4ds_v5"
    }
  }
}
```

**Blocked By:** None

**Blocks:** None (but enables Ticket 12 - zombie cluster monitoring)

**Labels:** `infrastructure`, `cost-optimization`, `critical`

---

## Ticket 3: Set Up Secret Scopes and Remove Hardcoded Credentials

**Title:** Implement Databricks secret scopes and audit/remove hardcoded credentials

**Type:** Security

**Priority:** Critical

**Story Points:** 5

**Description:**
Create Databricks secret scopes for all external system credentials. Audit existing notebooks and code for hardcoded credentials and migrate them to secret scopes. Create utility functions for consistent secret retrieval.

**Acceptance Criteria:**
- [ ] Create secret scopes per system category:
  - `databases` - PostgreSQL, MySQL, SQL Server credentials
  - `cloud-storage` - AWS/Azure/GCP keys
  - `apis` - External API keys
  - `alerts` - Slack/PagerDuty webhooks
  - `kafka` - Kafka/Event Hub credentials
- [ ] Audit all existing notebooks for hardcoded credentials
- [ ] Create list of notebooks requiring remediation
- [ ] Migrate at least 3 critical pipelines to use secret scopes
- [ ] Create `get_connection_config()` utility function
- [ ] Document secret scope naming conventions and usage

**Technical Details:**
```bash
# Create secret scopes via CLI
databricks secrets create-scope --scope databases
databricks secrets create-scope --scope cloud-storage
databricks secrets create-scope --scope apis
databricks secrets create-scope --scope alerts
databricks secrets create-scope --scope kafka

# Add secrets
databricks secrets put --scope databases --key postgres_prod_host
databricks secrets put --scope databases --key postgres_prod_username
databricks secrets put --scope databases --key postgres_prod_password
```

```python
# Utility function to add to shared library
def get_connection_config(scope: str, prefix: str) -> dict:
    """
    Retrieve connection config from secret scope.

    Args:
        scope: Secret scope name (e.g., 'databases')
        prefix: Key prefix (e.g., 'postgres_prod')

    Returns:
        Dict with host, port, username, password
    """
    return {
        "host": dbutils.secrets.get(scope, f"{prefix}_host"),
        "port": dbutils.secrets.get(scope, f"{prefix}_port"),
        "username": dbutils.secrets.get(scope, f"{prefix}_username"),
        "password": dbutils.secrets.get(scope, f"{prefix}_password"),
    }

def get_jdbc_url(scope: str, prefix: str, database: str) -> str:
    """Build JDBC URL from secrets."""
    config = get_connection_config(scope, prefix)
    return f"jdbc:postgresql://{config['host']}:{config['port']}/{database}"
```

**Blocked By:** None

**Blocks:** Ticket 6 (alerting needs webhook secrets)

**Labels:** `security`, `critical`, `tech-debt`

---

## Ticket 4: Create Monitoring Schema and Base Tables

**Title:** Create monitoring schema with tables for platform observability

**Type:** Infrastructure

**Priority:** Critical

**Story Points:** 3

**Description:**
Create the monitoring schema and base tables that will store job health metrics, data quality results, cluster usage, and other platform observability data.

**Acceptance Criteria:**
- [ ] Create `monitoring` schema in production catalog
- [ ] Create `job_runs` table for job execution history
- [ ] Create `job_health_metrics` table for daily health snapshots
- [ ] Create `dq_results` table for data quality check results
- [ ] Create `cluster_usage` table for cluster metrics
- [ ] Create `data_freshness` table for SLA tracking
- [ ] Create `alerts_sent` table for alert history
- [ ] Set appropriate retention policies (90 days default)
- [ ] Grant read access to analysts, write access to service principals

**Technical Details:**
```sql
CREATE SCHEMA IF NOT EXISTS production.monitoring;

-- Job execution history
CREATE TABLE IF NOT EXISTS production.monitoring.job_runs (
    run_id BIGINT,
    job_id BIGINT,
    job_name STRING,
    run_start_time TIMESTAMP,
    run_end_time TIMESTAMP,
    duration_minutes DOUBLE,
    status STRING,
    error_message STRING,
    trigger_type STRING,
    cluster_id STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(run_start_time))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Daily health metrics snapshot
CREATE TABLE IF NOT EXISTS production.monitoring.job_health_metrics (
    snapshot_date DATE,
    metrics_json STRING,
    failed_jobs_count INT,
    long_running_jobs_count INT,
    total_jobs_monitored INT,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Data quality results
CREATE TABLE IF NOT EXISTS production.monitoring.dq_results (
    check_id STRING,
    table_name STRING,
    check_name STRING,
    check_timestamp TIMESTAMP,
    passed BOOLEAN,
    severity STRING,
    row_count BIGINT,
    failure_details STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(check_timestamp));

-- Cluster usage metrics
CREATE TABLE IF NOT EXISTS production.monitoring.cluster_usage (
    snapshot_timestamp TIMESTAMP,
    cluster_id STRING,
    cluster_name STRING,
    state STRING,
    cluster_source STRING,
    num_workers INT,
    driver_node_type STRING,
    worker_node_type STRING,
    uptime_minutes DOUBLE,
    estimated_dbu_cost DOUBLE,
    owner_tag STRING,
    cost_center_tag STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(snapshot_timestamp));

-- Data freshness tracking
CREATE TABLE IF NOT EXISTS production.monitoring.data_freshness (
    check_timestamp TIMESTAMP,
    table_name STRING,
    last_updated TIMESTAMP,
    hours_since_update DOUBLE,
    sla_hours DOUBLE,
    sla_breached BOOLEAN,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Alert history
CREATE TABLE IF NOT EXISTS production.monitoring.alerts_sent (
    alert_id STRING,
    alert_timestamp TIMESTAMP,
    alert_type STRING,
    severity STRING,
    title STRING,
    message STRING,
    details_json STRING,
    destination STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Set retention
ALTER TABLE production.monitoring.job_runs
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 90 days');
```

**Blocked By:** Ticket 1 (Unity Catalog hierarchy)

**Blocks:** Ticket 5 (job health monitor)

**Labels:** `infrastructure`, `monitoring`, `critical`

---

## Ticket 5: Implement Job Health Monitor

**Title:** Create daily job health monitoring job with failure detection

**Type:** Feature

**Priority:** Critical

**Story Points:** 5

**Description:**
Create a Databricks job that runs daily (and optionally hourly) to monitor all job runs, detect failures, identify long-running jobs, and write metrics to the monitoring tables. Integrate with alerting for immediate notification of critical failures.

**Acceptance Criteria:**
- [ ] Create notebook/script that queries Databricks Jobs API
- [ ] Capture all failed jobs from last 24 hours with error messages
- [ ] Identify jobs running > 2x their average duration
- [ ] Identify jobs with > 3 retry attempts
- [ ] Write results to `monitoring.job_health_metrics` table
- [ ] Write detailed run data to `monitoring.job_runs` table
- [ ] Send Slack alert for any critical job failures
- [ ] Schedule job to run every hour
- [ ] Create daily summary alert at 8 AM

**Technical Details:**
```python
# job_health_monitor.py
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
from pyspark.sql.functions import *

def get_job_health_metrics(lookback_hours: int = 24):
    """
    Collect job health metrics from Databricks Jobs API.
    """
    w = WorkspaceClient()

    cutoff = int((datetime.now() - timedelta(hours=lookback_hours)).timestamp() * 1000)

    metrics = {
        "snapshot_date": datetime.now().date().isoformat(),
        "failed_jobs": [],
        "long_running_jobs": [],
        "high_retry_jobs": [],
        "successful_jobs": 0,
        "total_runs": 0
    }

    all_runs = []

    for job in w.jobs.list():
        try:
            runs = list(w.jobs.list_runs(job_id=job.job_id, start_time_from=cutoff))

            for run in runs:
                metrics["total_runs"] += 1

                run_data = {
                    "run_id": run.run_id,
                    "job_id": job.job_id,
                    "job_name": job.settings.name if job.settings else f"job_{job.job_id}",
                    "run_start_time": datetime.fromtimestamp(run.start_time / 1000) if run.start_time else None,
                    "run_end_time": datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None,
                    "status": str(run.state.result_state) if run.state else "UNKNOWN",
                    "error_message": run.state.state_message if run.state else None,
                    "trigger_type": str(run.trigger) if run.trigger else "UNKNOWN",
                    "cluster_id": run.cluster_instance.cluster_id if run.cluster_instance else None
                }

                # Calculate duration
                if run.end_time and run.start_time:
                    run_data["duration_minutes"] = (run.end_time - run.start_time) / 60000
                else:
                    run_data["duration_minutes"] = None

                all_runs.append(run_data)

                # Track failures
                if run.state and run.state.result_state and str(run.state.result_state) == "FAILED":
                    metrics["failed_jobs"].append({
                        "job_id": job.job_id,
                        "job_name": job.settings.name if job.settings else f"job_{job.job_id}",
                        "run_id": run.run_id,
                        "error": run.state.state_message
                    })
                elif run.state and run.state.result_state and str(run.state.result_state) == "SUCCESS":
                    metrics["successful_jobs"] += 1

                # Track long-running (> 2 hours as default threshold)
                if run_data["duration_minutes"] and run_data["duration_minutes"] > 120:
                    metrics["long_running_jobs"].append({
                        "job_id": job.job_id,
                        "job_name": job.settings.name if job.settings else f"job_{job.job_id}",
                        "duration_mins": run_data["duration_minutes"]
                    })

        except Exception as e:
            print(f"Error processing job {job.job_id}: {e}")

    return metrics, all_runs

def run_job_health_monitor():
    """Main entry point for the monitoring job."""

    # Get metrics
    metrics, all_runs = get_job_health_metrics(lookback_hours=24)

    # Write run details to monitoring table
    if all_runs:
        runs_df = spark.createDataFrame(all_runs)
        runs_df.write.mode("append").saveAsTable("production.monitoring.job_runs")

    # Write summary metrics
    import json
    summary_df = spark.createDataFrame([{
        "snapshot_date": metrics["snapshot_date"],
        "metrics_json": json.dumps(metrics),
        "failed_jobs_count": len(metrics["failed_jobs"]),
        "long_running_jobs_count": len(metrics["long_running_jobs"]),
        "total_jobs_monitored": metrics["total_runs"]
    }])
    summary_df.write.mode("append").saveAsTable("production.monitoring.job_health_metrics")

    # Alert on failures
    if metrics["failed_jobs"]:
        send_slack_alert(
            title="Job Failures Detected",
            message=f"{len(metrics['failed_jobs'])} jobs failed in the last 24 hours",
            severity="error",
            details={"failed_jobs": metrics["failed_jobs"][:10]}  # Limit to 10
        )

    return metrics

# Run
if __name__ == "__main__":
    run_job_health_monitor()
```

**Job Configuration:**
- Schedule: `0 0 * * * ?` (every hour)
- Cluster: Job cluster with 1 worker (lightweight)
- Timeout: 30 minutes
- Retries: 2
- Alerts: On failure

**Blocked By:** Ticket 4 (monitoring schema), Ticket 6 (alerting)

**Blocks:** None

**Labels:** `monitoring`, `feature`, `critical`

---

## Ticket 6: Create Slack Alerting Utility

**Title:** Create reusable Slack alerting utility function

**Type:** Feature

**Priority:** Critical

**Story Points:** 2

**Description:**
Create a reusable Python utility for sending alerts to Slack. This will be used by all monitoring jobs and can be called from any pipeline for failure notifications. Store webhook URL in secret scope.

**Acceptance Criteria:**
- [ ] Create `send_slack_alert()` function with severity levels
- [ ] Support info, warning, error severity with color coding
- [ ] Support structured details/attachments
- [ ] Retrieve webhook URL from secret scope
- [ ] Log all alerts to `monitoring.alerts_sent` table
- [ ] Handle webhook failures gracefully (don't crash calling job)
- [ ] Create wrapper for common alert types (job failure, DQ failure, SLA breach)
- [ ] Test with real Slack channel

**Technical Details:**
```python
# alerting.py - Add to shared utilities library
import requests
import json
from datetime import datetime
import uuid

def send_slack_alert(
    title: str,
    message: str,
    severity: str = "warning",
    details: dict = None,
    channel_override: str = None
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
        alert_record = spark.createDataFrame([{
            "alert_id": alert_id,
            "alert_timestamp": datetime.now(),
            "alert_type": "slack",
            "severity": severity,
            "title": title,
            "message": message,
            "details_json": json.dumps(details) if details else None,
            "destination": webhook_key
        }])
        alert_record.write.mode("append").saveAsTable("production.monitoring.alerts_sent")
    except Exception as e:
        print(f"Failed to log alert: {e}")

    return success


# Convenience wrappers for common alert types
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
```

**Setup Steps:**
1. Create Slack app with incoming webhooks enabled
2. Add webhook URL to secret scope: `databricks secrets put --scope alerts --key slack_webhook_default`
3. Add utility to shared library accessible from all notebooks

**Blocked By:** Ticket 3 (secret scopes)

**Blocks:** Ticket 5 (job health monitor needs alerting)

**Labels:** `feature`, `alerting`, `critical`

---

## Phase 1 Summary

| Ticket | Title | Points | Dependencies |
|--------|-------|--------|--------------|
| 1 | Unity Catalog Hierarchy | 5 | None |
| 2 | Cluster Policies | 3 | None |
| 3 | Secret Scopes | 5 | None |
| 4 | Monitoring Schema | 3 | Ticket 1 |
| 5 | Job Health Monitor | 5 | Tickets 4, 6 |
| 6 | Slack Alerting | 2 | Ticket 3 |

**Total Story Points:** 23

**Recommended Sprint Breakdown:**
- **Sprint 1:** Tickets 1, 2, 3 (parallel, no dependencies) - 13 points
- **Sprint 2:** Tickets 4, 6, then 5 (sequential dependencies) - 10 points

**Definition of Done for Phase 1:**
- [ ] All catalogs and schemas created and accessible
- [ ] Cluster policies applied to all user groups
- [ ] All critical credentials migrated to secret scopes
- [ ] Monitoring tables created and queryable
- [ ] Job health monitor running hourly
- [ ] Slack alerts firing for test failures
- [ ] Documentation updated with new standards
