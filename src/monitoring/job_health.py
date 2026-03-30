"""
Job health monitoring for Databricks.

Monitors job runs, detects failures, identifies long-running jobs,
and sends alerts for issues.

Usage:
    from monitoring.job_health import run_job_health_monitor

    # Run as scheduled job (hourly recommended)
    metrics = run_job_health_monitor()
"""

from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json


def get_job_health_metrics(lookback_hours: int = 24) -> Dict:
    """
    Collect job health metrics from Databricks Jobs API.

    Args:
        lookback_hours: Number of hours to look back for job runs

    Returns:
        Dict containing failed_jobs, long_running_jobs, metrics summary
    """
    w = WorkspaceClient()

    cutoff = int((datetime.now() - timedelta(hours=lookback_hours)).timestamp() * 1000)

    metrics = {
        "snapshot_date": datetime.now().date().isoformat(),
        "snapshot_timestamp": datetime.now().isoformat(),
        "lookback_hours": lookback_hours,
        "failed_jobs": [],
        "long_running_jobs": [],
        "high_retry_jobs": [],
        "successful_runs": 0,
        "failed_runs": 0,
        "total_runs": 0,
        "jobs_checked": 0
    }

    all_runs = []

    for job in w.jobs.list():
        metrics["jobs_checked"] += 1

        try:
            runs = list(w.jobs.list_runs(job_id=job.job_id, start_time_from=cutoff))

            for run in runs:
                metrics["total_runs"] += 1

                # Build run record
                run_data = {
                    "run_id": run.run_id,
                    "job_id": job.job_id,
                    "job_name": job.settings.name if job.settings else f"job_{job.job_id}",
                    "run_start_time": datetime.fromtimestamp(run.start_time / 1000) if run.start_time else None,
                    "run_end_time": datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None,
                    "status": str(run.state.result_state) if run.state and run.state.result_state else "RUNNING",
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
                result_state = str(run.state.result_state) if run.state and run.state.result_state else None

                if result_state == "FAILED":
                    metrics["failed_runs"] += 1
                    metrics["failed_jobs"].append({
                        "job_id": job.job_id,
                        "job_name": job.settings.name if job.settings else f"job_{job.job_id}",
                        "run_id": run.run_id,
                        "error": run.state.state_message if run.state else "Unknown error",
                        "failed_at": run_data["run_end_time"].isoformat() if run_data["run_end_time"] else None
                    })
                elif result_state == "SUCCESS":
                    metrics["successful_runs"] += 1

                # Track long-running (> 2 hours as default threshold)
                if run_data["duration_minutes"] and run_data["duration_minutes"] > 120:
                    metrics["long_running_jobs"].append({
                        "job_id": job.job_id,
                        "job_name": job.settings.name if job.settings else f"job_{job.job_id}",
                        "run_id": run.run_id,
                        "duration_minutes": round(run_data["duration_minutes"], 2)
                    })

        except Exception as e:
            print(f"Error processing job {job.job_id}: {e}")

    # Calculate success rate
    if metrics["total_runs"] > 0:
        metrics["success_rate"] = round(
            metrics["successful_runs"] / metrics["total_runs"] * 100, 2
        )
    else:
        metrics["success_rate"] = 100.0

    return metrics, all_runs


def run_job_health_monitor(
    lookback_hours: int = 24,
    alert_on_failures: bool = True,
    persist_results: bool = True
) -> Dict:
    """
    Main entry point for the job health monitoring job.

    Args:
        lookback_hours: Hours to look back
        alert_on_failures: Send Slack alerts for failures
        persist_results: Write results to monitoring tables

    Returns:
        Metrics summary dict
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Get metrics
    metrics, all_runs = get_job_health_metrics(lookback_hours=lookback_hours)

    # Persist run details
    if persist_results and all_runs:
        try:
            runs_df = spark.createDataFrame(all_runs)
            runs_df.write.mode("append").saveAsTable("production.monitoring.job_runs")
        except Exception as e:
            print(f"Warning: Failed to persist run details: {e}")

    # Persist summary metrics
    if persist_results:
        try:
            summary_df = spark.createDataFrame([{
                "snapshot_date": metrics["snapshot_date"],
                "metrics_json": json.dumps(metrics),
                "failed_jobs_count": len(metrics["failed_jobs"]),
                "long_running_jobs_count": len(metrics["long_running_jobs"]),
                "total_jobs_monitored": metrics["total_runs"],
                "success_rate": metrics["success_rate"]
            }])
            summary_df.write.mode("append").saveAsTable("production.monitoring.job_health_metrics")
        except Exception as e:
            print(f"Warning: Failed to persist metrics summary: {e}")

    # Alert on failures
    if alert_on_failures and metrics["failed_jobs"]:
        try:
            from ..utils.alerting import send_slack_alert

            send_slack_alert(
                title="Job Failures Detected",
                message=f"{len(metrics['failed_jobs'])} jobs failed in the last {lookback_hours} hours\nSuccess rate: {metrics['success_rate']}%",
                severity="error",
                details={"failed_jobs": metrics["failed_jobs"][:10]}  # Limit to 10
            )
        except Exception as e:
            print(f"Warning: Failed to send alert: {e}")

    # Print summary
    print(f"Job Health Monitor Complete:")
    print(f"  - Jobs checked: {metrics['jobs_checked']}")
    print(f"  - Total runs: {metrics['total_runs']}")
    print(f"  - Failed runs: {metrics['failed_runs']}")
    print(f"  - Success rate: {metrics['success_rate']}%")
    print(f"  - Long-running jobs: {len(metrics['long_running_jobs'])}")

    return metrics


def get_job_failure_trends(days: int = 30):
    """
    Get job failure trends over time.

    Args:
        days: Number of days to analyze

    Returns:
        DataFrame with daily failure counts and trends
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    return spark.sql(f"""
        SELECT
            date(run_start_time) as run_date,
            COUNT(*) as total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
            ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
        FROM production.monitoring.job_runs
        WHERE run_start_time > current_date() - INTERVAL {days} DAYS
        GROUP BY date(run_start_time)
        ORDER BY run_date DESC
    """)


if __name__ == "__main__":
    # Run monitor when executed directly
    run_job_health_monitor()
