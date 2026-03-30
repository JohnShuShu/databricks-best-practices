"""
Cluster monitoring for Databricks.

Detects zombie clusters, tracks costs, and identifies optimization opportunities.

Usage:
    from monitoring.cluster_monitor import detect_zombie_clusters

    zombies = detect_zombie_clusters()
    if zombies:
        # Alert and/or terminate
"""

from databricks.sdk import WorkspaceClient
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def get_cluster_inventory() -> List[Dict]:
    """
    Get inventory of all clusters with metadata.

    Returns:
        List of cluster info dicts
    """
    w = WorkspaceClient()

    clusters = []
    for cluster in w.clusters.list():
        try:
            info = w.clusters.get(cluster.cluster_id)

            # Calculate uptime
            uptime_minutes = 0
            if info.state and str(info.state) == "RUNNING" and info.start_time:
                uptime_minutes = (datetime.now().timestamp() * 1000 - info.start_time) / 60000

            cluster_data = {
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "state": str(cluster.state) if cluster.state else "UNKNOWN",
                "cluster_source": str(cluster.cluster_source) if cluster.cluster_source else "UNKNOWN",
                "creator_user_name": info.creator_user_name,
                "num_workers": info.num_workers or 0,
                "driver_node_type": info.driver_node_type_id,
                "worker_node_type": info.node_type_id,
                "autoscale_min": info.autoscale.min_workers if info.autoscale else None,
                "autoscale_max": info.autoscale.max_workers if info.autoscale else None,
                "auto_termination_minutes": info.autotermination_minutes,
                "uptime_minutes": round(uptime_minutes, 2),
                "spark_version": info.spark_version,
                "custom_tags": info.custom_tags if info.custom_tags else {},
                "start_time": datetime.fromtimestamp(info.start_time / 1000) if info.start_time else None,
                "snapshot_timestamp": datetime.now()
            }

            # Extract common tags
            if info.custom_tags:
                cluster_data["owner_tag"] = info.custom_tags.get("Owner", "")
                cluster_data["cost_center_tag"] = info.custom_tags.get("CostCenter", "")
                cluster_data["team_tag"] = info.custom_tags.get("Team", "")

            clusters.append(cluster_data)

        except Exception as e:
            print(f"Error getting cluster {cluster.cluster_id}: {e}")

    return clusters


def detect_zombie_clusters(
    uptime_threshold_hours: float = 8.0,
    exclude_job_clusters: bool = True
) -> List[Dict]:
    """
    Detect zombie clusters (running too long, likely forgotten).

    Args:
        uptime_threshold_hours: Alert if running longer than this
        exclude_job_clusters: Don't flag job clusters (they terminate automatically)

    Returns:
        List of zombie cluster info
    """
    clusters = get_cluster_inventory()

    zombies = []
    threshold_minutes = uptime_threshold_hours * 60

    for cluster in clusters:
        # Skip non-running clusters
        if cluster["state"] != "RUNNING":
            continue

        # Optionally skip job clusters
        if exclude_job_clusters and cluster["cluster_source"] == "JOB":
            continue

        # Check uptime threshold
        if cluster["uptime_minutes"] > threshold_minutes:
            cluster["hours_running"] = round(cluster["uptime_minutes"] / 60, 2)
            zombies.append(cluster)

    return zombies


def monitor_cluster_costs(persist_results: bool = True) -> Dict:
    """
    Monitor cluster costs and generate report.

    Args:
        persist_results: Write results to monitoring table

    Returns:
        Cost monitoring summary
    """
    spark = SparkSession.builder.getOrCreate()

    clusters = get_cluster_inventory()

    # Convert to DataFrame
    if clusters:
        clusters_df = spark.createDataFrame(clusters)

        if persist_results:
            clusters_df.write.mode("append").saveAsTable(
                "production.monitoring.cluster_usage"
            )

    # Generate summary
    running_clusters = [c for c in clusters if c["state"] == "RUNNING"]
    ui_clusters = [c for c in running_clusters if c["cluster_source"] == "UI"]

    summary = {
        "snapshot_timestamp": datetime.now().isoformat(),
        "total_clusters": len(clusters),
        "running_clusters": len(running_clusters),
        "ui_clusters_running": len(ui_clusters),
        "job_clusters_running": len([c for c in running_clusters if c["cluster_source"] == "JOB"]),
        "total_workers": sum(c["num_workers"] for c in running_clusters),
        "clusters_without_auto_terminate": len([
            c for c in running_clusters
            if not c["auto_termination_minutes"]
        ])
    }

    return summary


def get_cost_by_team(days: int = 30):
    """
    Get cluster costs aggregated by team tag.

    Args:
        days: Number of days to analyze

    Returns:
        DataFrame with cost breakdown by team
    """
    spark = SparkSession.builder.getOrCreate()

    return spark.sql(f"""
        SELECT
            COALESCE(team_tag, 'untagged') as team,
            COUNT(DISTINCT cluster_id) as cluster_count,
            SUM(uptime_minutes) / 60 as total_hours,
            SUM(num_workers * uptime_minutes) / 60 as worker_hours
        FROM production.monitoring.cluster_usage
        WHERE snapshot_timestamp > current_timestamp() - INTERVAL {days} DAYS
        AND state = 'RUNNING'
        GROUP BY COALESCE(team_tag, 'untagged')
        ORDER BY worker_hours DESC
    """)


def run_cluster_monitor(
    alert_on_zombies: bool = True,
    zombie_threshold_hours: float = 8.0
) -> Dict:
    """
    Main entry point for cluster monitoring job.

    Args:
        alert_on_zombies: Send alerts for zombie clusters
        zombie_threshold_hours: Hours threshold for zombie detection

    Returns:
        Monitoring summary
    """
    # Get cluster inventory and persist
    summary = monitor_cluster_costs(persist_results=True)

    # Detect zombies
    zombies = detect_zombie_clusters(uptime_threshold_hours=zombie_threshold_hours)

    summary["zombie_clusters"] = len(zombies)

    # Alert on zombies
    if alert_on_zombies and zombies:
        try:
            from ..utils.alerting import send_slack_alert

            zombie_details = [{
                "name": z["cluster_name"],
                "hours_running": z["hours_running"],
                "workers": z["num_workers"],
                "owner": z.get("creator_user_name", "unknown")
            } for z in zombies]

            send_slack_alert(
                title="Zombie Clusters Detected",
                message=f"{len(zombies)} clusters have been running for over {zombie_threshold_hours} hours",
                severity="warning",
                details={"clusters": zombie_details}
            )
        except Exception as e:
            print(f"Warning: Failed to send alert: {e}")

    print(f"Cluster Monitor Complete:")
    print(f"  - Total clusters: {summary['total_clusters']}")
    print(f"  - Running clusters: {summary['running_clusters']}")
    print(f"  - Zombie clusters: {summary['zombie_clusters']}")
    print(f"  - Total workers active: {summary['total_workers']}")

    return summary


if __name__ == "__main__":
    run_cluster_monitor()
