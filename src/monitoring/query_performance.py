"""
Query performance monitoring for Databricks.

Tracks slow queries, identifies optimization opportunities,
and monitors query patterns.

Requires Unity Catalog system tables access.

Usage:
    from monitoring.query_performance import get_slow_queries

    slow = get_slow_queries(threshold_ms=300000)  # 5 minutes
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Dict


def get_slow_queries(
    threshold_ms: int = 300000,
    lookback_hours: int = 24,
    limit: int = 100
) -> DataFrame:
    """
    Get queries that exceeded duration threshold.

    Args:
        threshold_ms: Duration threshold in milliseconds (default 5 min)
        lookback_hours: Hours to look back
        limit: Maximum number of queries to return

    Returns:
        DataFrame with slow query details
    """
    spark = SparkSession.builder.getOrCreate()

    return spark.sql(f"""
        SELECT
            user_name,
            statement_type,
            total_duration_ms,
            rows_produced,
            bytes_read,
            SUBSTRING(statement_text, 1, 500) as statement_preview,
            start_time,
            end_time
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL {lookback_hours} HOURS
        AND total_duration_ms > {threshold_ms}
        ORDER BY total_duration_ms DESC
        LIMIT {limit}
    """)


def get_query_patterns_by_user(lookback_hours: int = 24) -> DataFrame:
    """
    Analyze query patterns grouped by user.

    Args:
        lookback_hours: Hours to look back

    Returns:
        DataFrame with per-user query statistics
    """
    spark = SparkSession.builder.getOrCreate()

    return spark.sql(f"""
        SELECT
            user_name,
            COUNT(*) as query_count,
            ROUND(AVG(total_duration_ms), 2) as avg_duration_ms,
            ROUND(MAX(total_duration_ms), 2) as max_duration_ms,
            SUM(bytes_read) as total_bytes_read,
            SUM(rows_produced) as total_rows_produced,
            COUNT(CASE WHEN total_duration_ms > 60000 THEN 1 END) as slow_query_count
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL {lookback_hours} HOURS
        GROUP BY user_name
        ORDER BY total_bytes_read DESC
    """)


def get_problematic_queries(lookback_hours: int = 24) -> DataFrame:
    """
    Identify potentially problematic query patterns.

    Looks for:
    - High average duration users
    - Large data scans
    - Frequent similar queries (possible missing caching)

    Args:
        lookback_hours: Hours to look back

    Returns:
        DataFrame with problematic patterns
    """
    spark = SparkSession.builder.getOrCreate()

    return spark.sql(f"""
        SELECT
            user_name,
            COUNT(*) as query_count,
            ROUND(AVG(total_duration_ms) / 1000, 2) as avg_duration_seconds,
            ROUND(SUM(bytes_read) / 1024 / 1024 / 1024, 2) as total_gb_read,
            ROUND(AVG(bytes_read) / 1024 / 1024, 2) as avg_mb_per_query
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL {lookback_hours} HOURS
        GROUP BY user_name
        HAVING AVG(total_duration_ms) > 60000  -- Avg > 1 minute
           OR SUM(bytes_read) > 100 * 1024 * 1024 * 1024  -- > 100 GB total
        ORDER BY avg_duration_seconds DESC
    """)


def get_table_scan_patterns(lookback_hours: int = 24) -> DataFrame:
    """
    Analyze which tables are being scanned most frequently.

    Args:
        lookback_hours: Hours to look back

    Returns:
        DataFrame with table access patterns
    """
    spark = SparkSession.builder.getOrCreate()

    # Note: This requires parsing query text or using lineage tables
    # Simplified version using query history
    return spark.sql(f"""
        SELECT
            statement_type,
            COUNT(*) as query_count,
            ROUND(AVG(total_duration_ms), 2) as avg_duration_ms,
            ROUND(SUM(bytes_read) / 1024 / 1024 / 1024, 2) as total_gb_read
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL {lookback_hours} HOURS
        GROUP BY statement_type
        ORDER BY total_gb_read DESC
    """)


def monitor_query_performance(
    lookback_hours: int = 24,
    slow_query_threshold_ms: int = 300000,
    alert_on_issues: bool = True,
    persist_results: bool = True
) -> Dict:
    """
    Main entry point for query performance monitoring.

    Args:
        lookback_hours: Hours to analyze
        slow_query_threshold_ms: Threshold for slow query alerts
        alert_on_issues: Send alerts for performance issues
        persist_results: Write analysis to monitoring tables

    Returns:
        Summary dict
    """
    spark = SparkSession.builder.getOrCreate()

    # Get slow queries
    slow_queries = get_slow_queries(
        threshold_ms=slow_query_threshold_ms,
        lookback_hours=lookback_hours
    )
    slow_count = slow_queries.count()

    # Get problematic patterns
    problematic = get_problematic_queries(lookback_hours=lookback_hours)
    problematic_users = problematic.count()

    # Get user patterns
    user_patterns = get_query_patterns_by_user(lookback_hours=lookback_hours)

    summary = {
        "check_timestamp": datetime.now().isoformat(),
        "lookback_hours": lookback_hours,
        "slow_queries_count": slow_count,
        "problematic_users_count": problematic_users,
        "total_users_querying": user_patterns.count()
    }

    # Persist results
    if persist_results:
        try:
            # Save slow queries for analysis
            slow_queries.withColumn(
                "snapshot_timestamp",
                spark.sql("SELECT current_timestamp()").collect()[0][0]
            ).write.mode("append").saveAsTable(
                "production.monitoring.slow_queries"
            )
        except Exception as e:
            print(f"Warning: Failed to persist slow queries: {e}")

    # Alert on issues
    if alert_on_issues and (slow_count > 10 or problematic_users > 0):
        try:
            from ..utils.alerting import send_slack_alert

            if problematic_users > 0:
                problem_list = problematic.limit(5).collect()
                details = [{
                    "user": row.user_name,
                    "avg_duration_sec": row.avg_duration_seconds,
                    "total_gb_read": row.total_gb_read
                } for row in problem_list]

                send_slack_alert(
                    title="Query Performance Issues Detected",
                    message=f"{problematic_users} users with problematic query patterns",
                    severity="warning",
                    details={"users": details}
                )
        except Exception as e:
            print(f"Warning: Failed to send alert: {e}")

    print(f"Query Performance Monitor Complete:")
    print(f"  - Slow queries (>{slow_query_threshold_ms}ms): {slow_count}")
    print(f"  - Users with issues: {problematic_users}")
    print(f"  - Total active users: {summary['total_users_querying']}")

    return summary


if __name__ == "__main__":
    monitor_query_performance()
