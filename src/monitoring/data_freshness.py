"""
Data freshness monitoring for Databricks.

Monitors table update times against SLAs and alerts on breaches.

Usage:
    from monitoring.data_freshness import run_freshness_monitor

    # Run as scheduled job (hourly recommended)
    results = run_freshness_monitor()
"""

from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import SparkSession


# Default SLA configuration
# In production, load this from a config table
DEFAULT_TABLE_SLA_CONFIG = {
    # Critical tables - 4 hour SLA
    "production.sales.orders": {"sla_hours": 4, "tier": "critical"},
    "production.sales.transactions": {"sla_hours": 4, "tier": "critical"},

    # Standard tables - 24 hour SLA
    "production.marketing.campaigns": {"sla_hours": 24, "tier": "standard"},
    "production.finance.invoices": {"sla_hours": 24, "tier": "standard"},

    # Low priority - 72 hour SLA
    "production.analytics.user_segments": {"sla_hours": 72, "tier": "low"},
}

DEFAULT_SLA_HOURS = 24


def get_sla_config(table_name: str, config: Dict = None) -> Dict:
    """
    Get SLA configuration for a table.

    Args:
        table_name: Fully qualified table name
        config: Optional config dict (uses default if not provided)

    Returns:
        Dict with sla_hours and tier
    """
    config = config or DEFAULT_TABLE_SLA_CONFIG
    return config.get(table_name, {"sla_hours": DEFAULT_SLA_HOURS, "tier": "standard"})


def get_table_freshness(table_name: str, config: Dict = None) -> Dict:
    """
    Get freshness information for a single table.

    Args:
        table_name: Fully qualified table name
        config: Optional SLA config

    Returns:
        Dict with table freshness details
    """
    spark = SparkSession.builder.getOrCreate()

    result = {
        "table_name": table_name,
        "check_timestamp": datetime.now(),
        "last_updated": None,
        "hours_since_update": None,
        "sla_hours": None,
        "sla_breached": None,
        "tier": None,
        "error": None
    }

    # Get SLA config
    sla_config = get_sla_config(table_name, config)
    result["sla_hours"] = sla_config.get("sla_hours", DEFAULT_SLA_HOURS)
    result["tier"] = sla_config.get("tier", "standard")

    try:
        # Get last modification from Delta history
        history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()

        if history:
            last_modified = history[0].timestamp
            result["last_updated"] = last_modified

            hours_since = (datetime.now() - last_modified).total_seconds() / 3600
            result["hours_since_update"] = round(hours_since, 2)
            result["sla_breached"] = hours_since > result["sla_hours"]
        else:
            result["error"] = "No history available"

    except Exception as e:
        result["error"] = str(e)

    return result


def check_all_table_freshness(
    tables: List[str] = None,
    catalog: str = None,
    schema: str = None,
    config: Dict = None
) -> List[Dict]:
    """
    Check freshness for multiple tables.

    Args:
        tables: Explicit list of tables to check
        catalog: If provided with schema, discover tables
        schema: If provided with catalog, discover tables
        config: Optional SLA config

    Returns:
        List of freshness results
    """
    spark = SparkSession.builder.getOrCreate()

    if tables is None:
        if catalog and schema:
            # Discover tables in schema
            discovered = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            tables = [f"{catalog}.{schema}.{t.tableName}" for t in discovered]
        else:
            # Use configured tables
            config = config or DEFAULT_TABLE_SLA_CONFIG
            tables = list(config.keys())

    results = []
    for table in tables:
        result = get_table_freshness(table, config)
        results.append(result)

    return results


def run_freshness_monitor(
    tables: List[str] = None,
    config: Dict = None,
    alert_on_breach: bool = True,
    persist_results: bool = True
) -> Dict:
    """
    Main entry point for freshness monitoring job.

    Args:
        tables: Optional list of tables to check
        config: Optional SLA config
        alert_on_breach: Send alerts for SLA breaches
        persist_results: Write results to monitoring table

    Returns:
        Summary dict
    """
    spark = SparkSession.builder.getOrCreate()

    # Check all configured tables
    results = check_all_table_freshness(tables=tables, config=config)

    # Separate by status
    breached = [r for r in results if r.get("sla_breached") == True]
    errors = [r for r in results if r.get("error") is not None]
    healthy = [r for r in results if r.get("sla_breached") == False and r.get("error") is None]

    # Persist results
    if persist_results and results:
        try:
            results_df = spark.createDataFrame(results)
            results_df.write.mode("append").saveAsTable("production.monitoring.data_freshness")
        except Exception as e:
            print(f"Warning: Failed to persist freshness results: {e}")

    # Alert on SLA breaches
    if alert_on_breach and breached:
        try:
            from ..utils.alerting import send_slack_alert

            critical_breaches = [b for b in breached if b.get("tier") == "critical"]
            standard_breaches = [b for b in breached if b.get("tier") != "critical"]

            if critical_breaches:
                breach_details = [{
                    "table": b["table_name"],
                    "hours_stale": b["hours_since_update"],
                    "sla_hours": b["sla_hours"]
                } for b in critical_breaches]

                send_slack_alert(
                    title="CRITICAL: Data Freshness SLA Breach",
                    message=f"{len(critical_breaches)} critical tables have breached SLA",
                    severity="error",
                    details={"tables": breach_details}
                )

            if standard_breaches:
                breach_details = [{
                    "table": b["table_name"],
                    "hours_stale": b["hours_since_update"],
                    "sla_hours": b["sla_hours"]
                } for b in standard_breaches]

                send_slack_alert(
                    title="Data Freshness SLA Breach",
                    message=f"{len(standard_breaches)} tables have breached SLA",
                    severity="warning",
                    details={"tables": breach_details}
                )
        except Exception as e:
            print(f"Warning: Failed to send alert: {e}")

    # Summary
    summary = {
        "check_timestamp": datetime.now().isoformat(),
        "total_tables": len(results),
        "healthy": len(healthy),
        "breached": len(breached),
        "errors": len(errors),
        "critical_breaches": len([b for b in breached if b.get("tier") == "critical"])
    }

    print(f"Freshness Monitor Complete:")
    print(f"  - Tables checked: {summary['total_tables']}")
    print(f"  - Healthy: {summary['healthy']}")
    print(f"  - SLA breaches: {summary['breached']}")
    print(f"  - Critical breaches: {summary['critical_breaches']}")
    print(f"  - Errors: {summary['errors']}")

    return summary


def get_freshness_dashboard_data():
    """
    Get data formatted for freshness dashboard.

    Returns:
        DataFrame with current freshness status
    """
    spark = SparkSession.builder.getOrCreate()

    return spark.sql("""
        WITH latest_checks AS (
            SELECT
                table_name,
                check_timestamp,
                last_updated,
                hours_since_update,
                sla_hours,
                sla_breached,
                tier,
                ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY check_timestamp DESC) as rn
            FROM production.monitoring.data_freshness
            WHERE check_timestamp > current_timestamp() - INTERVAL 7 DAYS
        )
        SELECT
            table_name,
            check_timestamp,
            last_updated,
            hours_since_update,
            sla_hours,
            sla_breached,
            tier,
            CASE
                WHEN sla_breached THEN 'BREACH'
                WHEN hours_since_update > sla_hours * 0.8 THEN 'WARNING'
                ELSE 'HEALTHY'
            END as status
        FROM latest_checks
        WHERE rn = 1
        ORDER BY sla_breached DESC, hours_since_update DESC
    """)


if __name__ == "__main__":
    run_freshness_monitor()
