"""
Data Quality Framework for Databricks.

A flexible, reusable framework for running data quality checks
against DataFrames with severity levels, alerting, and persistence.

Usage:
    from data_quality.framework import run_dq_checks
    from data_quality.checks import check_no_nulls, check_no_duplicates

    checks = [
        check_no_nulls("order_id"),
        check_no_duplicates(["order_id"]),
    ]

    results = run_dq_checks(df, checks, "production.sales.orders")
"""

from dataclasses import dataclass
from typing import List, Callable, Optional, Dict, Any
from pyspark.sql import DataFrame
from datetime import datetime
import uuid
import time


@dataclass
class DQCheck:
    """Data quality check definition."""
    name: str
    check_fn: Callable[[DataFrame], bool]
    severity: str  # "error" | "warning"
    description: str = ""


@dataclass
class DQResult:
    """Result of a single DQ check."""
    check_name: str
    passed: bool
    severity: str
    execution_time_ms: float
    error_message: Optional[str] = None


def run_dq_checks(
    df: DataFrame,
    checks: List[DQCheck],
    table_name: str,
    fail_on_error: bool = True,
    alert_on_failure: bool = True,
    persist_results: bool = True
) -> Dict[str, Any]:
    """
    Run data quality checks against a DataFrame.

    Args:
        df: DataFrame to validate
        checks: List of DQCheck objects to run
        table_name: Name of table being checked (for logging)
        fail_on_error: If True, raise exception on error-severity failures
        alert_on_failure: Send Slack alert on failures
        persist_results: Write results to monitoring table

    Returns:
        Dict with check results summary

    Raises:
        ValueError: If fail_on_error=True and any error-severity checks fail

    Example:
        >>> checks = [check_no_nulls("id"), check_no_duplicates(["id"])]
        >>> results = run_dq_checks(orders_df, checks, "sales.orders")
        >>> print(f"Passed: {len(results['passed'])}, Failed: {len(results['failed'])}")
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    check_id = str(uuid.uuid4())
    check_timestamp = datetime.now()

    # Cache DataFrame to avoid recomputation
    df.cache()
    row_count = df.count()

    results = {
        "check_id": check_id,
        "table": table_name,
        "timestamp": check_timestamp.isoformat(),
        "row_count": row_count,
        "passed": [],
        "failed": [],
        "warnings": [],
        "errors": [],
        "total_checks": len(checks),
        "total_execution_time_ms": 0
    }

    check_details = []

    for check in checks:
        start_time = time.time()
        passed = False
        error_msg = None

        try:
            passed = check.check_fn(df)
        except Exception as e:
            error_msg = str(e)
            passed = False

        execution_time_ms = (time.time() - start_time) * 1000
        results["total_execution_time_ms"] += execution_time_ms

        # Categorize result
        if passed:
            results["passed"].append(check.name)
        elif check.severity == "error":
            results["failed"].append(check.name)
            results["errors"].append({
                "check": check.name,
                "error": error_msg,
                "description": check.description
            })
        else:
            results["warnings"].append(check.name)

        # Store detail for persistence
        check_details.append({
            "check_id": check_id,
            "table_name": table_name,
            "check_name": check.name,
            "check_timestamp": check_timestamp,
            "passed": passed,
            "severity": check.severity,
            "row_count": row_count,
            "failure_details": error_msg,
            "execution_time_ms": round(execution_time_ms, 2),
            "description": check.description
        })

    # Uncache
    df.unpersist()

    # Persist results to monitoring table
    if persist_results:
        try:
            details_df = spark.createDataFrame(check_details)
            details_df.write.mode("append").saveAsTable("production.monitoring.dq_results")
        except Exception as e:
            print(f"Warning: Failed to persist DQ results: {e}")

    # Alert on failures
    if alert_on_failure and results["failed"]:
        try:
            from ..utils.alerting import alert_dq_failure
            alert_dq_failure(table_name, results["failed"])
        except Exception as e:
            print(f"Warning: Failed to send DQ alert: {e}")

    # Alert on warnings (lower priority)
    if alert_on_failure and results["warnings"] and not results["failed"]:
        try:
            from ..utils.alerting import send_slack_alert
            send_slack_alert(
                title=f"DQ Warnings: {table_name}",
                message=f"{len(results['warnings'])} warning checks for `{table_name}`",
                severity="warning",
                details={"warnings": results["warnings"]}
            )
        except Exception as e:
            print(f"Warning: Failed to send warning alert: {e}")

    # Summary stats
    results["pass_rate"] = round(
        len(results["passed"]) / len(checks) * 100, 2
    ) if checks else 100.0

    # Fail pipeline if configured and errors exist
    if fail_on_error and results["failed"]:
        raise ValueError(
            f"Data quality checks failed for {table_name}. "
            f"Failed checks: {results['failed']}. "
            f"Errors: {results['errors']}"
        )

    return results


def create_dq_check(
    name: str,
    check_fn: Callable[[DataFrame], bool],
    severity: str = "error",
    description: str = ""
) -> DQCheck:
    """
    Factory function to create a DQCheck.

    Args:
        name: Check name
        check_fn: Function that takes DataFrame and returns bool
        severity: "error" or "warning"
        description: Human-readable description

    Returns:
        DQCheck instance
    """
    return DQCheck(
        name=name,
        check_fn=check_fn,
        severity=severity,
        description=description
    )


def combine_checks(*check_lists: List[DQCheck]) -> List[DQCheck]:
    """
    Combine multiple lists of checks into one.

    Args:
        *check_lists: Variable number of check lists

    Returns:
        Combined list of checks
    """
    combined = []
    for check_list in check_lists:
        combined.extend(check_list)
    return combined
