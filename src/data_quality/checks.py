"""
Reusable Data Quality Checks Library.

A collection of common data quality checks that can be parameterized
and combined for different tables.

Usage:
    from data_quality.checks import (
        check_no_nulls,
        check_no_duplicates,
        get_standard_checks
    )

    # Individual checks
    checks = [
        check_no_nulls("order_id"),
        check_no_duplicates(["order_id"]),
    ]

    # Or use standard checks
    checks = get_standard_checks(
        primary_key_columns=["order_id"],
        required_columns=["customer_id"]
    )
"""

from typing import List, Set, Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, current_date, date_sub, min, max, avg
from datetime import datetime

from .framework import DQCheck


# =============================================================================
# NULL CHECKS
# =============================================================================

def check_no_nulls(column: str) -> DQCheck:
    """
    Check that a column has no null values.

    Args:
        column: Column name to check

    Returns:
        DQCheck that fails if any nulls found
    """
    return DQCheck(
        name=f"no_nulls_{column}",
        check_fn=lambda df: df.filter(col(column).isNull()).count() == 0,
        severity="error",
        description=f"Column '{column}' must not contain null values"
    )


def check_string_not_empty(column: str) -> DQCheck:
    """
    Check that string column has no empty strings or nulls.

    Args:
        column: Column name to check

    Returns:
        DQCheck that fails if any nulls or empty strings found
    """
    return DQCheck(
        name=f"not_empty_string_{column}",
        check_fn=lambda df: df.filter(
            (col(column).isNull()) | (trim(col(column)) == "")
        ).count() == 0,
        severity="error",
        description=f"Column '{column}' must not be null or empty string"
    )


# =============================================================================
# UNIQUENESS CHECKS
# =============================================================================

def check_no_duplicates(key_columns: List[str]) -> DQCheck:
    """
    Check that key columns have no duplicate combinations.

    Args:
        key_columns: List of columns that should be unique together

    Returns:
        DQCheck that fails if duplicates found
    """
    key_str = "_".join(key_columns)
    return DQCheck(
        name=f"no_duplicates_{key_str}",
        check_fn=lambda df: df.count() == df.dropDuplicates(key_columns).count(),
        severity="error",
        description=f"Columns {key_columns} must have unique combinations"
    )


# =============================================================================
# ROW COUNT CHECKS
# =============================================================================

def check_not_empty() -> DQCheck:
    """
    Check that DataFrame is not empty.

    Returns:
        DQCheck that fails if DataFrame has zero rows
    """
    return DQCheck(
        name="not_empty",
        check_fn=lambda df: df.count() > 0,
        severity="error",
        description="DataFrame must contain at least one row"
    )


def check_row_count_in_range(min_rows: int, max_rows: int) -> DQCheck:
    """
    Check that row count is within expected range.

    Args:
        min_rows: Minimum expected rows
        max_rows: Maximum expected rows

    Returns:
        DQCheck that warns if count outside range
    """
    return DQCheck(
        name=f"row_count_{min_rows}_to_{max_rows}",
        check_fn=lambda df: min_rows <= df.count() <= max_rows,
        severity="warning",
        description=f"Row count must be between {min_rows} and {max_rows}"
    )


def check_row_count_minimum(min_rows: int) -> DQCheck:
    """
    Check that row count meets minimum threshold.

    Args:
        min_rows: Minimum expected rows

    Returns:
        DQCheck that fails if below minimum
    """
    return DQCheck(
        name=f"min_row_count_{min_rows}",
        check_fn=lambda df: df.count() >= min_rows,
        severity="warning",
        description=f"Row count must be at least {min_rows}"
    )


# =============================================================================
# VALUE CHECKS
# =============================================================================

def check_values_in_set(column: str, valid_values: Set) -> DQCheck:
    """
    Check that all values in column are from allowed set.

    Args:
        column: Column to check
        valid_values: Set of allowed values

    Returns:
        DQCheck that fails if invalid values found
    """
    return DQCheck(
        name=f"valid_values_{column}",
        check_fn=lambda df: df.filter(~col(column).isin(valid_values)).count() == 0,
        severity="error",
        description=f"Column '{column}' must only contain values from {valid_values}"
    )


def check_no_negative(column: str) -> DQCheck:
    """
    Check that numeric column has no negative values.

    Args:
        column: Numeric column to check

    Returns:
        DQCheck that warns if negatives found
    """
    return DQCheck(
        name=f"no_negative_{column}",
        check_fn=lambda df: df.filter(col(column) < 0).count() == 0,
        severity="warning",
        description=f"Column '{column}' should not contain negative values"
    )


def check_positive(column: str) -> DQCheck:
    """
    Check that numeric column has only positive values (> 0).

    Args:
        column: Numeric column to check

    Returns:
        DQCheck that fails if non-positive values found
    """
    return DQCheck(
        name=f"positive_{column}",
        check_fn=lambda df: df.filter(col(column) <= 0).count() == 0,
        severity="error",
        description=f"Column '{column}' must contain only positive values"
    )


def check_regex_match(column: str, pattern: str, pattern_name: str) -> DQCheck:
    """
    Check that string column matches regex pattern.

    Args:
        column: Column to check
        pattern: Regex pattern
        pattern_name: Human-readable name for the pattern

    Returns:
        DQCheck that fails if any values don't match
    """
    return DQCheck(
        name=f"regex_{pattern_name}_{column}",
        check_fn=lambda df: df.filter(
            col(column).isNotNull() & ~col(column).rlike(pattern)
        ).count() == 0,
        severity="error",
        description=f"Column '{column}' must match pattern '{pattern_name}'"
    )


# =============================================================================
# DATE CHECKS
# =============================================================================

def check_date_not_future(column: str) -> DQCheck:
    """
    Check that date column doesn't have future dates.

    Args:
        column: Date column to check

    Returns:
        DQCheck that warns if future dates found
    """
    return DQCheck(
        name=f"not_future_{column}",
        check_fn=lambda df: df.filter(col(column) > current_date()).count() == 0,
        severity="warning",
        description=f"Column '{column}' should not contain future dates"
    )


def check_date_not_too_old(column: str, days: int) -> DQCheck:
    """
    Check that dates are not older than specified days.

    Args:
        column: Date column to check
        days: Maximum age in days

    Returns:
        DQCheck that warns if dates too old
    """
    return DQCheck(
        name=f"not_older_than_{days}d_{column}",
        check_fn=lambda df: df.filter(
            col(column) < date_sub(current_date(), days)
        ).count() == 0,
        severity="warning",
        description=f"Column '{column}' should not be older than {days} days"
    )


def check_date_in_range(column: str, start_date: str, end_date: str) -> DQCheck:
    """
    Check that dates fall within a specific range.

    Args:
        column: Date column to check
        start_date: Start of valid range (YYYY-MM-DD)
        end_date: End of valid range (YYYY-MM-DD)

    Returns:
        DQCheck that fails if dates outside range
    """
    return DQCheck(
        name=f"date_range_{column}",
        check_fn=lambda df: df.filter(
            (col(column) < start_date) | (col(column) > end_date)
        ).count() == 0,
        severity="warning",
        description=f"Column '{column}' must be between {start_date} and {end_date}"
    )


# =============================================================================
# REFERENTIAL INTEGRITY CHECKS
# =============================================================================

def check_referential_integrity(
    column: str,
    ref_table: str,
    ref_column: str
) -> DQCheck:
    """
    Check that all values exist in reference table.

    Args:
        column: Column in source DataFrame
        ref_table: Reference table name
        ref_column: Column in reference table

    Returns:
        DQCheck that fails if orphan records found
    """
    def check_fn(df):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        ref_df = spark.table(ref_table).select(ref_column).distinct()
        orphans = df.join(
            ref_df,
            df[column] == ref_df[ref_column],
            "left_anti"
        )
        return orphans.count() == 0

    return DQCheck(
        name=f"ref_integrity_{column}_to_{ref_table}",
        check_fn=check_fn,
        severity="error",
        description=f"Column '{column}' must reference {ref_table}.{ref_column}"
    )


# =============================================================================
# FRESHNESS CHECKS
# =============================================================================

def check_freshness(timestamp_column: str, max_hours: int) -> DQCheck:
    """
    Check that most recent record is within time threshold.

    Args:
        timestamp_column: Timestamp column to check
        max_hours: Maximum hours since last record

    Returns:
        DQCheck that warns if data too stale
    """
    def check_fn(df):
        max_ts = df.agg(max(col(timestamp_column))).collect()[0][0]
        if max_ts is None:
            return False
        hours_old = (datetime.now() - max_ts).total_seconds() / 3600
        return hours_old <= max_hours

    return DQCheck(
        name=f"freshness_{max_hours}h_{timestamp_column}",
        check_fn=check_fn,
        severity="warning",
        description=f"Most recent '{timestamp_column}' must be within {max_hours} hours"
    )


# =============================================================================
# STATISTICAL CHECKS
# =============================================================================

def check_column_stats(
    column: str,
    min_val: float = None,
    max_val: float = None,
    avg_min: float = None,
    avg_max: float = None
) -> DQCheck:
    """
    Check that column statistics are within expected ranges.

    Args:
        column: Numeric column to check
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        avg_min: Minimum allowed average
        avg_max: Maximum allowed average

    Returns:
        DQCheck that warns if stats outside range
    """
    def check_fn(df):
        stats = df.agg(
            min(col(column)).alias("min_val"),
            max(col(column)).alias("max_val"),
            avg(col(column)).alias("avg_val")
        ).collect()[0]

        if min_val is not None and stats.min_val is not None and stats.min_val < min_val:
            return False
        if max_val is not None and stats.max_val is not None and stats.max_val > max_val:
            return False
        if avg_min is not None and stats.avg_val is not None and stats.avg_val < avg_min:
            return False
        if avg_max is not None and stats.avg_val is not None and stats.avg_val > avg_max:
            return False
        return True

    return DQCheck(
        name=f"stats_check_{column}",
        check_fn=check_fn,
        severity="warning",
        description=f"Column '{column}' statistics must be within expected ranges"
    )


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_standard_checks(
    primary_key_columns: List[str],
    required_columns: List[str] = None,
    timestamp_column: str = None,
    freshness_hours: int = 24
) -> List[DQCheck]:
    """
    Generate standard checks for a typical table.

    Args:
        primary_key_columns: Columns that form the primary key
        required_columns: Additional columns that must not be null
        timestamp_column: Column to check for freshness
        freshness_hours: Max hours for freshness check

    Returns:
        List of DQCheck objects

    Example:
        >>> checks = get_standard_checks(
        ...     primary_key_columns=["order_id"],
        ...     required_columns=["customer_id", "order_date"],
        ...     timestamp_column="created_at"
        ... )
    """
    checks = [
        check_not_empty(),
        check_no_duplicates(primary_key_columns),
    ]

    # Add null checks for primary key
    for pk_col in primary_key_columns:
        checks.append(check_no_nulls(pk_col))

    # Add null checks for required columns
    if required_columns:
        for req_col in required_columns:
            checks.append(check_no_nulls(req_col))

    # Add freshness check if timestamp provided
    if timestamp_column:
        checks.append(check_freshness(timestamp_column, max_hours=freshness_hours))

    return checks


def get_common_email_check(column: str) -> DQCheck:
    """Get check for valid email format."""
    return check_regex_match(
        column,
        r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        "email_format"
    )


def get_common_phone_check(column: str) -> DQCheck:
    """Get check for valid phone format (US)."""
    return check_regex_match(
        column,
        r"^\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$",
        "phone_format"
    )


def get_common_uuid_check(column: str) -> DQCheck:
    """Get check for valid UUID format."""
    return check_regex_match(
        column,
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        "uuid_format"
    )
