"""
Schema Evolution Handler for Databricks.

Safely handles schema changes with detection, alerting, and controlled evolution.

Usage:
    from data_quality.schema_evolution import evolve_schema_safely

    df, result = evolve_schema_safely(
        source_df=incoming_data,
        target_table="production.sales.orders"
    )

    if result.is_compatible:
        # Safe to write
        df.write.mode("append").saveAsTable(target_table)
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SchemaChange:
    """Represents a single schema change."""
    change_type: str  # "added", "removed", "type_changed"
    column_name: str
    old_value: Optional[str]
    new_value: Optional[str]
    is_breaking: bool


@dataclass
class SchemaComparisonResult:
    """Result of schema comparison."""
    is_compatible: bool
    has_changes: bool
    added_columns: List[str]
    removed_columns: List[str]
    type_changes: List[Dict]
    changes: List[SchemaChange]
    error_message: Optional[str]


def compare_schemas(
    source_schema: StructType,
    target_schema: StructType
) -> SchemaComparisonResult:
    """
    Compare source and target schemas to detect changes.

    Args:
        source_schema: Schema of incoming data
        target_schema: Schema of existing target table

    Returns:
        SchemaComparisonResult with detailed change information
    """
    source_fields = {f.name: f for f in source_schema.fields}
    target_fields = {f.name: f for f in target_schema.fields}

    source_names = set(source_fields.keys())
    target_names = set(target_fields.keys())

    # Find changes
    added_columns = list(source_names - target_names)
    removed_columns = list(target_names - source_names)

    # Check for type changes in common columns
    type_changes = []
    common_columns = source_names & target_names

    for col_name in common_columns:
        source_type = str(source_fields[col_name].dataType)
        target_type = str(target_fields[col_name].dataType)

        if source_type != target_type:
            type_changes.append({
                "column": col_name,
                "source_type": source_type,
                "target_type": target_type
            })

    # Build change list
    changes = []

    for col in added_columns:
        changes.append(SchemaChange(
            change_type="added",
            column_name=col,
            old_value=None,
            new_value=str(source_fields[col].dataType),
            is_breaking=False
        ))

    for col in removed_columns:
        changes.append(SchemaChange(
            change_type="removed",
            column_name=col,
            old_value=str(target_fields[col].dataType),
            new_value=None,
            is_breaking=False  # Removed columns are usually not breaking
        ))

    for tc in type_changes:
        changes.append(SchemaChange(
            change_type="type_changed",
            column_name=tc["column"],
            old_value=tc["target_type"],
            new_value=tc["source_type"],
            is_breaking=True  # Type changes are breaking
        ))

    # Determine compatibility
    is_compatible = len(type_changes) == 0
    has_changes = len(changes) > 0

    error_message = None
    if type_changes:
        error_message = f"Breaking schema changes detected: {type_changes}"

    return SchemaComparisonResult(
        is_compatible=is_compatible,
        has_changes=has_changes,
        added_columns=added_columns,
        removed_columns=removed_columns,
        type_changes=type_changes,
        changes=changes,
        error_message=error_message
    )


def evolve_schema_safely(
    source_df: DataFrame,
    target_table: str,
    dry_run: bool = False,
    allow_type_changes: bool = False,
    alert_on_changes: bool = True
) -> Tuple[DataFrame, SchemaComparisonResult]:
    """
    Safely handle schema evolution with change detection and alerts.

    Args:
        source_df: DataFrame with potentially new schema
        target_table: Target Delta table name
        dry_run: If True, only report changes without writing
        allow_type_changes: If True, allow type changes (dangerous)
        alert_on_changes: Send alerts for schema changes

    Returns:
        Tuple of (processed DataFrame, SchemaComparisonResult)

    Raises:
        ValueError: If breaking changes detected and not allowed
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Get target schema
    target_df = spark.table(target_table)
    target_schema = target_df.schema
    source_schema = source_df.schema

    # Compare schemas
    result = compare_schemas(source_schema, target_schema)

    # Log schema comparison
    log_schema_change(target_table, result)

    # Handle no changes case
    if not result.has_changes:
        return source_df, result

    # Alert on changes
    if alert_on_changes and result.has_changes:
        alert_schema_changes(target_table, result)

    # Block on breaking changes (type changes)
    if result.type_changes and not allow_type_changes:
        raise ValueError(
            f"Breaking schema changes detected for {target_table}. "
            f"Type changes: {result.type_changes}. "
            f"Set allow_type_changes=True to override (dangerous)."
        )

    if dry_run:
        print(f"DRY RUN - Schema changes detected for {target_table}:")
        print(f"  Added columns: {result.added_columns}")
        print(f"  Removed columns: {result.removed_columns}")
        print(f"  Type changes: {result.type_changes}")
        return source_df, result

    # If we get here, changes are safe (new columns only)
    return source_df, result


def write_with_schema_evolution(
    source_df: DataFrame,
    target_table: str,
    mode: str = "append",
    partition_by: List[str] = None
) -> SchemaComparisonResult:
    """
    Write DataFrame with automatic schema evolution for safe changes.

    Args:
        source_df: DataFrame to write
        target_table: Target table name
        mode: Write mode ("append" or "overwrite")
        partition_by: Optional partition columns

    Returns:
        SchemaComparisonResult with change details
    """
    # Check schema compatibility
    processed_df, result = evolve_schema_safely(
        source_df=source_df,
        target_table=target_table,
        dry_run=False,
        allow_type_changes=False,
        alert_on_changes=True
    )

    # Build writer
    writer = processed_df.write.mode(mode)

    # Enable schema evolution if new columns
    if result.added_columns:
        writer = writer.option("mergeSchema", "true")

    # Add partitioning
    if partition_by:
        writer = writer.partitionBy(partition_by)

    # Write
    writer.saveAsTable(target_table)

    return result


def alert_schema_changes(table_name: str, result: SchemaComparisonResult):
    """Send Slack alert for schema changes."""
    try:
        from ..utils.alerting import send_slack_alert

        if result.type_changes:
            severity = "error"
            title = f"BREAKING Schema Change: {table_name}"
        elif result.removed_columns:
            severity = "warning"
            title = f"Schema Drift Detected: {table_name}"
        else:
            severity = "info"
            title = f"Schema Evolution: {table_name}"

        details = {
            "added_columns": result.added_columns,
            "removed_columns": result.removed_columns,
            "type_changes": result.type_changes
        }

        send_slack_alert(
            title=title,
            message=f"Schema changes detected for `{table_name}`",
            severity=severity,
            details=details
        )
    except Exception as e:
        print(f"Warning: Failed to send schema alert: {e}")


def log_schema_change(table_name: str, result: SchemaComparisonResult):
    """Log schema change to monitoring table."""
    try:
        from pyspark.sql import SparkSession
        import json

        spark = SparkSession.builder.getOrCreate()

        record = spark.createDataFrame([{
            "table_name": table_name,
            "change_timestamp": datetime.now(),
            "has_changes": result.has_changes,
            "is_compatible": result.is_compatible,
            "added_columns": json.dumps(result.added_columns),
            "removed_columns": json.dumps(result.removed_columns),
            "type_changes": json.dumps(result.type_changes),
            "error_message": result.error_message
        }])

        # Create table if not exists
        spark.sql("""
            CREATE TABLE IF NOT EXISTS production.monitoring.schema_changes (
                table_name STRING,
                change_timestamp TIMESTAMP,
                has_changes BOOLEAN,
                is_compatible BOOLEAN,
                added_columns STRING,
                removed_columns STRING,
                type_changes STRING,
                error_message STRING
            ) USING DELTA
        """)

        record.write.mode("append").saveAsTable("production.monitoring.schema_changes")
    except Exception as e:
        print(f"Warning: Failed to log schema change: {e}")


def get_schema_change_history(table_name: str, days: int = 30) -> DataFrame:
    """
    Get schema change history for a table.

    Args:
        table_name: Table to check history for
        days: Days to look back

    Returns:
        DataFrame with change history
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    return spark.sql(f"""
        SELECT *
        FROM production.monitoring.schema_changes
        WHERE table_name = '{table_name}'
        AND change_timestamp > current_timestamp() - INTERVAL {days} DAYS
        ORDER BY change_timestamp DESC
    """)
