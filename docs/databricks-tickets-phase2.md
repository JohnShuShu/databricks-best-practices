# Phase 2: Data Quality - Kanban Tickets

> **Phase Goal:** Prevent bad data from propagating through pipelines
> **Timeline:** Recommended 2-3 sprints
> **Dependencies:** Phase 1 (monitoring schema, alerting)

---

## Ticket 7: Implement Data Quality Check Framework

**Title:** Create reusable data quality check framework with severity levels

**Type:** Feature

**Priority:** High

**Story Points:** 5

**Description:**
Create a flexible, reusable data quality framework that can be applied to any DataFrame. Support multiple check types, severity levels (error vs warning), result persistence, and integration with alerting.

**Acceptance Criteria:**
- [ ] Create `DQCheck` dataclass with name, check function, and severity
- [ ] Create `run_dq_checks()` function that executes checks against a DataFrame
- [ ] Support "error" severity (fails pipeline) and "warning" severity (alert only)
- [ ] Persist all check results to `monitoring.dq_results` table
- [ ] Integrate with Slack alerting for failures
- [ ] Create library of 10+ common reusable checks
- [ ] Support check parameterization (e.g., column names, thresholds)
- [ ] Return detailed results for debugging
- [ ] Add timing metrics per check

**Technical Details:**
```python
# data_quality.py - Add to shared utilities library
from dataclasses import dataclass
from typing import List, Callable, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
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
    fail_on_error: bool = True
) -> dict:
    """
    Run data quality checks against a DataFrame.

    Args:
        df: DataFrame to validate
        checks: List of DQCheck objects to run
        table_name: Name of table being checked (for logging)
        fail_on_error: If True, raise exception on error-severity failures

    Returns:
        Dict with check results summary
    """
    check_id = str(uuid.uuid4())
    check_timestamp = datetime.now()
    row_count = df.count()

    results = {
        "check_id": check_id,
        "table": table_name,
        "timestamp": check_timestamp.isoformat(),
        "row_count": row_count,
        "passed": [],
        "failed": [],
        "warnings": [],
        "errors": []
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

        # Categorize result
        if passed:
            results["passed"].append(check.name)
        elif check.severity == "error":
            results["failed"].append(check.name)
            results["errors"].append({"check": check.name, "error": error_msg})
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
            "execution_time_ms": execution_time_ms
        })

    # Persist results to monitoring table
    try:
        details_df = spark.createDataFrame(check_details)
        details_df.write.mode("append").saveAsTable("production.monitoring.dq_results")
    except Exception as e:
        print(f"Warning: Failed to persist DQ results: {e}")

    # Alert on failures
    if results["failed"]:
        alert_dq_failure(table_name, results["failed"])

    # Alert on warnings (separate channel or lower priority)
    if results["warnings"]:
        send_slack_alert(
            title=f"DQ Warnings: {table_name}",
            message=f"{len(results['warnings'])} warning checks for `{table_name}`",
            severity="warning",
            details={"warnings": results["warnings"]}
        )

    # Fail pipeline if configured and errors exist
    if fail_on_error and results["failed"]:
        raise ValueError(
            f"Data quality checks failed for {table_name}: {results['failed']}"
        )

    return results


# =============================================================================
# COMMON CHECK LIBRARY
# =============================================================================

def check_no_nulls(column: str) -> DQCheck:
    """Check that a column has no null values."""
    return DQCheck(
        name=f"no_nulls_{column}",
        check_fn=lambda df: df.filter(col(column).isNull()).count() == 0,
        severity="error",
        description=f"Column {column} must not contain null values"
    )


def check_no_duplicates(key_columns: List[str]) -> DQCheck:
    """Check that key columns have no duplicate combinations."""
    key_str = "_".join(key_columns)
    return DQCheck(
        name=f"no_duplicates_{key_str}",
        check_fn=lambda df: df.count() == df.dropDuplicates(key_columns).count(),
        severity="error",
        description=f"Columns {key_columns} must be unique"
    )


def check_not_empty() -> DQCheck:
    """Check that DataFrame is not empty."""
    return DQCheck(
        name="not_empty",
        check_fn=lambda df: df.count() > 0,
        severity="error",
        description="DataFrame must contain at least one row"
    )


def check_row_count_in_range(min_rows: int, max_rows: int) -> DQCheck:
    """Check that row count is within expected range."""
    return DQCheck(
        name=f"row_count_{min_rows}_to_{max_rows}",
        check_fn=lambda df: min_rows <= df.count() <= max_rows,
        severity="warning",
        description=f"Row count must be between {min_rows} and {max_rows}"
    )


def check_values_in_set(column: str, valid_values: set) -> DQCheck:
    """Check that all values in column are from allowed set."""
    return DQCheck(
        name=f"valid_values_{column}",
        check_fn=lambda df: df.filter(~col(column).isin(valid_values)).count() == 0,
        severity="error",
        description=f"Column {column} must only contain values from {valid_values}"
    )


def check_no_negative(column: str) -> DQCheck:
    """Check that numeric column has no negative values."""
    return DQCheck(
        name=f"no_negative_{column}",
        check_fn=lambda df: df.filter(col(column) < 0).count() == 0,
        severity="warning",
        description=f"Column {column} should not contain negative values"
    )


def check_date_not_future(column: str) -> DQCheck:
    """Check that date column doesn't have future dates."""
    return DQCheck(
        name=f"not_future_{column}",
        check_fn=lambda df: df.filter(col(column) > current_date()).count() == 0,
        severity="warning",
        description=f"Column {column} should not contain future dates"
    )


def check_date_not_too_old(column: str, days: int) -> DQCheck:
    """Check that dates are not older than specified days."""
    return DQCheck(
        name=f"not_older_than_{days}d_{column}",
        check_fn=lambda df: df.filter(
            col(column) < date_sub(current_date(), days)
        ).count() == 0,
        severity="warning",
        description=f"Column {column} should not be older than {days} days"
    )


def check_string_not_empty(column: str) -> DQCheck:
    """Check that string column has no empty strings."""
    return DQCheck(
        name=f"not_empty_string_{column}",
        check_fn=lambda df: df.filter(
            (col(column).isNull()) | (trim(col(column)) == "")
        ).count() == 0,
        severity="error",
        description=f"Column {column} must not be null or empty string"
    )


def check_regex_match(column: str, pattern: str, pattern_name: str) -> DQCheck:
    """Check that string column matches regex pattern."""
    return DQCheck(
        name=f"regex_{pattern_name}_{column}",
        check_fn=lambda df: df.filter(
            ~col(column).rlike(pattern)
        ).count() == 0,
        severity="error",
        description=f"Column {column} must match pattern {pattern_name}"
    )


def check_referential_integrity(
    column: str,
    ref_table: str,
    ref_column: str
) -> DQCheck:
    """Check that all values exist in reference table."""
    def check_fn(df):
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
        description=f"Column {column} must reference {ref_table}.{ref_column}"
    )


def check_freshness(timestamp_column: str, max_hours: int) -> DQCheck:
    """Check that most recent record is within time threshold."""
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
        description=f"Most recent {timestamp_column} must be within {max_hours} hours"
    )


def check_column_stats(
    column: str,
    min_val: float = None,
    max_val: float = None,
    avg_min: float = None,
    avg_max: float = None
) -> DQCheck:
    """Check that column statistics are within expected ranges."""
    def check_fn(df):
        stats = df.agg(
            min(col(column)).alias("min_val"),
            max(col(column)).alias("max_val"),
            avg(col(column)).alias("avg_val")
        ).collect()[0]

        if min_val is not None and stats.min_val < min_val:
            return False
        if max_val is not None and stats.max_val > max_val:
            return False
        if avg_min is not None and stats.avg_val < avg_min:
            return False
        if avg_max is not None and stats.avg_val > avg_max:
            return False
        return True

    return DQCheck(
        name=f"stats_check_{column}",
        check_fn=check_fn,
        severity="warning",
        description=f"Column {column} statistics must be within expected ranges"
    )


# =============================================================================
# CONVENIENCE FUNCTION FOR COMMON PATTERNS
# =============================================================================

def get_standard_checks(
    primary_key_columns: List[str],
    required_columns: List[str] = None,
    timestamp_column: str = None
) -> List[DQCheck]:
    """
    Generate standard checks for a typical table.

    Args:
        primary_key_columns: Columns that form the primary key
        required_columns: Additional columns that must not be null
        timestamp_column: Column to check for freshness

    Returns:
        List of DQCheck objects
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
        checks.append(check_freshness(timestamp_column, max_hours=24))

    return checks
```

**Usage Example:**
```python
# In your pipeline
from data_quality import run_dq_checks, get_standard_checks, check_no_negative

# Get standard checks + custom ones
checks = get_standard_checks(
    primary_key_columns=["order_id"],
    required_columns=["customer_id", "order_date"],
    timestamp_column="created_at"
)
checks.append(check_no_negative("amount"))
checks.append(check_values_in_set("status", {"pending", "completed", "cancelled"}))

# Run checks
results = run_dq_checks(
    df=orders_df,
    checks=checks,
    table_name="production.sales.orders"
)
```

**Blocked By:** Phase 1 (Tickets 4, 6)

**Blocks:** Ticket 11

**Labels:** `feature`, `data-quality`, `high-priority`

---

## Ticket 8: Create Idempotent MERGE Upsert Utility

**Title:** Create reusable MERGE utility for idempotent upserts

**Type:** Feature

**Priority:** High

**Story Points:** 3

**Description:**
Create a utility function that handles idempotent upserts using Delta Lake MERGE. The function should deduplicate source data, support partition pruning for performance, and handle schema evolution safely.

**Acceptance Criteria:**
- [ ] Deduplicate source data before merge (keep latest by timestamp)
- [ ] Build dynamic merge condition from key columns
- [ ] Support optional partition pruning for performance
- [ ] Return merge metrics (rows inserted, updated, deleted)
- [ ] Support soft deletes (optional)
- [ ] Handle schema evolution with mergeSchema option
- [ ] Log merge operations for debugging
- [ ] Unit tests with test data

**Technical Details:**
```python
# merge_utils.py - Add to shared utilities library
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from typing import List, Optional, Dict
from datetime import datetime

def upsert_to_delta(
    source_df: DataFrame,
    target_table: str,
    merge_keys: List[str],
    partition_col: Optional[str] = None,
    order_by_col: Optional[str] = "_ingested_at",
    enable_schema_evolution: bool = False,
    soft_delete: bool = False,
    delete_condition: Optional[str] = None
) -> Dict:
    """
    Perform idempotent upsert to Delta table using MERGE.

    Args:
        source_df: Source DataFrame with new/updated records
        target_table: Fully qualified target table name
        merge_keys: List of columns that form the unique key
        partition_col: Optional partition column for pruning
        order_by_col: Column to determine latest record (descending)
        enable_schema_evolution: Allow adding new columns
        soft_delete: If True, use soft delete pattern
        delete_condition: Optional condition for identifying deletes

    Returns:
        Dict with merge metrics
    """
    start_time = datetime.now()
    metrics = {
        "target_table": target_table,
        "merge_keys": merge_keys,
        "source_row_count": source_df.count(),
        "started_at": start_time.isoformat()
    }

    # Step 1: Deduplicate source data (keep latest per key)
    if order_by_col and order_by_col in source_df.columns:
        window = Window.partitionBy(merge_keys).orderBy(col(order_by_col).desc())
        deduped_df = source_df \
            .withColumn("_dedup_rank", row_number().over(window)) \
            .filter(col("_dedup_rank") == 1) \
            .drop("_dedup_rank")
    else:
        # No ordering column, just drop duplicates
        deduped_df = source_df.dropDuplicates(merge_keys)

    metrics["deduped_row_count"] = deduped_df.count()
    metrics["duplicates_removed"] = metrics["source_row_count"] - metrics["deduped_row_count"]

    # Step 2: Build merge condition
    merge_conditions = [f"target.{k} = source.{k}" for k in merge_keys]

    # Add partition pruning if applicable
    if partition_col and partition_col in deduped_df.columns:
        partitions = [row[0] for row in deduped_df.select(partition_col).distinct().collect()]

        if partitions:
            if isinstance(partitions[0], str):
                partition_list = ",".join([f"'{p}'" for p in partitions])
            else:
                partition_list = ",".join([str(p) for p in partitions])

            partition_filter = f"target.{partition_col} IN ({partition_list})"
            merge_conditions.insert(0, partition_filter)
            metrics["partitions_touched"] = len(partitions)

    merge_condition = " AND ".join(merge_conditions)

    # Step 3: Get or create Delta table
    target = DeltaTable.forName(spark, target_table)

    # Step 4: Build and execute merge
    merge_builder = target.alias("target").merge(
        deduped_df.alias("source"),
        merge_condition
    )

    # Handle soft delete pattern
    if soft_delete and delete_condition:
        merge_builder = merge_builder.whenMatchedUpdate(
            condition=delete_condition,
            set={"is_deleted": lit(True), "deleted_at": current_timestamp()}
        )

    # Standard update for matched records
    merge_builder = merge_builder.whenMatchedUpdateAll()

    # Insert new records
    if enable_schema_evolution:
        # With schema evolution
        merge_builder = merge_builder.whenNotMatchedInsertAll()
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    else:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    # Execute merge
    merge_builder.execute()

    # Step 5: Collect metrics
    end_time = datetime.now()
    metrics["completed_at"] = end_time.isoformat()
    metrics["duration_seconds"] = (end_time - start_time).total_seconds()

    # Get operation metrics from Delta history
    try:
        history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()
        if history:
            op_metrics = history[0].operationMetrics
            if op_metrics:
                metrics["rows_inserted"] = op_metrics.get("numTargetRowsInserted", 0)
                metrics["rows_updated"] = op_metrics.get("numTargetRowsUpdated", 0)
                metrics["rows_deleted"] = op_metrics.get("numTargetRowsDeleted", 0)
    except Exception as e:
        print(f"Warning: Could not retrieve merge metrics: {e}")

    print(f"Merge completed: {metrics}")
    return metrics


def upsert_scd_type2(
    source_df: DataFrame,
    target_table: str,
    business_keys: List[str],
    tracked_columns: List[str],
    effective_date_col: str = "effective_date",
    end_date_col: str = "end_date",
    current_flag_col: str = "is_current"
) -> Dict:
    """
    Perform SCD Type 2 upsert (maintain history of changes).

    Args:
        source_df: Source DataFrame with new/updated records
        target_table: Target Delta table
        business_keys: Columns that identify the business entity
        tracked_columns: Columns to track for changes
        effective_date_col: Column name for effective date
        end_date_col: Column name for end date
        current_flag_col: Column name for current record flag

    Returns:
        Dict with operation metrics
    """
    target = DeltaTable.forName(spark, target_table)

    # Build key condition
    key_condition = " AND ".join([f"target.{k} = source.{k}" for k in business_keys])

    # Build change detection condition
    change_conditions = [
        f"target.{c} <> source.{c}" for c in tracked_columns
    ]
    change_condition = " OR ".join(change_conditions)

    # Prepare source with SCD columns
    staged_source = source_df \
        .withColumn(effective_date_col, current_date()) \
        .withColumn(end_date_col, lit(None).cast("date")) \
        .withColumn(current_flag_col, lit(True))

    # Merge: close old records and insert new versions
    target.alias("target").merge(
        staged_source.alias("source"),
        key_condition
    ).whenMatchedUpdate(
        condition=f"target.{current_flag_col} = true AND ({change_condition})",
        set={
            end_date_col: current_date(),
            current_flag_col: lit(False)
        }
    ).whenNotMatchedInsertAll().execute()

    # Insert new versions for changed records
    # (This requires a second pass for true SCD2)
    changed_records = source_df.alias("source").join(
        spark.table(target_table).filter(col(current_flag_col) == False).alias("closed"),
        on=business_keys,
        how="inner"
    ).select("source.*")

    if changed_records.count() > 0:
        new_versions = changed_records \
            .withColumn(effective_date_col, current_date()) \
            .withColumn(end_date_col, lit(None).cast("date")) \
            .withColumn(current_flag_col, lit(True))

        new_versions.write.mode("append").saveAsTable(target_table)

    return {"status": "completed", "table": target_table}
```

**Usage Example:**
```python
from merge_utils import upsert_to_delta

# Simple upsert
metrics = upsert_to_delta(
    source_df=new_orders_df,
    target_table="production.sales.orders",
    merge_keys=["order_id"],
    partition_col="order_date",
    order_by_col="updated_at"
)

print(f"Inserted: {metrics.get('rows_inserted')}, Updated: {metrics.get('rows_updated')}")
```

**Blocked By:** None

**Blocks:** Ticket 10 (medallion templates use this)

**Labels:** `feature`, `data-quality`, `high-priority`

---

## Ticket 9: Implement Schema Evolution Handler

**Title:** Create safe schema evolution handler with change detection and alerts

**Type:** Feature

**Priority:** High

**Story Points:** 3

**Description:**
Create a utility that safely handles schema changes between source and target. Detect new columns (safe), removed columns (alert), and type changes (block). Enable controlled schema evolution with proper notifications.

**Acceptance Criteria:**
- [ ] Compare source and target schemas
- [ ] Detect new columns (allow with alert)
- [ ] Detect removed columns (alert, don't fail)
- [ ] Detect type changes (block and alert - breaking change)
- [ ] Return detailed schema diff report
- [ ] Integrate with Slack alerting
- [ ] Support dry-run mode (check without writing)
- [ ] Log schema changes to monitoring table

**Technical Details:**
```python
# schema_evolution.py - Add to shared utilities library
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField
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


def log_schema_change(table_name: str, result: SchemaComparisonResult):
    """Log schema change to monitoring table."""
    try:
        import json
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
```

**Usage Example:**
```python
from schema_evolution import write_with_schema_evolution, evolve_schema_safely

# Option 1: Check schema before processing
df, result = evolve_schema_safely(
    source_df=incoming_df,
    target_table="production.sales.orders",
    dry_run=True  # Just check, don't write
)

if result.is_compatible:
    # Safe to proceed
    df.write.mode("append").saveAsTable("production.sales.orders")

# Option 2: Write with automatic evolution
result = write_with_schema_evolution(
    source_df=incoming_df,
    target_table="production.sales.orders",
    mode="append",
    partition_by=["order_date"]
)
```

**Blocked By:** Phase 1 (Ticket 6 - alerting)

**Blocks:** None

**Labels:** `feature`, `data-quality`, `high-priority`

---

## Ticket 10: Create Data Freshness Monitor

**Title:** Create data freshness monitoring job with SLA tracking

**Type:** Feature

**Priority:** High

**Story Points:** 3

**Description:**
Create a job that monitors when tables were last updated and alerts when data becomes stale beyond configured SLA thresholds. Support per-table SLA configuration and track freshness metrics over time.

**Acceptance Criteria:**
- [ ] Check last update time for all configured tables
- [ ] Support per-table SLA configuration (hours)
- [ ] Alert when table exceeds SLA threshold
- [ ] Write freshness metrics to monitoring table
- [ ] Create dashboard view for freshness status
- [ ] Support table groups/tiers (critical, standard, low)
- [ ] Schedule to run every hour

**Technical Details:**
```python
# data_freshness_monitor.py
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pyspark.sql.functions import *

# SLA Configuration - move to a config table or file
TABLE_SLA_CONFIG = {
    # Critical tables - 4 hour SLA
    "production.sales.orders": {"sla_hours": 4, "tier": "critical"},
    "production.sales.transactions": {"sla_hours": 4, "tier": "critical"},

    # Standard tables - 24 hour SLA
    "production.marketing.campaigns": {"sla_hours": 24, "tier": "standard"},
    "production.finance.invoices": {"sla_hours": 24, "tier": "standard"},

    # Low priority - 72 hour SLA
    "production.analytics.user_segments": {"sla_hours": 72, "tier": "low"},
}

# Default SLA for tables not in config
DEFAULT_SLA_HOURS = 24


def get_table_freshness(table_name: str) -> Dict:
    """
    Get freshness information for a single table.

    Returns:
        Dict with table freshness details
    """
    result = {
        "table_name": table_name,
        "check_timestamp": datetime.now(),
        "last_updated": None,
        "hours_since_update": None,
        "sla_hours": None,
        "sla_breached": None,
        "error": None
    }

    # Get SLA config
    config = TABLE_SLA_CONFIG.get(table_name, {})
    result["sla_hours"] = config.get("sla_hours", DEFAULT_SLA_HOURS)
    result["tier"] = config.get("tier", "standard")

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
    schema: str = None
) -> List[Dict]:
    """
    Check freshness for multiple tables.

    Args:
        tables: Explicit list of tables to check
        catalog: If provided with schema, discover tables
        schema: If provided with catalog, discover tables

    Returns:
        List of freshness results
    """
    if tables is None:
        if catalog and schema:
            # Discover tables in schema
            discovered = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            tables = [f"{catalog}.{schema}.{t.tableName}" for t in discovered]
        else:
            # Use configured tables
            tables = list(TABLE_SLA_CONFIG.keys())

    results = []
    for table in tables:
        result = get_table_freshness(table)
        results.append(result)

    return results


def run_freshness_monitor():
    """
    Main entry point for freshness monitoring job.
    """
    # Check all configured tables
    results = check_all_table_freshness()

    # Separate by status
    breached = [r for r in results if r.get("sla_breached") == True]
    errors = [r for r in results if r.get("error") is not None]
    healthy = [r for r in results if r.get("sla_breached") == False and r.get("error") is None]

    # Write results to monitoring table
    results_df = spark.createDataFrame(results)
    results_df.write.mode("append").saveAsTable("production.monitoring.data_freshness")

    # Alert on SLA breaches
    if breached:
        critical_breaches = [b for b in breached if b.get("tier") == "critical"]
        standard_breaches = [b for b in breached if b.get("tier") != "critical"]

        if critical_breaches:
            send_slack_alert(
                title="CRITICAL: Data Freshness SLA Breach",
                message=f"{len(critical_breaches)} critical tables have breached SLA",
                severity="error",
                details={"tables": critical_breaches}
            )

        if standard_breaches:
            send_slack_alert(
                title="Data Freshness SLA Breach",
                message=f"{len(standard_breaches)} tables have breached SLA",
                severity="warning",
                details={"tables": standard_breaches}
            )

    # Alert on errors
    if errors:
        send_slack_alert(
            title="Data Freshness Check Errors",
            message=f"Failed to check freshness for {len(errors)} tables",
            severity="warning",
            details={"tables": errors}
        )

    # Summary
    summary = {
        "check_timestamp": datetime.now().isoformat(),
        "total_tables": len(results),
        "healthy": len(healthy),
        "breached": len(breached),
        "errors": len(errors)
    }

    print(f"Freshness check complete: {summary}")
    return summary


def get_freshness_dashboard_data() -> DataFrame:
    """
    Get data for freshness dashboard.
    """
    return spark.sql("""
        WITH latest_checks AS (
            SELECT
                table_name,
                check_timestamp,
                last_updated,
                hours_since_update,
                sla_hours,
                sla_breached,
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
            CASE
                WHEN sla_breached THEN 'BREACH'
                WHEN hours_since_update > sla_hours * 0.8 THEN 'WARNING'
                ELSE 'HEALTHY'
            END as status
        FROM latest_checks
        WHERE rn = 1
        ORDER BY sla_breached DESC, hours_since_update DESC
    """)
```

**SLA Configuration Table (Alternative):**
```sql
-- Create SLA config table for easier management
CREATE TABLE IF NOT EXISTS production.monitoring.table_sla_config (
    table_name STRING,
    sla_hours INT,
    tier STRING,
    owner STRING,
    alert_channel STRING,
    enabled BOOLEAN DEFAULT true,
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- Insert config
INSERT INTO production.monitoring.table_sla_config VALUES
('production.sales.orders', 4, 'critical', 'data-team', 'alerts-critical', true, current_timestamp()),
('production.sales.transactions', 4, 'critical', 'data-team', 'alerts-critical', true, current_timestamp()),
('production.marketing.campaigns', 24, 'standard', 'marketing-team', 'alerts-standard', true, current_timestamp());
```

**Blocked By:** Phase 1 (Tickets 4, 6)

**Blocks:** None

**Labels:** `feature`, `monitoring`, `high-priority`

---

## Ticket 11: Add Data Quality Checks to Existing Pipelines

**Title:** Integrate DQ framework into existing critical pipelines

**Type:** Task

**Priority:** High

**Story Points:** 5

**Description:**
Apply the data quality framework (Ticket 7) to existing critical pipelines. Start with 3-5 most important pipelines and add appropriate checks based on data characteristics and business requirements.

**Acceptance Criteria:**
- [ ] Identify top 5 critical pipelines for DQ integration
- [ ] Document expected data characteristics for each pipeline
- [ ] Add appropriate DQ checks (nulls, duplicates, ranges, etc.)
- [ ] Configure severity levels appropriately
- [ ] Test DQ checks with sample bad data
- [ ] Verify alerts fire correctly
- [ ] Document DQ checks for each pipeline
- [ ] Create runbook for handling DQ failures

**Technical Details:**
```python
# Example: Adding DQ to orders pipeline
# orders_pipeline.py

from data_quality import (
    run_dq_checks,
    check_no_nulls,
    check_no_duplicates,
    check_not_empty,
    check_values_in_set,
    check_no_negative,
    check_date_not_future,
    check_referential_integrity
)

def process_orders(source_df: DataFrame) -> DataFrame:
    """
    Process orders with data quality checks.
    """

    # Define checks for orders data
    orders_checks = [
        # Critical checks (error severity)
        check_not_empty(),
        check_no_nulls("order_id"),
        check_no_nulls("customer_id"),
        check_no_nulls("order_date"),
        check_no_duplicates(["order_id"]),

        # Business rule checks (error severity)
        check_values_in_set("status", {"pending", "processing", "shipped", "delivered", "cancelled"}),
        check_values_in_set("currency", {"USD", "EUR", "GBP", "CAD"}),

        # Referential integrity (error severity)
        check_referential_integrity("customer_id", "production.sales.customers", "customer_id"),

        # Warning checks
        check_no_negative("total_amount"),
        check_no_negative("quantity"),
        check_date_not_future("order_date"),
    ]

    # Run DQ checks - will fail pipeline on errors
    dq_results = run_dq_checks(
        df=source_df,
        checks=orders_checks,
        table_name="production.sales.orders",
        fail_on_error=True
    )

    # Log DQ summary
    print(f"DQ Results: {len(dq_results['passed'])} passed, "
          f"{len(dq_results['warnings'])} warnings, "
          f"{len(dq_results['failed'])} failed")

    # Continue with transformation
    transformed_df = source_df \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("order_year", year("order_date"))

    return transformed_df


# Example: Adding DQ to customer pipeline
def process_customers(source_df: DataFrame) -> DataFrame:
    """
    Process customers with data quality checks.
    """

    customer_checks = [
        check_not_empty(),
        check_no_nulls("customer_id"),
        check_no_duplicates(["customer_id"]),
        check_no_nulls("email"),
        check_regex_match("email", r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", "email_format"),
        check_values_in_set("status", {"active", "inactive", "suspended"}),
        check_values_in_set("country", get_valid_country_codes()),  # Your lookup
    ]

    run_dq_checks(
        df=source_df,
        checks=customer_checks,
        table_name="production.sales.customers",
        fail_on_error=True
    )

    return source_df
```

**Pipeline Inventory Template:**
| Pipeline | Table | Critical Checks | Warning Checks | Owner |
|----------|-------|-----------------|----------------|-------|
| Orders ETL | production.sales.orders | PK, nulls, status values, customer FK | amounts > 0, dates not future | data-team |
| Customers ETL | production.sales.customers | PK, nulls, email format | country codes | data-team |
| Transactions ETL | production.finance.transactions | PK, nulls, account FK | amounts balanced | finance-team |

**Blocked By:** Ticket 7 (DQ framework)

**Blocks:** None

**Labels:** `task`, `data-quality`, `high-priority`

---

## Phase 2 Summary

| Ticket | Title | Points | Dependencies |
|--------|-------|--------|--------------|
| 7 | Data Quality Framework | 5 | Phase 1 |
| 8 | Idempotent MERGE Utility | 3 | None |
| 9 | Schema Evolution Handler | 3 | Phase 1 (alerting) |
| 10 | Data Freshness Monitor | 3 | Phase 1 |
| 11 | Add DQ to Existing Pipelines | 5 | Ticket 7 |

**Total Story Points:** 19

**Recommended Sprint Breakdown:**
- **Sprint 3:** Tickets 7, 8, 9 (parallel development) - 11 points
- **Sprint 4:** Tickets 10, 11 (freshness + integration) - 8 points

**Definition of Done for Phase 2:**
- [ ] DQ framework created and documented
- [ ] At least 10 reusable DQ checks available
- [ ] MERGE utility tested and documented
- [ ] Schema evolution handling in place with alerts
- [ ] Data freshness monitor running hourly
- [ ] Top 5 critical pipelines have DQ checks
- [ ] DQ results visible in monitoring tables
- [ ] Alerts firing for DQ failures and SLA breaches
