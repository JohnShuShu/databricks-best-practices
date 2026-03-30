"""
MERGE utilities for idempotent upserts in Databricks.

Provides robust upsert functionality with deduplication,
partition pruning, and SCD Type 2 support.

Usage:
    from pipelines.merge_utils import upsert_to_delta

    metrics = upsert_to_delta(
        source_df=new_orders,
        target_table="production.sales.orders",
        merge_keys=["order_id"]
    )
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, current_timestamp, current_date, lit
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

    Example:
        >>> metrics = upsert_to_delta(
        ...     source_df=orders_df,
        ...     target_table="sales.orders",
        ...     merge_keys=["order_id"],
        ...     partition_col="order_date"
        ... )
        >>> print(f"Inserted: {metrics['rows_inserted']}")
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

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

    # Step 3: Get Delta table
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
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

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
                metrics["rows_inserted"] = int(op_metrics.get("numTargetRowsInserted", 0))
                metrics["rows_updated"] = int(op_metrics.get("numTargetRowsUpdated", 0))
                metrics["rows_deleted"] = int(op_metrics.get("numTargetRowsDeleted", 0))
    except Exception as e:
        print(f"Warning: Could not retrieve merge metrics: {e}")

    print(f"Merge completed for {target_table}:")
    print(f"  - Source rows: {metrics['source_row_count']}")
    print(f"  - Duplicates removed: {metrics['duplicates_removed']}")
    print(f"  - Rows inserted: {metrics.get('rows_inserted', 'N/A')}")
    print(f"  - Rows updated: {metrics.get('rows_updated', 'N/A')}")
    print(f"  - Duration: {metrics['duration_seconds']:.2f}s")

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

    Example:
        >>> metrics = upsert_scd_type2(
        ...     source_df=customers_df,
        ...     target_table="dim.customers",
        ...     business_keys=["customer_id"],
        ...     tracked_columns=["name", "email", "address"]
        ... )
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    start_time = datetime.now()
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

    # Step 1: Close old records for changed entities
    target.alias("target").merge(
        staged_source.alias("source"),
        f"{key_condition} AND target.{current_flag_col} = true"
    ).whenMatchedUpdate(
        condition=change_condition,
        set={
            end_date_col: current_date(),
            current_flag_col: lit(False)
        }
    ).execute()

    # Step 2: Insert new versions for changed records + new records
    # Get records that were just closed
    closed_keys = spark.table(target_table) \
        .filter(f"{current_flag_col} = false AND {end_date_col} = current_date()") \
        .select(business_keys)

    # Find source records that match closed keys (changed) or don't exist (new)
    records_to_insert = staged_source.alias("source").join(
        spark.table(target_table).filter(f"{current_flag_col} = true").alias("existing"),
        on=business_keys,
        how="left_anti"
    )

    # Also include changed records
    changed_records = staged_source.alias("source").join(
        closed_keys.alias("closed"),
        on=business_keys,
        how="inner"
    ).select("source.*")

    # Union and insert
    all_new_records = records_to_insert.union(changed_records)

    if all_new_records.count() > 0:
        all_new_records.write.mode("append").saveAsTable(target_table)

    end_time = datetime.now()

    metrics = {
        "target_table": target_table,
        "started_at": start_time.isoformat(),
        "completed_at": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds(),
        "source_rows": source_df.count(),
        "new_versions_created": all_new_records.count()
    }

    print(f"SCD Type 2 merge completed for {target_table}")
    return metrics


def delete_by_keys(
    target_table: str,
    keys_to_delete: DataFrame,
    key_columns: List[str],
    soft_delete: bool = True
) -> Dict:
    """
    Delete records from Delta table by key.

    Args:
        target_table: Target Delta table
        keys_to_delete: DataFrame containing keys to delete
        key_columns: Columns that form the key
        soft_delete: If True, set is_deleted flag; if False, hard delete

    Returns:
        Dict with deletion metrics
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    target = DeltaTable.forName(spark, target_table)

    key_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])

    if soft_delete:
        target.alias("target").merge(
            keys_to_delete.alias("source"),
            key_condition
        ).whenMatchedUpdate(
            set={"is_deleted": lit(True), "deleted_at": current_timestamp()}
        ).execute()
    else:
        target.alias("target").merge(
            keys_to_delete.alias("source"),
            key_condition
        ).whenMatchedDelete().execute()

    # Get metrics
    history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()
    rows_affected = 0
    if history and history[0].operationMetrics:
        rows_affected = int(history[0].operationMetrics.get("numTargetRowsDeleted", 0))
        if soft_delete:
            rows_affected = int(history[0].operationMetrics.get("numTargetRowsUpdated", 0))

    return {
        "target_table": target_table,
        "rows_affected": rows_affected,
        "soft_delete": soft_delete
    }
