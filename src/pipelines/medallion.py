"""
Medallion Architecture Templates for Databricks.

Provides templates for Bronze (raw), Silver (cleansed), and Gold (aggregated)
layers following Databricks best practices.

Usage:
    from pipelines.medallion import ingest_to_bronze, process_to_silver

    # Ingest raw data
    ingest_to_bronze(
        source_path="/mnt/landing/orders/",
        target_table="bronze.orders",
        source_system="salesforce"
    )

    # Process to silver
    process_to_silver(
        bronze_table="bronze.orders",
        silver_table="silver.orders",
        transform_fn=clean_orders
    )
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, to_json, struct,
    col, to_date, year, month
)
from typing import Callable, List, Optional, Dict
from datetime import datetime


def ingest_to_bronze(
    source_path: str,
    target_table: str,
    source_system: str,
    file_format: str = "json",
    read_options: Dict = None,
    partition_by: List[str] = None,
    add_ingestion_date_partition: bool = True
) -> Dict:
    """
    Ingest raw data to Bronze layer with metadata.

    Adds standard metadata columns:
    - _ingested_at: Timestamp of ingestion
    - _source_file: Original source file path
    - _source_system: Name of source system
    - _raw_data: Original data as JSON string (for recovery)

    Args:
        source_path: Path to source data
        target_table: Target Bronze table name
        source_system: Identifier for the source system
        file_format: Source file format (json, csv, parquet, etc.)
        read_options: Additional read options
        partition_by: Partition columns (in addition to date)
        add_ingestion_date_partition: Add _ingested_date partition

    Returns:
        Dict with ingestion metrics

    Example:
        >>> ingest_to_bronze(
        ...     source_path="/mnt/landing/orders/2024/01/",
        ...     target_table="bronze.orders",
        ...     source_system="shopify",
        ...     file_format="json"
        ... )
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    start_time = datetime.now()

    # Build reader
    reader = spark.read.format(file_format)

    if read_options:
        for key, value in read_options.items():
            reader = reader.option(key, value)

    # Read raw data
    raw_df = reader.load(source_path)

    # Add metadata columns
    bronze_df = raw_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_source_system", lit(source_system)) \
        .withColumn("_raw_data", to_json(struct([col(c) for c in raw_df.columns])))

    # Add ingestion date for partitioning
    if add_ingestion_date_partition:
        bronze_df = bronze_df.withColumn(
            "_ingested_date",
            to_date(col("_ingested_at"))
        )

    # Build partition list
    partitions = partition_by or []
    if add_ingestion_date_partition:
        partitions = ["_ingested_date"] + partitions

    # Write to Bronze
    writer = bronze_df.write \
        .mode("append") \
        .option("mergeSchema", "true")

    if partitions:
        writer = writer.partitionBy(partitions)

    writer.saveAsTable(target_table)

    end_time = datetime.now()

    metrics = {
        "target_table": target_table,
        "source_path": source_path,
        "source_system": source_system,
        "rows_ingested": bronze_df.count(),
        "started_at": start_time.isoformat(),
        "completed_at": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds()
    }

    print(f"Bronze ingestion completed for {target_table}:")
    print(f"  - Rows ingested: {metrics['rows_ingested']}")
    print(f"  - Duration: {metrics['duration_seconds']:.2f}s")

    return metrics


def process_to_silver(
    bronze_table: str,
    silver_table: str,
    transform_fn: Callable[[DataFrame], DataFrame],
    incremental: bool = True,
    watermark_column: str = "_ingested_at",
    dq_checks: List = None,
    partition_by: List[str] = None
) -> Dict:
    """
    Process Bronze data to Silver layer with transformations and DQ.

    Args:
        bronze_table: Source Bronze table
        silver_table: Target Silver table
        transform_fn: Function to transform Bronze data
        incremental: If True, only process new data
        watermark_column: Column for incremental processing
        dq_checks: Optional list of DQ checks to run
        partition_by: Partition columns for Silver table

    Returns:
        Dict with processing metrics

    Example:
        >>> def clean_orders(df):
        ...     return df.filter(col("order_id").isNotNull())
        ...
        >>> process_to_silver(
        ...     bronze_table="bronze.orders",
        ...     silver_table="silver.orders",
        ...     transform_fn=clean_orders
        ... )
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    start_time = datetime.now()

    # Get watermark for incremental processing
    if incremental:
        try:
            last_processed = spark.sql(f"""
                SELECT COALESCE(MAX(_processed_at), '1900-01-01') as last_ts
                FROM {silver_table}
            """).collect()[0].last_ts
        except Exception:
            # Table doesn't exist yet
            last_processed = '1900-01-01'

        bronze_df = spark.table(bronze_table) \
            .filter(col(watermark_column) > last_processed)
    else:
        bronze_df = spark.table(bronze_table)

    # Check if there's data to process
    row_count = bronze_df.count()
    if row_count == 0:
        print(f"No new data to process for {silver_table}")
        return {
            "target_table": silver_table,
            "rows_processed": 0,
            "status": "no_new_data"
        }

    # Apply transformations
    transformed_df = transform_fn(bronze_df)

    # Add processing metadata
    silver_df = transformed_df \
        .withColumn("_processed_at", current_timestamp())

    # Run DQ checks if provided
    if dq_checks:
        from ..data_quality.framework import run_dq_checks
        run_dq_checks(silver_df, dq_checks, silver_table)

    # Write to Silver
    writer = silver_df.write.mode("append")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.saveAsTable(silver_table)

    end_time = datetime.now()

    metrics = {
        "target_table": silver_table,
        "source_table": bronze_table,
        "rows_processed": row_count,
        "started_at": start_time.isoformat(),
        "completed_at": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds()
    }

    print(f"Silver processing completed for {silver_table}:")
    print(f"  - Rows processed: {metrics['rows_processed']}")
    print(f"  - Duration: {metrics['duration_seconds']:.2f}s")

    return metrics


def create_gold_aggregate(
    silver_table: str,
    gold_table: str,
    group_columns: List[str],
    aggregations: Dict[str, str],
    filters: str = None,
    partition_by: List[str] = None
) -> Dict:
    """
    Create Gold aggregate table from Silver data.

    Args:
        silver_table: Source Silver table
        gold_table: Target Gold table
        group_columns: Columns to group by
        aggregations: Dict of {output_col: agg_expression}
        filters: Optional WHERE clause filter
        partition_by: Partition columns

    Returns:
        Dict with creation metrics

    Example:
        >>> create_gold_aggregate(
        ...     silver_table="silver.orders",
        ...     gold_table="gold.daily_order_summary",
        ...     group_columns=["order_date", "region"],
        ...     aggregations={
        ...         "total_orders": "COUNT(*)",
        ...         "total_revenue": "SUM(amount)",
        ...         "avg_order_value": "AVG(amount)"
        ...     }
        ... )
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    start_time = datetime.now()

    # Build aggregation expressions
    agg_exprs = [f"{expr} as {name}" for name, expr in aggregations.items()]

    # Build query
    group_cols_str = ", ".join(group_columns)
    agg_cols_str = ", ".join(agg_exprs)

    query = f"""
        SELECT
            {group_cols_str},
            {agg_cols_str},
            current_timestamp() as _refreshed_at
        FROM {silver_table}
    """

    if filters:
        query += f" WHERE {filters}"

    query += f" GROUP BY {group_cols_str}"

    # Execute aggregation
    gold_df = spark.sql(query)

    # Write Gold table (replace for aggregates)
    writer = gold_df.write.mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.saveAsTable(gold_table)

    end_time = datetime.now()

    metrics = {
        "target_table": gold_table,
        "source_table": silver_table,
        "rows_created": gold_df.count(),
        "started_at": start_time.isoformat(),
        "completed_at": end_time.isoformat(),
        "duration_seconds": (end_time - start_time).total_seconds()
    }

    print(f"Gold aggregate created for {gold_table}:")
    print(f"  - Rows: {metrics['rows_created']}")
    print(f"  - Duration: {metrics['duration_seconds']:.2f}s")

    return metrics


def optimize_table(
    table_name: str,
    vacuum_hours: int = 168,
    analyze: bool = True
) -> Dict:
    """
    Optimize a Delta table (OPTIMIZE, VACUUM, ANALYZE).

    Args:
        table_name: Table to optimize
        vacuum_hours: Hours to retain for VACUUM
        analyze: Run ANALYZE for statistics

    Returns:
        Dict with optimization metrics
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    results = {"table": table_name}

    # Get table stats before
    detail_before = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    results["files_before"] = detail_before.numFiles
    results["size_gb_before"] = round(detail_before.sizeInBytes / (1024**3), 2)

    # OPTIMIZE
    spark.sql(f"OPTIMIZE {table_name}")

    # VACUUM
    spark.sql(f"VACUUM {table_name} RETAIN {vacuum_hours} HOURS")

    # ANALYZE
    if analyze:
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

    # Get table stats after
    detail_after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    results["files_after"] = detail_after.numFiles
    results["size_gb_after"] = round(detail_after.sizeInBytes / (1024**3), 2)

    print(f"Optimized {table_name}:")
    print(f"  - Files: {results['files_before']} -> {results['files_after']}")
    print(f"  - Size: {results['size_gb_before']} GB -> {results['size_gb_after']} GB")

    return results
