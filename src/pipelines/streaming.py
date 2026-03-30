"""
Streaming Pipeline Patterns for Databricks.

Provides robust streaming patterns with exactly-once semantics,
checkpointing, and health monitoring.

Usage:
    from pipelines.streaming import create_robust_stream

    query = create_robust_stream(
        source_topic="orders",
        target_table="bronze.orders_stream",
        checkpoint_path="/checkpoints/orders"
    )
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType
from typing import Dict, Optional, Callable
from datetime import datetime


def create_robust_stream(
    source_topic: str,
    target_table: str,
    checkpoint_path: str,
    schema: StructType = None,
    bootstrap_servers_secret: tuple = ("kafka", "bootstrap_servers"),
    starting_offsets: str = "earliest",
    trigger_interval: str = "1 minute",
    max_offsets_per_trigger: int = 100000,
    transform_fn: Callable[[DataFrame], DataFrame] = None
):
    """
    Create a production-ready streaming job.

    Features:
    - Exactly-once processing via checkpoints
    - Rate limiting to prevent overload
    - Graceful handling of data loss/compaction
    - Optional transformation function

    Args:
        source_topic: Kafka topic to consume
        target_table: Target Delta table
        checkpoint_path: Path for streaming checkpoints
        schema: Schema for parsing JSON values
        bootstrap_servers_secret: Tuple of (scope, key) for Kafka servers
        starting_offsets: "earliest" or "latest"
        trigger_interval: Processing trigger interval
        max_offsets_per_trigger: Rate limit per trigger
        transform_fn: Optional transformation function

    Returns:
        StreamingQuery object

    Example:
        >>> query = create_robust_stream(
        ...     source_topic="orders",
        ...     target_table="bronze.orders_stream",
        ...     checkpoint_path="/checkpoints/orders",
        ...     schema=order_schema
        ... )
        >>> query.awaitTermination()
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Get Kafka bootstrap servers from secrets
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        bootstrap_servers = dbutils.secrets.get(
            bootstrap_servers_secret[0],
            bootstrap_servers_secret[1]
        )
    except Exception as e:
        raise ValueError(f"Failed to get Kafka bootstrap servers: {e}")

    # Configure stream reader
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", source_topic) \
        .option("startingOffsets", starting_offsets) \
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger) \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse Kafka message
    if schema:
        parsed_df = stream_df \
            .select(
                col("key").cast("string").alias("_kafka_key"),
                from_json(col("value").cast("string"), schema).alias("data"),
                col("timestamp").alias("_kafka_timestamp"),
                col("partition").alias("_kafka_partition"),
                col("offset").alias("_kafka_offset")
            ) \
            .select("_kafka_key", "data.*", "_kafka_timestamp", "_kafka_partition", "_kafka_offset")
    else:
        # Just cast to string if no schema
        parsed_df = stream_df \
            .select(
                col("key").cast("string").alias("_kafka_key"),
                col("value").cast("string").alias("_kafka_value"),
                col("timestamp").alias("_kafka_timestamp"),
                col("partition").alias("_kafka_partition"),
                col("offset").alias("_kafka_offset")
            )

    # Add processing metadata
    processed_df = parsed_df.withColumn("_processed_at", current_timestamp())

    # Apply optional transformation
    if transform_fn:
        processed_df = transform_fn(processed_df)

    # Write stream with exactly-once semantics
    query = processed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime=trigger_interval) \
        .queryName(f"stream_{source_topic}") \
        .toTable(target_table)

    print(f"Started streaming query: stream_{source_topic}")
    print(f"  - Source: {source_topic}")
    print(f"  - Target: {target_table}")
    print(f"  - Checkpoint: {checkpoint_path}")

    return query


def create_file_stream(
    source_path: str,
    target_table: str,
    checkpoint_path: str,
    file_format: str = "json",
    schema: StructType = None,
    trigger_interval: str = "1 minute",
    max_files_per_trigger: int = 100,
    transform_fn: Callable[[DataFrame], DataFrame] = None
):
    """
    Create a file-based streaming job (Auto Loader pattern).

    Args:
        source_path: Path to watch for new files
        target_table: Target Delta table
        checkpoint_path: Path for streaming checkpoints
        file_format: File format (json, csv, parquet)
        schema: Schema for reading files (optional for schema inference)
        trigger_interval: Processing trigger interval
        max_files_per_trigger: Rate limit
        transform_fn: Optional transformation function

    Returns:
        StreamingQuery object
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Configure Auto Loader
    reader = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", file_format) \
        .option("cloudFiles.maxFilesPerTrigger", max_files_per_trigger) \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")

    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("cloudFiles.inferColumnTypes", "true")

    stream_df = reader.load(source_path)

    # Add metadata
    from pyspark.sql.functions import input_file_name
    processed_df = stream_df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_processed_at", current_timestamp())

    # Apply optional transformation
    if transform_fn:
        processed_df = transform_fn(processed_df)

    # Write stream
    query = processed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime=trigger_interval) \
        .queryName(f"file_stream_{target_table.replace('.', '_')}") \
        .toTable(target_table)

    return query


def monitor_streaming_queries() -> Dict:
    """
    Monitor all active streaming queries.

    Returns:
        Dict with query health metrics
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    active_streams = spark.streams.active

    results = {
        "timestamp": datetime.now().isoformat(),
        "active_queries": len(active_streams),
        "queries": []
    }

    for stream in active_streams:
        progress = stream.lastProgress

        query_info = {
            "name": stream.name,
            "id": str(stream.id),
            "is_active": stream.isActive,
            "status": str(stream.status)
        }

        if progress:
            query_info.update({
                "batch_id": progress.get("batchId"),
                "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                "batch_duration_ms": progress.get("batchDuration", 0),
                "num_input_rows": progress.get("numInputRows", 0)
            })

            # Check if falling behind
            input_rate = progress.get("inputRowsPerSecond", 0)
            process_rate = progress.get("processedRowsPerSecond", 0)

            if input_rate > 0 and process_rate > 0:
                if input_rate > process_rate * 1.5:
                    query_info["status"] = "FALLING_BEHIND"
                    query_info["lag_ratio"] = round(input_rate / process_rate, 2)

        results["queries"].append(query_info)

    # Alert if any queries are falling behind
    falling_behind = [q for q in results["queries"] if q.get("status") == "FALLING_BEHIND"]

    if falling_behind:
        try:
            from ..utils.alerting import send_slack_alert
            send_slack_alert(
                title="Streaming Queries Falling Behind",
                message=f"{len(falling_behind)} streaming queries are not keeping up with input rate",
                severity="warning",
                details={"queries": falling_behind}
            )
        except Exception as e:
            print(f"Warning: Failed to send alert: {e}")

    return results


def stop_all_streams(graceful: bool = True, timeout_seconds: int = 60):
    """
    Stop all active streaming queries.

    Args:
        graceful: If True, wait for current batch to complete
        timeout_seconds: Timeout for graceful shutdown
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    for stream in spark.streams.active:
        print(f"Stopping stream: {stream.name}")
        stream.stop()

        if graceful:
            stream.awaitTermination(timeout_seconds)

    print("All streams stopped")
