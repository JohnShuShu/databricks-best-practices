# Databricks Platform Best Practices & Implementation Guide

> **Purpose:** Comprehensive checklist for building a robust Databricks platform
> **Owner:** Data Platform Team
> **Created:** 2026-03-30
> **Status:** Planning

---

## Table of Contents

1. [Common Issues to Address](#1-common-issues-to-address)
2. [Monitoring Jobs & Alerting](#2-monitoring-jobs--alerting)
3. [ETL Pipeline Best Practices](#3-etl-pipeline-best-practices)
4. [Streaming Pipeline Patterns](#4-streaming-pipeline-patterns)
5. [Governance & Security](#5-governance--security)
6. [Cost Optimization](#6-cost-optimization)
7. [Testing Framework](#7-testing-framework)
8. [Alerting Integration](#8-alerting-integration)
9. [Documentation & Onboarding](#9-documentation--onboarding)
10. [Metrics Dashboard](#10-metrics-dashboard)
11. [Implementation Checklist](#11-implementation-checklist)

---

## 1. Common Issues to Address

### 1.1 Cluster & Compute Problems

| Issue | Description | Impact | Priority |
|-------|-------------|--------|----------|
| **Over-provisioned clusters** | Engineers spin up large clusters "just in case" | Cost waste | High |
| **Zombie clusters** | Interactive clusters left running overnight/weekends | Cost waste | High |
| **Wrong cluster types** | Using all-purpose clusters for jobs (2-3x more expensive) | Cost waste | High |
| **No autoscaling** | Fixed-size clusters that can't handle variable workloads | Performance/Cost | Medium |
| **Driver OOM** | Collecting too much data to the driver node | Job failures | Medium |

### 1.2 Data Quality Issues

| Issue | Description | Impact | Priority |
|-------|-------------|--------|----------|
| **Schema drift** | Upstream sources change without notice | Pipeline failures | Critical |
| **Duplicate records** | No idempotent writes or deduplication logic | Data integrity | Critical |
| **Late-arriving data** | Pipelines don't handle out-of-order data | Data completeness | High |
| **Silent failures** | Jobs "succeed" but produce incorrect/incomplete data | Data integrity | Critical |
| **No data validation** | Bad data propagates downstream before detection | Data integrity | High |

### 1.3 Pipeline Reliability

| Issue | Description | Impact | Priority |
|-------|-------------|--------|----------|
| **No retry logic** | Transient failures cause full pipeline failures | Reliability | High |
| **Monolithic notebooks** | 2000+ line notebooks impossible to test/maintain | Maintainability | Medium |
| **Hardcoded paths/credentials** | Makes promotion across environments painful | Security/DevOps | High |
| **No dependency management** | Jobs run before upstream data is ready | Data integrity | High |
| **Checkpoint corruption** | Streaming jobs fail and can't recover | Reliability | High |

### 1.4 Governance & Security

| Issue | Description | Impact | Priority |
|-------|-------------|--------|----------|
| **Over-permissive access** | Everyone has admin or ALL PRIVILEGES | Security | Critical |
| **No data lineage** | Can't track data origin or changes | Compliance | High |
| **Secrets in code** | Credentials committed to repos or notebooks | Security | Critical |
| **No audit logging** | Can't answer "who accessed what when" | Compliance | High |

---

## 2. Monitoring Jobs & Alerting

### 2.1 Job Health Monitor

- [ ] **Ticket:** Create daily job health monitoring job
- **Description:** Monitor all job runs from last 24 hours, track failures, long-running jobs, and high-retry jobs
- **Acceptance Criteria:**
  - Captures failed jobs with error messages
  - Identifies jobs running > 2x average duration
  - Tracks jobs with excessive retries
  - Writes metrics to Delta table for historical tracking

```python
# Implementation Reference
from databricks.sdk import WorkspaceClient
from datetime import datetime, timedelta
import json

def get_job_health_metrics():
    w = WorkspaceClient()

    cutoff = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)

    metrics = {
        "failed_jobs": [],
        "long_running_jobs": [],
        "high_retry_jobs": [],
        "cluster_utilization": []
    }

    for job in w.jobs.list():
        runs = list(w.jobs.list_runs(job_id=job.job_id, start_time_from=cutoff))

        for run in runs:
            # Track failures
            if run.state.result_state == "FAILED":
                metrics["failed_jobs"].append({
                    "job_id": job.job_id,
                    "job_name": job.settings.name,
                    "run_id": run.run_id,
                    "error": run.state.state_message
                })

            # Track long-running (> 2x average)
            duration_mins = (run.end_time - run.start_time) / 60000 if run.end_time else 0
            if duration_mins > 120:  # Configure threshold
                metrics["long_running_jobs"].append({
                    "job_id": job.job_id,
                    "job_name": job.settings.name,
                    "duration_mins": duration_mins
                })

    return metrics

# Write to Delta table for historical tracking
df = spark.createDataFrame([get_job_health_metrics()])
df.write.mode("append").saveAsTable("monitoring.job_health_metrics")
```

---

### 2.2 Cluster Cost Monitor

- [ ] **Ticket:** Create cluster cost and zombie cluster monitor
- **Description:** Track cluster costs, identify idle/zombie clusters, alert on waste
- **Acceptance Criteria:**
  - Lists all running clusters with metadata
  - Identifies clusters running > 8 hours from UI
  - Calculates estimated cost per cluster
  - Sends alerts for zombie clusters

```python
# Implementation Reference
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import *

def monitor_cluster_costs():
    w = WorkspaceClient()

    clusters = []
    for cluster in w.clusters.list():
        info = w.clusters.get(cluster.cluster_id)

        clusters.append({
            "cluster_id": cluster.cluster_id,
            "cluster_name": cluster.cluster_name,
            "state": str(cluster.state),
            "cluster_source": str(cluster.cluster_source),  # JOB vs UI vs API
            "num_workers": info.num_workers or 0,
            "driver_node_type": info.driver_node_type_id,
            "worker_node_type": info.node_type_id,
            "autoscale_min": info.autoscale.min_workers if info.autoscale else None,
            "autoscale_max": info.autoscale.max_workers if info.autoscale else None,
            "auto_termination_mins": info.autotermination_minutes,
            "uptime_mins": (datetime.now().timestamp() * 1000 - info.start_time) / 60000 if info.start_time else 0
        })

    df = spark.createDataFrame(clusters)

    # Flag issues
    issues = df.filter(
        (col("state") == "RUNNING") &
        (col("cluster_source") == "UI") &
        (col("uptime_mins") > 480)  # 8 hours
    )

    return issues

# Alert on zombie clusters
zombies = monitor_cluster_costs()
if zombies.count() > 0:
    # Send to Slack/PagerDuty/email
    send_alert("Zombie clusters detected", zombies.toPandas().to_dict())
```

---

### 2.3 Data Freshness Monitor

- [ ] **Ticket:** Create data freshness/SLA monitoring job
- **Description:** Monitor when tables were last updated, alert on SLA breaches
- **Acceptance Criteria:**
  - Configurable SLA per table
  - Checks last update timestamp via Delta history
  - Alerts when tables exceed SLA threshold
  - Tracks freshness metrics over time

```python
# Implementation Reference
def check_data_freshness(catalog: str, schema: str):
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

    freshness_issues = []

    for table in tables:
        table_name = f"{catalog}.{schema}.{table.tableName}"

        try:
            # Get table history
            history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()

            if history:
                last_modified = history[0].timestamp
                hours_since_update = (datetime.now() - last_modified).total_seconds() / 3600

                # Configure SLAs per table
                sla_hours = get_table_sla(table_name)  # Your config

                if hours_since_update > sla_hours:
                    freshness_issues.append({
                        "table": table_name,
                        "last_updated": str(last_modified),
                        "hours_stale": hours_since_update,
                        "sla_hours": sla_hours
                    })
        except Exception as e:
            freshness_issues.append({
                "table": table_name,
                "error": str(e)
            })

    return freshness_issues
```

---

### 2.4 Query Performance Monitor

- [ ] **Ticket:** Create query performance monitoring and optimization alerts
- **Description:** Track slow queries, identify users/patterns causing performance issues
- **Acceptance Criteria:**
  - Queries system.query.history for slow queries (> 5 min)
  - Aggregates by user to identify problematic patterns
  - Identifies optimization opportunities
  - Writes to monitoring table for trending

```python
# Implementation Reference
def monitor_query_performance():
    # Query the system tables (requires Unity Catalog)
    slow_queries = spark.sql("""
        SELECT
            user_name,
            statement_type,
            total_duration_ms,
            rows_produced,
            bytes_read,
            statement_text,
            start_time
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL 24 HOURS
        AND total_duration_ms > 300000  -- 5 minutes
        ORDER BY total_duration_ms DESC
        LIMIT 100
    """)

    # Identify patterns
    problematic_patterns = spark.sql("""
        SELECT
            user_name,
            COUNT(*) as query_count,
            AVG(total_duration_ms) as avg_duration_ms,
            SUM(bytes_read) as total_bytes_read
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL 24 HOURS
        GROUP BY user_name
        HAVING AVG(total_duration_ms) > 60000
        ORDER BY total_bytes_read DESC
    """)

    return slow_queries, problematic_patterns
```

---

## 3. ETL Pipeline Best Practices

### 3.1 Idempotent Writes with MERGE

- [ ] **Ticket:** Create reusable MERGE upsert utility function
- **Description:** Implement idempotent upsert pattern that handles duplicates and late-arriving data
- **Acceptance Criteria:**
  - Deduplicates source data before merge
  - Supports partition pruning for performance
  - Handles schema evolution
  - Works with any Delta table

```python
# Implementation Reference
def upsert_to_delta(source_df, target_table: str, merge_keys: list, partition_col: str = None):
    """
    Idempotent upsert that handles duplicates and late-arriving data.
    """
    from delta.tables import DeltaTable

    # Deduplicate source data first
    window = Window.partitionBy(merge_keys).orderBy(col("_ingested_at").desc())
    deduped_df = source_df.withColumn("_rank", row_number().over(window)) \
                          .filter(col("_rank") == 1) \
                          .drop("_rank")

    # Build merge condition
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    # Add partition pruning if applicable
    if partition_col and partition_col in source_df.columns:
        partitions = [r[0] for r in deduped_df.select(partition_col).distinct().collect()]
        partition_filter = f"target.{partition_col} IN ({','.join(map(repr, partitions))})"
        merge_condition = f"({partition_filter}) AND ({merge_condition})"

    target = DeltaTable.forName(spark, target_table)

    target.alias("target").merge(
        deduped_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
```

---

### 3.2 Schema Evolution Handler

- [ ] **Ticket:** Create safe schema evolution utility with alerts
- **Description:** Compare schemas, handle safe evolution, alert on breaking changes
- **Acceptance Criteria:**
  - Detects new columns (safe to add)
  - Detects removed columns (alert)
  - Detects type changes (block and alert)
  - Enables mergeSchema for safe additions

```python
# Implementation Reference
def evolve_schema_safely(source_df, target_table: str):
    """
    Compare schemas and handle evolution with alerts.
    """
    from pyspark.sql.types import StructType

    target_schema = spark.table(target_table).schema
    source_schema = source_df.schema

    # Find new columns
    target_cols = {f.name: f for f in target_schema.fields}
    source_cols = {f.name: f for f in source_schema.fields}

    new_cols = set(source_cols.keys()) - set(target_cols.keys())
    removed_cols = set(target_cols.keys()) - set(source_cols.keys())

    # Type changes (dangerous)
    type_changes = []
    for col_name in set(source_cols.keys()) & set(target_cols.keys()):
        if source_cols[col_name].dataType != target_cols[col_name].dataType:
            type_changes.append({
                "column": col_name,
                "old_type": str(target_cols[col_name].dataType),
                "new_type": str(source_cols[col_name].dataType)
            })

    # Alert on breaking changes
    if type_changes:
        raise ValueError(f"Breaking schema changes detected: {type_changes}")

    if removed_cols:
        send_alert("Schema drift: columns removed", list(removed_cols))

    if new_cols:
        send_alert("Schema evolution: new columns", list(new_cols))
        # Enable schema evolution for safe additions
        return source_df.write \
            .option("mergeSchema", "true") \
            .mode("append") \
            .saveAsTable(target_table)

    return source_df.write.mode("append").saveAsTable(target_table)
```

---

### 3.3 Data Quality Framework

- [ ] **Ticket:** Create reusable data quality check framework
- **Description:** Implement DQ checks as reusable functions with severity levels
- **Acceptance Criteria:**
  - Supports error vs warning severity
  - Persists results to monitoring table
  - Fails pipeline on critical errors
  - Provides common check library

```python
# Implementation Reference
from dataclasses import dataclass
from typing import List, Callable
from pyspark.sql import DataFrame

@dataclass
class DQCheck:
    name: str
    check_fn: Callable[[DataFrame], bool]
    severity: str  # "error" | "warning"

def run_dq_checks(df: DataFrame, checks: List[DQCheck], table_name: str) -> dict:
    results = {
        "table": table_name,
        "timestamp": datetime.now().isoformat(),
        "row_count": df.count(),
        "passed": [],
        "failed": [],
        "warnings": []
    }

    for check in checks:
        try:
            passed = check.check_fn(df)
            if passed:
                results["passed"].append(check.name)
            elif check.severity == "error":
                results["failed"].append(check.name)
            else:
                results["warnings"].append(check.name)
        except Exception as e:
            results["failed"].append(f"{check.name}: {str(e)}")

    # Persist results
    spark.createDataFrame([results]).write.mode("append") \
         .saveAsTable("monitoring.dq_results")

    # Fail pipeline on errors
    if results["failed"]:
        raise ValueError(f"DQ checks failed: {results['failed']}")

    return results

# Example checks
common_checks = [
    DQCheck("no_nulls_in_pk", lambda df: df.filter(col("id").isNull()).count() == 0, "error"),
    DQCheck("no_duplicates", lambda df: df.count() == df.dropDuplicates(["id"]).count(), "error"),
    DQCheck("valid_dates", lambda df: df.filter(col("event_date") > current_date()).count() == 0, "warning"),
    DQCheck("row_count_threshold", lambda df: df.count() > 0, "error"),
    DQCheck("no_negative_amounts", lambda df: df.filter(col("amount") < 0).count() == 0, "warning"),
]
```

---

### 3.4 Retry Wrapper with Exponential Backoff

- [ ] **Ticket:** Create retry decorator for transient failure handling
- **Description:** Implement retry logic with exponential backoff for API calls and external systems
- **Acceptance Criteria:**
  - Configurable max attempts
  - Exponential backoff between retries
  - Configurable retryable exceptions
  - Logging of retry attempts

```python
# Implementation Reference
import time
from functools import wraps

def with_retry(max_attempts: int = 3, backoff_factor: float = 2.0,
               retryable_exceptions: tuple = (Exception,)):
    """
    Decorator for retry logic with exponential backoff.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        sleep_time = backoff_factor ** attempt
                        print(f"Attempt {attempt + 1} failed: {e}. Retrying in {sleep_time}s...")
                        time.sleep(sleep_time)
                    else:
                        print(f"All {max_attempts} attempts failed")

            raise last_exception
        return wrapper
    return decorator

# Usage
@with_retry(max_attempts=3, retryable_exceptions=(TimeoutError, ConnectionError))
def load_from_api(endpoint: str):
    # Your API call here
    pass
```

---

### 3.5 Medallion Architecture Templates

- [ ] **Ticket:** Create Bronze layer ingestion template
- [ ] **Ticket:** Create Silver layer transformation template
- [ ] **Ticket:** Create Gold layer aggregation template
- **Description:** Standardized templates for each medallion layer
- **Acceptance Criteria:**
  - Bronze: Raw ingestion with metadata columns
  - Silver: Incremental processing with DQ checks
  - Gold: Business aggregates with refresh tracking

```python
# Implementation Reference

# Bronze Layer - Raw ingestion with metadata
def ingest_to_bronze(source_path: str, target_table: str, source_system: str):
    raw_df = spark.read.format("json").load(source_path)  # or other format

    bronze_df = raw_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_source_system", lit(source_system)) \
        .withColumn("_raw_data", to_json(struct("*")))  # Preserve original

    bronze_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_ingested_date") \
        .saveAsTable(target_table)

# Silver Layer - Cleansed and conformed
def process_to_silver(bronze_table: str, silver_table: str, transform_fn: Callable):
    # Read only new data (incremental)
    last_processed = spark.sql(f"""
        SELECT COALESCE(MAX(_processed_at), '1900-01-01') as last_ts
        FROM {silver_table}
    """).collect()[0].last_ts

    bronze_df = spark.table(bronze_table) \
        .filter(col("_ingested_at") > last_processed)

    if bronze_df.count() == 0:
        print("No new data to process")
        return

    # Apply transformations
    silver_df = transform_fn(bronze_df) \
        .withColumn("_processed_at", current_timestamp())

    # Run DQ checks
    run_dq_checks(silver_df, common_checks, silver_table)

    # Write (use MERGE for updates)
    silver_df.write.mode("append").saveAsTable(silver_table)

# Gold Layer - Business aggregates
def create_gold_aggregate(silver_table: str, gold_table: str,
                          group_cols: list, agg_exprs: list):
    spark.sql(f"""
        CREATE OR REPLACE TABLE {gold_table} AS
        SELECT
            {', '.join(group_cols)},
            {', '.join(agg_exprs)},
            current_timestamp() as _refreshed_at
        FROM {silver_table}
        GROUP BY {', '.join(group_cols)}
    """)
```

---

## 4. Streaming Pipeline Patterns

### 4.1 Robust Streaming with Checkpoints

- [ ] **Ticket:** Create production-ready streaming job template
- **Description:** Streaming pattern with exactly-once semantics and proper error handling
- **Acceptance Criteria:**
  - Exactly-once processing via checkpoints
  - Rate limiting to prevent overload
  - Handles topic compaction gracefully
  - Trigger interval configuration

```python
# Implementation Reference
def create_robust_stream(source_topic: str, target_table: str, checkpoint_path: str):
    """
    Production-ready streaming job with proper error handling.
    """

    # Configure for exactly-once processing
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", dbutils.secrets.get("kafka", "bootstrap_servers")) \
        .option("subscribe", source_topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 100000)  # Rate limit \
        .option("failOnDataLoss", "false")  # Handle topic compaction \
        .load()

    # Parse and transform
    parsed_df = stream_df \
        .select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("key", "data.*", "kafka_timestamp") \
        .withColumn("_processed_at", current_timestamp())

    # Write with exactly-once semantics
    query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="1 minute") \
        .queryName(f"stream_{source_topic}") \
        .start(target_table)

    return query
```

---

### 4.2 Streaming Health Monitor

- [ ] **Ticket:** Create streaming query health monitor
- **Description:** Monitor active streams, detect backlog buildup, alert on issues
- **Acceptance Criteria:**
  - Lists all active streaming queries
  - Calculates input vs processed rate
  - Alerts when falling behind (input > 1.5x processed)
  - Tracks metrics over time

```python
# Implementation Reference
def monitor_streaming_queries():
    active_streams = spark.streams.active

    for stream in active_streams:
        progress = stream.lastProgress
        if progress:
            metrics = {
                "name": stream.name,
                "id": str(stream.id),
                "batch_id": progress.get("batchId"),
                "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                "batch_duration_ms": progress.get("batchDuration", 0),
            }

            # Alert if falling behind
            if metrics["input_rows_per_second"] > metrics["processed_rows_per_second"] * 1.5:
                send_alert(f"Stream {stream.name} falling behind", metrics)
```

---

## 5. Governance & Security

### 5.1 Unity Catalog Setup

- [ ] **Ticket:** Set up Unity Catalog hierarchy (catalogs, schemas)
- [ ] **Ticket:** Implement principle of least privilege access controls
- [ ] **Ticket:** Restrict analyst access to bronze/raw data
- **Description:** Proper catalog hierarchy and access controls
- **Acceptance Criteria:**
  - Separate catalogs per environment (prod/staging/dev)
  - Schemas per domain
  - Role-based access with least privilege
  - Analysts restricted from raw data

```sql
-- Implementation Reference

-- Create proper hierarchy
CREATE CATALOG IF NOT EXISTS production;
CREATE CATALOG IF NOT EXISTS development;
CREATE CATALOG IF NOT EXISTS staging;

-- Schema per domain
CREATE SCHEMA IF NOT EXISTS production.sales;
CREATE SCHEMA IF NOT EXISTS production.marketing;
CREATE SCHEMA IF NOT EXISTS production.finance;

-- Principle of least privilege
-- Data engineers get full access to their domain
GRANT ALL PRIVILEGES ON SCHEMA production.sales TO `sales-data-engineers`;

-- Analysts get read-only
GRANT USE CATALOG ON CATALOG production TO `analysts`;
GRANT USE SCHEMA ON SCHEMA production.sales TO `analysts`;
GRANT SELECT ON SCHEMA production.sales TO `analysts`;

-- Deny direct access to bronze (raw) data
REVOKE ALL PRIVILEGES ON SCHEMA production.bronze FROM `analysts`;
```

---

### 5.2 Secret Management

- [ ] **Ticket:** Set up Databricks secret scopes for all credentials
- [ ] **Ticket:** Create secret retrieval utility functions
- [ ] **Ticket:** Audit and remove hardcoded credentials from notebooks
- **Description:** Centralized secret management using Databricks secret scopes
- **Acceptance Criteria:**
  - All credentials in secret scopes
  - Utility functions for retrieval
  - No hardcoded credentials in code
  - Scopes per environment/system

```python
# Implementation Reference
def get_connection_config(scope: str, prefix: str) -> dict:
    """
    Retrieve connection config from secret scope.
    """
    return {
        "host": dbutils.secrets.get(scope, f"{prefix}_host"),
        "port": dbutils.secrets.get(scope, f"{prefix}_port"),
        "username": dbutils.secrets.get(scope, f"{prefix}_username"),
        "password": dbutils.secrets.get(scope, f"{prefix}_password"),
    }

# Usage
postgres_config = get_connection_config("databases", "postgres_prod")
```

---

### 5.3 Audit Log Analysis

- [ ] **Ticket:** Create audit log analysis job for security monitoring
- **Description:** Analyze Unity Catalog audit logs for suspicious activity
- **Acceptance Criteria:**
  - Detects unusual access patterns
  - Tracks failed access attempts
  - Identifies potential security issues
  - Generates daily security report

```python
# Implementation Reference
def analyze_audit_logs():
    # Unusual access patterns
    suspicious_activity = spark.sql("""
        SELECT
            user_identity.email as user_email,
            action_name,
            request_params.full_name_arg as resource,
            COUNT(*) as access_count,
            MIN(event_time) as first_access,
            MAX(event_time) as last_access
        FROM system.access.audit
        WHERE event_date > current_date() - INTERVAL 7 DAYS
        AND action_name IN ('getTable', 'commandSubmit', 'getPermissions')
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 100
        ORDER BY access_count DESC
    """)

    # Failed access attempts (potential security issues)
    failed_access = spark.sql("""
        SELECT
            user_identity.email,
            action_name,
            request_params,
            response.status_code,
            COUNT(*) as failure_count
        FROM system.access.audit
        WHERE event_date > current_date() - INTERVAL 1 DAY
        AND response.status_code >= 400
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(*) > 10
    """)

    return suspicious_activity, failed_access
```

---

## 6. Cost Optimization

### 6.1 Cluster Policy Enforcement

- [ ] **Ticket:** Create and enforce cluster policies
- **Description:** Policies to enforce cost controls and prevent waste
- **Acceptance Criteria:**
  - Required auto-termination (max 2 hours)
  - Required cost center tags
  - Worker count limits
  - Approved node types only

```json
// Implementation Reference - Cluster Policy JSON
{
  "spark_version": {
    "type": "regex",
    "pattern": "1[3-5]\\.[0-9]+\\.x-scala.*",
    "defaultValue": "14.3.x-scala2.12"
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 120,
    "defaultValue": 30
  },
  "custom_tags.CostCenter": {
    "type": "fixed",
    "value": "data-platform"
  },
  "custom_tags.Owner": {
    "type": "regex",
    "pattern": ".*@company\\.com"
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 20
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_D4ds_v5",
      "Standard_D8ds_v5",
      "Standard_D16ds_v5"
    ],
    "defaultValue": "Standard_D4ds_v5"
  },
  "driver_node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_D4ds_v5",
      "Standard_D8ds_v5"
    ]
  }
}
```

---

### 6.2 Table Optimization Jobs

- [ ] **Ticket:** Create nightly table optimization job
- **Description:** OPTIMIZE, VACUUM, and ANALYZE tables automatically
- **Acceptance Criteria:**
  - Identifies fragmented tables (many small files)
  - Runs OPTIMIZE on fragmented tables
  - Runs VACUUM with 7-day retention
  - Computes statistics for query optimization

```python
# Implementation Reference
def optimize_tables(catalog: str, schema: str):
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()

    for table in tables:
        table_name = f"{catalog}.{schema}.{table.tableName}"

        try:
            # Get table stats
            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            num_files = detail.numFiles
            size_gb = detail.sizeInBytes / (1024**3)

            # Optimize if fragmented (too many small files)
            if num_files > 100 and size_gb / num_files < 0.1:  # < 100MB per file
                print(f"Optimizing {table_name}: {num_files} files, {size_gb:.2f} GB")
                spark.sql(f"OPTIMIZE {table_name}")

            # Vacuum old versions (retain 7 days)
            spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")

            # Analyze for query optimization
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

        except Exception as e:
            print(f"Error optimizing {table_name}: {e}")
```

---

## 7. Testing Framework

### 7.1 Unit Testing Setup

- [ ] **Ticket:** Set up unit testing framework for transformations
- **Description:** pytest + chispa for DataFrame testing
- **Acceptance Criteria:**
  - Local Spark session for tests
  - DataFrame equality assertions
  - Test coverage for core transformations
  - CI/CD integration

```python
# Implementation Reference - tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality  # pip install chispa

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("unit-tests") \
        .getOrCreate()

def test_deduplication_logic(spark):
    # Arrange
    input_data = [
        (1, "2024-01-01", 100),
        (1, "2024-01-02", 150),  # Duplicate ID, newer date
        (2, "2024-01-01", 200),
    ]
    input_df = spark.createDataFrame(input_data, ["id", "date", "amount"])

    expected_data = [
        (1, "2024-01-02", 150),  # Keep latest
        (2, "2024-01-01", 200),
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "date", "amount"])

    # Act
    from src.transformations import deduplicate_by_latest
    result_df = deduplicate_by_latest(input_df, key_col="id", date_col="date")

    # Assert
    assert_df_equality(result_df, expected_df, ignore_row_order=True)

def test_null_handling(spark):
    input_data = [(1, None), (2, "value"), (3, "")]
    input_df = spark.createDataFrame(input_data, ["id", "field"])

    from src.transformations import clean_nulls
    result_df = clean_nulls(input_df, ["field"])

    # Verify nulls and empty strings are handled
    assert result_df.filter("field IS NULL OR field = ''").count() == 0
```

---

### 7.2 Integration Testing

- [ ] **Ticket:** Create integration test framework with test schema
- **Description:** End-to-end tests using isolated test data
- **Acceptance Criteria:**
  - Separate test schema for isolation
  - Test data generators
  - Full pipeline execution
  - Automatic cleanup

```python
# Implementation Reference - tests/integration/test_pipeline_e2e.py
def test_full_pipeline_e2e():
    """
    End-to-end test using test data in a separate schema.
    """
    test_schema = "test_integration"

    # Setup: Create test data
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")

    test_data = create_test_data()  # Your test data generator
    test_data.write.mode("overwrite").saveAsTable(f"{test_schema}.source_data")

    try:
        # Act: Run pipeline
        from src.pipelines import run_etl_pipeline
        run_etl_pipeline(
            source_table=f"{test_schema}.source_data",
            target_table=f"{test_schema}.target_data"
        )

        # Assert: Verify results
        result = spark.table(f"{test_schema}.target_data")

        assert result.count() > 0, "Pipeline produced no output"
        assert result.filter("id IS NULL").count() == 0, "Nulls in primary key"

        # Verify specific business logic
        assert result.filter("status = 'PROCESSED'").count() == test_data.count()

    finally:
        # Cleanup
        spark.sql(f"DROP SCHEMA {test_schema} CASCADE")
```

---

## 8. Alerting Integration

### 8.1 Slack/Teams Alert Function

- [ ] **Ticket:** Create Slack webhook alerting utility
- [ ] **Ticket:** Create Microsoft Teams alerting utility (if needed)
- **Description:** Reusable alerting functions for pipeline notifications
- **Acceptance Criteria:**
  - Configurable severity (info/warning/error)
  - Supports structured details
  - Uses secret scope for webhook URLs
  - Consistent formatting

```python
# Implementation Reference
import requests
import json

def send_slack_alert(webhook_url: str, title: str, message: str,
                     severity: str = "warning", details: dict = None):
    """
    Send alert to Slack channel.
    """
    color = {
        "info": "#36a64f",
        "warning": "#ff9900",
        "error": "#ff0000"
    }.get(severity, "#808080")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Alert: {title}"}
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": message}
        }
    ]

    if details:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```{json.dumps(details, indent=2)}```"}
        })

    payload = {
        "attachments": [{
            "color": color,
            "blocks": blocks
        }]
    }

    webhook_url = dbutils.secrets.get("alerts", "slack_webhook")
    response = requests.post(webhook_url, json=payload)
    response.raise_for_status()

# Usage in pipelines
try:
    run_etl_pipeline()
except Exception as e:
    send_slack_alert(
        webhook_url=SLACK_WEBHOOK,
        title="Pipeline Failure",
        message=f"ETL pipeline failed: {str(e)}",
        severity="error",
        details={"job_id": JOB_ID, "run_id": RUN_ID}
    )
    raise
```

---

## 9. Documentation & Onboarding

### 9.1 Pipeline Notebook Template

- [ ] **Ticket:** Create standardized notebook template for all pipelines
- **Description:** Self-documenting notebook template with standard sections
- **Acceptance Criteria:**
  - Metadata header (owner, schedule, SLA)
  - Overview and dependencies documented
  - Widget-based parameterization
  - Environment-aware configuration

```python
# Implementation Reference - Databricks notebook template

# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Template
# MAGIC
# MAGIC **Owner:** [Your Name]
# MAGIC **Last Updated:** [Date]
# MAGIC **Schedule:** [Cron expression]
# MAGIC
# MAGIC ## Overview
# MAGIC [Brief description of what this pipeline does]
# MAGIC
# MAGIC ## Dependencies
# MAGIC - Upstream: `catalog.schema.source_table`
# MAGIC - Downstream: `catalog.schema.target_table`
# MAGIC
# MAGIC ## SLA
# MAGIC - Must complete by: 6:00 AM UTC
# MAGIC - Typical duration: 15-30 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for parameterization
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("start_date", "", "Start Date (optional)")
dbutils.widgets.text("end_date", "", "End Date (optional)")

ENV = dbutils.widgets.get("environment")
CATALOG = f"{ENV}_catalog"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Logic

# COMMAND ----------

# Your pipeline code here, using the patterns above...
```

---

### 9.2 Onboarding Documentation

- [ ] **Ticket:** Create Databricks onboarding guide for new engineers
- [ ] **Ticket:** Create "common pitfalls" documentation
- [ ] **Ticket:** Create code review checklist for Databricks PRs
- **Description:** Documentation to accelerate engineer onboarding
- **Acceptance Criteria:**
  - Getting started guide
  - Common pitfalls and how to avoid them
  - Code review checklist
  - Links to templates and utilities

---

## 10. Metrics Dashboard

### 10.1 Dashboard SQL Views

- [ ] **Ticket:** Create monitoring schema and dashboard views
- **Description:** SQL views for platform monitoring dashboard
- **Acceptance Criteria:**
  - Pipeline health summary
  - Data quality summary
  - Cost by team breakdown
  - Historical trending

```sql
-- Implementation Reference

-- Create views for monitoring dashboard
CREATE OR REPLACE VIEW monitoring.pipeline_health AS
SELECT
    date_trunc('day', run_start_time) as run_date,
    job_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(AVG(duration_minutes), 2) as avg_duration_mins,
    ROUND(MAX(duration_minutes), 2) as max_duration_mins
FROM monitoring.job_runs
WHERE run_start_time > current_date() - INTERVAL 30 DAYS
GROUP BY 1, 2;

CREATE OR REPLACE VIEW monitoring.data_quality_summary AS
SELECT
    date_trunc('day', check_timestamp) as check_date,
    table_name,
    COUNT(*) as total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_checks,
    SUM(CASE WHEN NOT passed AND severity = 'error' THEN 1 ELSE 0 END) as critical_failures
FROM monitoring.dq_results
WHERE check_timestamp > current_date() - INTERVAL 30 DAYS
GROUP BY 1, 2;

CREATE OR REPLACE VIEW monitoring.cost_by_team AS
SELECT
    date_trunc('week', usage_date) as week,
    tags.team as team,
    ROUND(SUM(dbu_cost), 2) as total_dbu_cost,
    ROUND(SUM(compute_cost), 2) as total_compute_cost
FROM system.billing.usage
WHERE usage_date > current_date() - INTERVAL 90 DAYS
GROUP BY 1, 2;
```

---

## 11. Implementation Checklist

### Phase 1: Foundation (Critical)
| # | Task | Priority | Status |
|---|------|----------|--------|
| 1 | Set up Unity Catalog hierarchy | Critical | [ ] |
| 2 | Create and enforce cluster policies | Critical | [ ] |
| 3 | Set up secret scopes, remove hardcoded creds | Critical | [ ] |
| 4 | Create monitoring schema | Critical | [ ] |
| 5 | Implement job health monitor | Critical | [ ] |
| 6 | Create Slack alerting utility | Critical | [ ] |

### Phase 2: Data Quality (High)
| # | Task | Priority | Status |
|---|------|----------|--------|
| 7 | Implement DQ check framework | High | [ ] |
| 8 | Create idempotent MERGE utility | High | [ ] |
| 9 | Implement schema evolution handler | High | [ ] |
| 10 | Create data freshness monitor | High | [ ] |
| 11 | Add common DQ checks to existing pipelines | High | [ ] |

### Phase 3: Cost & Performance (High)
| # | Task | Priority | Status |
|---|------|----------|--------|
| 12 | Create zombie cluster monitor | High | [ ] |
| 13 | Create nightly table optimization job | High | [ ] |
| 14 | Create query performance monitor | High | [ ] |
| 15 | Implement cost dashboards | High | [ ] |

### Phase 4: Reliability (Medium)
| # | Task | Priority | Status |
|---|------|----------|--------|
| 16 | Create retry wrapper utility | Medium | [ ] |
| 17 | Create medallion layer templates | Medium | [ ] |
| 18 | Implement streaming health monitor | Medium | [ ] |
| 19 | Set up audit log analysis | Medium | [ ] |

### Phase 5: Testing & Documentation (Medium)
| # | Task | Priority | Status |
|---|------|----------|--------|
| 20 | Set up unit testing framework | Medium | [ ] |
| 21 | Create integration test framework | Medium | [ ] |
| 22 | Create notebook template | Medium | [ ] |
| 23 | Write onboarding documentation | Medium | [ ] |
| 24 | Create code review checklist | Medium | [ ] |

---

## Quick Reference

| Category | Must Have | Nice to Have |
|----------|-----------|--------------|
| **Compute** | Cluster policies, auto-termination, job clusters | Spot instances, instance pools |
| **Data Quality** | Null checks, duplicate detection, row counts | Statistical drift detection, ML-based anomaly detection |
| **Monitoring** | Job failure alerts, SLA tracking | Cost dashboards, query performance analysis |
| **Security** | Unity Catalog, secret scopes, least privilege | Row-level security, column masking |
| **Pipeline** | Idempotent writes, retry logic, checkpoints | CDC, streaming exactly-once |
| **Testing** | Unit tests for transforms | Integration tests, data contract tests |
| **Documentation** | README per project, notebook template | Self-service templates, runbooks |

---

## Notes

- This document should be updated as items are completed
- Each checkbox item can be converted to a Kanban ticket
- Code snippets are reference implementations - adapt to your specific environment
- Priority can be adjusted based on team capacity and immediate needs
