-- Monitoring Schema and Tables Setup
-- Run this script to create all monitoring infrastructure

-- Create monitoring schema
CREATE SCHEMA IF NOT EXISTS production.monitoring;

-- ============================================================================
-- JOB MONITORING TABLES
-- ============================================================================

-- Job execution history
CREATE TABLE IF NOT EXISTS production.monitoring.job_runs (
    run_id BIGINT,
    job_id BIGINT,
    job_name STRING,
    run_start_time TIMESTAMP,
    run_end_time TIMESTAMP,
    duration_minutes DOUBLE,
    status STRING,
    error_message STRING,
    trigger_type STRING,
    cluster_id STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(run_start_time))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 90 days'
);

-- Daily health metrics snapshot
CREATE TABLE IF NOT EXISTS production.monitoring.job_health_metrics (
    snapshot_date DATE,
    metrics_json STRING,
    failed_jobs_count INT,
    long_running_jobs_count INT,
    total_jobs_monitored INT,
    success_rate DOUBLE,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- ============================================================================
-- DATA QUALITY TABLES
-- ============================================================================

-- Data quality check results
CREATE TABLE IF NOT EXISTS production.monitoring.dq_results (
    check_id STRING,
    table_name STRING,
    check_name STRING,
    check_timestamp TIMESTAMP,
    passed BOOLEAN,
    severity STRING,
    row_count BIGINT,
    failure_details STRING,
    execution_time_ms DOUBLE,
    description STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(check_timestamp))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Schema change tracking
CREATE TABLE IF NOT EXISTS production.monitoring.schema_changes (
    table_name STRING,
    change_timestamp TIMESTAMP,
    has_changes BOOLEAN,
    is_compatible BOOLEAN,
    added_columns STRING,
    removed_columns STRING,
    type_changes STRING,
    error_message STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA;

-- ============================================================================
-- CLUSTER MONITORING TABLES
-- ============================================================================

-- Cluster usage snapshots
CREATE TABLE IF NOT EXISTS production.monitoring.cluster_usage (
    snapshot_timestamp TIMESTAMP,
    cluster_id STRING,
    cluster_name STRING,
    state STRING,
    cluster_source STRING,
    creator_user_name STRING,
    num_workers INT,
    driver_node_type STRING,
    worker_node_type STRING,
    autoscale_min INT,
    autoscale_max INT,
    auto_termination_minutes INT,
    uptime_minutes DOUBLE,
    owner_tag STRING,
    cost_center_tag STRING,
    team_tag STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(snapshot_timestamp))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- DATA FRESHNESS TABLES
-- ============================================================================

-- Data freshness tracking
CREATE TABLE IF NOT EXISTS production.monitoring.data_freshness (
    check_timestamp TIMESTAMP,
    table_name STRING,
    last_updated TIMESTAMP,
    hours_since_update DOUBLE,
    sla_hours DOUBLE,
    sla_breached BOOLEAN,
    tier STRING,
    error STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(check_timestamp));

-- Table SLA configuration
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

-- Insert sample SLA config
-- MERGE INTO production.monitoring.table_sla_config AS target
-- USING (
--     SELECT 'production.sales.orders' as table_name, 4 as sla_hours, 'critical' as tier, 'data-team' as owner, 'alerts-critical' as alert_channel, true as enabled
-- ) AS source
-- ON target.table_name = source.table_name
-- WHEN NOT MATCHED THEN INSERT *;

-- ============================================================================
-- ALERTING TABLES
-- ============================================================================

-- Alert history
CREATE TABLE IF NOT EXISTS production.monitoring.alerts_sent (
    alert_id STRING,
    alert_timestamp TIMESTAMP,
    alert_type STRING,
    severity STRING,
    title STRING,
    message STRING,
    details_json STRING,
    destination STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(alert_timestamp));

-- ============================================================================
-- QUERY PERFORMANCE TABLES
-- ============================================================================

-- Slow query snapshots
CREATE TABLE IF NOT EXISTS production.monitoring.slow_queries (
    snapshot_timestamp TIMESTAMP,
    user_name STRING,
    statement_type STRING,
    total_duration_ms BIGINT,
    rows_produced BIGINT,
    bytes_read BIGINT,
    statement_preview STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (date(snapshot_timestamp));

-- ============================================================================
-- DASHBOARD VIEWS
-- ============================================================================

-- Pipeline health summary
CREATE OR REPLACE VIEW production.monitoring.v_pipeline_health AS
SELECT
    date_trunc('day', run_start_time) as run_date,
    job_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate,
    ROUND(AVG(duration_minutes), 2) as avg_duration_mins,
    ROUND(MAX(duration_minutes), 2) as max_duration_mins
FROM production.monitoring.job_runs
WHERE run_start_time > current_date() - INTERVAL 30 DAYS
GROUP BY 1, 2;

-- Data quality summary
CREATE OR REPLACE VIEW production.monitoring.v_dq_summary AS
SELECT
    date_trunc('day', check_timestamp) as check_date,
    table_name,
    COUNT(*) as total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_checks,
    SUM(CASE WHEN NOT passed AND severity = 'error' THEN 1 ELSE 0 END) as critical_failures,
    ROUND(SUM(CASE WHEN passed THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pass_rate
FROM production.monitoring.dq_results
WHERE check_timestamp > current_date() - INTERVAL 30 DAYS
GROUP BY 1, 2;

-- Current freshness status
CREATE OR REPLACE VIEW production.monitoring.v_freshness_current AS
WITH latest AS (
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
    WHERE check_timestamp > current_timestamp() - INTERVAL 24 HOURS
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
FROM latest
WHERE rn = 1;
