# Databricks Platform Best Practices

A comprehensive guide and reusable code library for building robust, production-ready Databricks platforms. This repository contains battle-tested patterns for monitoring, data quality, ETL pipelines, cost optimization, and governance.

## Why This Exists

Organizations adopting Databricks often face common challenges:

- **Cost overruns** from zombie clusters and over-provisioned compute
- **Silent data failures** where jobs "succeed" but produce incorrect data
- **Pipeline fragility** from schema drift, missing retry logic, and hardcoded credentials
- **Governance gaps** with over-permissive access and no audit trails

This guide provides practical solutions with production-ready code you can adapt to your environment.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/JohnShuShu/databricks-platform-guide.git

# Copy utilities to your Databricks workspace
# Upload src/ directory to /Workspace/Shared/libs/ or package as a wheel
```

## Repository Structure

```
databricks-platform-guide/
├── src/
│   ├── monitoring/           # Platform observability
│   │   ├── job_health.py     # Job failure detection
│   │   ├── cluster_monitor.py # Zombie cluster detection
│   │   ├── data_freshness.py # SLA monitoring
│   │   └── query_performance.py
│   ├── data_quality/         # DQ framework
│   │   ├── framework.py      # Core DQ engine
│   │   ├── checks.py         # Reusable check library
│   │   └── schema_evolution.py
│   ├── pipelines/            # ETL patterns
│   │   ├── merge_utils.py    # Idempotent upserts
│   │   ├── medallion.py      # Bronze/Silver/Gold templates
│   │   └── streaming.py      # Streaming patterns
│   └── utils/                # Shared utilities
│       ├── alerting.py       # Slack/Teams integration
│       ├── secrets.py        # Secret management
│       └── retry.py          # Retry with backoff
├── config/
│   ├── cluster_policies/     # Cluster policy JSONs
│   └── table_sla_config.sql  # SLA configuration
├── tests/                    # Unit and integration tests
├── docs/                     # Additional documentation
└── README.md
```

## Key Components

### 1. Monitoring & Alerting

Detect issues before they impact downstream consumers.

```python
from monitoring.job_health import get_job_health_metrics
from monitoring.data_freshness import run_freshness_monitor
from utils.alerting import send_slack_alert

# Run daily health check
metrics = get_job_health_metrics(lookback_hours=24)

if metrics["failed_jobs"]:
    send_slack_alert(
        title="Job Failures Detected",
        message=f"{len(metrics['failed_jobs'])} jobs failed",
        severity="error"
    )
```

### 2. Data Quality Framework

Catch bad data before it propagates.

```python
from data_quality.framework import run_dq_checks
from data_quality.checks import (
    check_no_nulls,
    check_no_duplicates,
    check_values_in_set
)

checks = [
    check_no_nulls("order_id"),
    check_no_duplicates(["order_id"]),
    check_values_in_set("status", {"pending", "shipped", "delivered"})
]

# Fails pipeline if critical checks fail
run_dq_checks(df, checks, "production.sales.orders")
```

### 3. Idempotent ETL Patterns

Write pipelines that can safely re-run.

```python
from pipelines.merge_utils import upsert_to_delta

# Handles deduplication, partition pruning, and metrics
metrics = upsert_to_delta(
    source_df=new_orders,
    target_table="production.sales.orders",
    merge_keys=["order_id"],
    partition_col="order_date"
)
```

### 4. Schema Evolution

Handle upstream changes gracefully.

```python
from data_quality.schema_evolution import evolve_schema_safely

# Detects changes, alerts on drift, blocks breaking changes
df, result = evolve_schema_safely(
    source_df=incoming_data,
    target_table="production.sales.orders"
)
```

## Implementation Phases

We recommend a phased rollout:

| Phase | Focus | Tickets | Story Points |
|-------|-------|---------|--------------|
| 1 | Foundation | Unity Catalog, Cluster Policies, Secrets, Monitoring Schema, Alerting | 23 |
| 2 | Data Quality | DQ Framework, MERGE Utils, Schema Evolution, Freshness Monitor | 19 |
| 3 | Cost & Performance | Zombie Detection, Table Optimization, Query Analysis | 13 |
| 4 | Reliability | Retry Logic, Medallion Templates, Streaming Patterns | 15 |
| 5 | Testing & Docs | Unit Tests, Integration Tests, Onboarding Guides | 12 |

## Common Issues Addressed

### Compute & Cost
| Issue | Solution | Code |
|-------|----------|------|
| Zombie clusters | Automated detection + alerts | `cluster_monitor.py` |
| Over-provisioned clusters | Cluster policies with limits | `config/cluster_policies/` |
| Wrong cluster types | Enforce job clusters for jobs | Cluster policies |

### Data Quality
| Issue | Solution | Code |
|-------|----------|------|
| Duplicate records | Deduplication in MERGE | `merge_utils.py` |
| Schema drift | Change detection + alerts | `schema_evolution.py` |
| Silent failures | DQ checks with severity levels | `framework.py` |
| Stale data | SLA monitoring | `data_freshness.py` |

### Security & Governance
| Issue | Solution | Code |
|-------|----------|------|
| Hardcoded credentials | Secret scopes + utilities | `secrets.py` |
| Over-permissive access | Unity Catalog + least privilege | Documentation |
| No audit trail | Audit log analysis | `docs/governance.md` |

## Configuration

### Cluster Policies

Apply cost controls and standards:

```json
{
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 120,
    "defaultValue": 60
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 20
  }
}
```

### Table SLA Configuration

```sql
INSERT INTO monitoring.table_sla_config VALUES
('production.sales.orders', 4, 'critical'),
('production.marketing.campaigns', 24, 'standard');
```

## Requirements

- Databricks Runtime 13.0+
- Unity Catalog enabled (recommended)
- Python 3.9+
- Delta Lake

### Python Dependencies

```
databricks-sdk>=0.12.0
pyspark>=3.4.0
requests>=2.28.0
```

## Usage Examples

### Setting Up Monitoring

```python
# 1. Create monitoring schema
spark.sql("CREATE SCHEMA IF NOT EXISTS production.monitoring")

# 2. Create tables (see config/monitoring_tables.sql)

# 3. Schedule job health monitor (hourly)
# Use Databricks Jobs UI or API to schedule monitoring/job_health.py

# 4. Configure Slack webhook
# databricks secrets put --scope alerts --key slack_webhook
```

### Adding DQ to a Pipeline

```python
from data_quality.framework import run_dq_checks
from data_quality.checks import get_standard_checks

def process_orders(df):
    # Get standard checks for this table
    checks = get_standard_checks(
        primary_key_columns=["order_id"],
        required_columns=["customer_id", "order_date"],
        timestamp_column="created_at"
    )

    # Run checks - raises on failure
    run_dq_checks(df, checks, "production.sales.orders")

    # Continue processing...
    return df.withColumn("processed_at", current_timestamp())
```

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

Built from real-world experience enabling data teams on Databricks. Special thanks to the data engineering community for sharing patterns and anti-patterns.

---

**Found this useful?** Star the repo and share with your team!
