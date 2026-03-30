-- Table SLA Configuration
-- Modify this file with your actual tables and SLA requirements

-- Clear existing config (optional)
-- TRUNCATE TABLE production.monitoring.table_sla_config;

-- Insert/Update SLA configurations
MERGE INTO production.monitoring.table_sla_config AS target
USING (
    SELECT * FROM (
        VALUES
        -- Critical tables - 4 hour SLA
        ('production.sales.orders', 4, 'critical', 'data-team', 'alerts-critical'),
        ('production.sales.transactions', 4, 'critical', 'data-team', 'alerts-critical'),
        ('production.sales.customers', 4, 'critical', 'data-team', 'alerts-critical'),
        ('production.inventory.stock_levels', 4, 'critical', 'data-team', 'alerts-critical'),

        -- Standard tables - 24 hour SLA
        ('production.marketing.campaigns', 24, 'standard', 'marketing-team', 'alerts-marketing'),
        ('production.marketing.email_events', 24, 'standard', 'marketing-team', 'alerts-marketing'),
        ('production.finance.invoices', 24, 'standard', 'finance-team', 'alerts-finance'),
        ('production.finance.payments', 24, 'standard', 'finance-team', 'alerts-finance'),
        ('production.hr.employees', 24, 'standard', 'hr-team', 'alerts-hr'),

        -- Low priority - 72 hour SLA
        ('production.analytics.user_segments', 72, 'low', 'analytics-team', 'alerts-standard'),
        ('production.analytics.cohort_analysis', 72, 'low', 'analytics-team', 'alerts-standard'),
        ('production.reporting.monthly_metrics', 72, 'low', 'bi-team', 'alerts-standard')
    ) AS t(table_name, sla_hours, tier, owner, alert_channel)
) AS source
ON target.table_name = source.table_name
WHEN MATCHED THEN UPDATE SET
    sla_hours = source.sla_hours,
    tier = source.tier,
    owner = source.owner,
    alert_channel = source.alert_channel,
    updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (table_name, sla_hours, tier, owner, alert_channel, enabled, updated_at)
VALUES (source.table_name, source.sla_hours, source.tier, source.owner, source.alert_channel, true, current_timestamp());

-- View current configuration
SELECT * FROM production.monitoring.table_sla_config ORDER BY tier, table_name;
