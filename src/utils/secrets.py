"""
Secret management utilities for Databricks.

Usage:
    from utils.secrets import get_connection_config, get_jdbc_url

    # Get database credentials
    config = get_connection_config("databases", "postgres_prod")

    # Build JDBC URL
    url = get_jdbc_url("databases", "postgres_prod", "analytics")
"""

from typing import Dict, Optional


def get_dbutils():
    """Get dbutils object, works in both notebook and job contexts."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except ImportError:
        # Fallback for local development
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


def get_connection_config(scope: str, prefix: str) -> Dict[str, str]:
    """
    Retrieve connection config from secret scope.

    Args:
        scope: Secret scope name (e.g., 'databases')
        prefix: Key prefix (e.g., 'postgres_prod')

    Returns:
        Dict with host, port, username, password

    Example:
        >>> config = get_connection_config("databases", "postgres_prod")
        >>> config
        {'host': 'db.example.com', 'port': '5432', 'username': 'user', 'password': '***'}
    """
    dbutils = get_dbutils()

    return {
        "host": dbutils.secrets.get(scope, f"{prefix}_host"),
        "port": dbutils.secrets.get(scope, f"{prefix}_port"),
        "username": dbutils.secrets.get(scope, f"{prefix}_username"),
        "password": dbutils.secrets.get(scope, f"{prefix}_password"),
    }


def get_jdbc_url(
    scope: str,
    prefix: str,
    database: str,
    driver: str = "postgresql"
) -> str:
    """
    Build JDBC URL from secrets.

    Args:
        scope: Secret scope name
        prefix: Key prefix for credentials
        database: Database name
        driver: JDBC driver type (postgresql, mysql, sqlserver)

    Returns:
        Complete JDBC URL string

    Example:
        >>> url = get_jdbc_url("databases", "postgres_prod", "analytics")
        >>> url
        'jdbc:postgresql://db.example.com:5432/analytics'
    """
    config = get_connection_config(scope, prefix)

    driver_prefixes = {
        "postgresql": "jdbc:postgresql",
        "mysql": "jdbc:mysql",
        "sqlserver": "jdbc:sqlserver",
        "oracle": "jdbc:oracle:thin:@"
    }

    prefix_str = driver_prefixes.get(driver, f"jdbc:{driver}")

    if driver == "sqlserver":
        return f"{prefix_str}://{config['host']}:{config['port']};databaseName={database}"
    elif driver == "oracle":
        return f"{prefix_str}{config['host']}:{config['port']}/{database}"
    else:
        return f"{prefix_str}://{config['host']}:{config['port']}/{database}"


def get_secret(scope: str, key: str) -> str:
    """
    Get a single secret value.

    Args:
        scope: Secret scope name
        key: Secret key

    Returns:
        Secret value as string
    """
    dbutils = get_dbutils()
    return dbutils.secrets.get(scope, key)


def get_storage_credentials(scope: str, cloud: str = "azure") -> Dict[str, str]:
    """
    Get cloud storage credentials.

    Args:
        scope: Secret scope name
        cloud: Cloud provider (azure, aws, gcp)

    Returns:
        Dict with appropriate credentials for the cloud provider
    """
    dbutils = get_dbutils()

    if cloud == "azure":
        return {
            "storage_account": dbutils.secrets.get(scope, "storage_account"),
            "access_key": dbutils.secrets.get(scope, "access_key"),
            # Or for service principal:
            # "client_id": dbutils.secrets.get(scope, "client_id"),
            # "client_secret": dbutils.secrets.get(scope, "client_secret"),
            # "tenant_id": dbutils.secrets.get(scope, "tenant_id"),
        }
    elif cloud == "aws":
        return {
            "access_key_id": dbutils.secrets.get(scope, "aws_access_key_id"),
            "secret_access_key": dbutils.secrets.get(scope, "aws_secret_access_key"),
            # "session_token": dbutils.secrets.get(scope, "aws_session_token"),  # Optional
        }
    elif cloud == "gcp":
        return {
            "service_account_key": dbutils.secrets.get(scope, "gcp_service_account_key"),
        }
    else:
        raise ValueError(f"Unsupported cloud provider: {cloud}")


def get_api_key(scope: str, service: str) -> str:
    """
    Get API key for external service.

    Args:
        scope: Secret scope (typically 'apis')
        service: Service name (e.g., 'stripe', 'sendgrid')

    Returns:
        API key string
    """
    dbutils = get_dbutils()
    return dbutils.secrets.get(scope, f"{service}_api_key")
