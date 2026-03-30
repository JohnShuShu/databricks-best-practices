"""
Unit tests for data quality framework.

Run with: pytest tests/test_data_quality.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("dq-unit-tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def sample_orders(spark):
    """Create sample orders DataFrame."""
    data = [
        (1, "C001", "2024-01-15", 100.00, "completed"),
        (2, "C002", "2024-01-16", 250.50, "pending"),
        (3, "C003", "2024-01-17", 75.25, "completed"),
        (4, "C001", "2024-01-18", 300.00, "shipped"),
    ]
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_with_nulls(spark):
    """Create orders DataFrame with null values."""
    data = [
        (1, "C001", "2024-01-15", 100.00, "completed"),
        (None, "C002", "2024-01-16", 250.50, "pending"),  # Null order_id
        (3, None, "2024-01-17", 75.25, "completed"),      # Null customer_id
    ]
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def orders_with_duplicates(spark):
    """Create orders DataFrame with duplicate keys."""
    data = [
        (1, "C001", "2024-01-15", 100.00, "completed"),
        (1, "C001", "2024-01-15", 100.00, "completed"),  # Duplicate
        (2, "C002", "2024-01-16", 250.50, "pending"),
    ]
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)


class TestNullChecks:
    """Tests for null value checks."""

    def test_no_nulls_passes(self, sample_orders):
        """Test that no_nulls check passes on clean data."""
        from src.data_quality.checks import check_no_nulls

        check = check_no_nulls("order_id")
        result = check.check_fn(sample_orders)

        assert result is True

    def test_no_nulls_fails(self, orders_with_nulls):
        """Test that no_nulls check fails when nulls present."""
        from src.data_quality.checks import check_no_nulls

        check = check_no_nulls("order_id")
        result = check.check_fn(orders_with_nulls)

        assert result is False

    def test_no_nulls_different_column(self, orders_with_nulls):
        """Test no_nulls on different columns."""
        from src.data_quality.checks import check_no_nulls

        # order_date has no nulls
        check = check_no_nulls("order_date")
        result = check.check_fn(orders_with_nulls)

        assert result is True


class TestDuplicateChecks:
    """Tests for duplicate detection checks."""

    def test_no_duplicates_passes(self, sample_orders):
        """Test that no_duplicates check passes on unique data."""
        from src.data_quality.checks import check_no_duplicates

        check = check_no_duplicates(["order_id"])
        result = check.check_fn(sample_orders)

        assert result is True

    def test_no_duplicates_fails(self, orders_with_duplicates):
        """Test that no_duplicates check fails when duplicates present."""
        from src.data_quality.checks import check_no_duplicates

        check = check_no_duplicates(["order_id"])
        result = check.check_fn(orders_with_duplicates)

        assert result is False

    def test_composite_key_duplicates(self, spark):
        """Test duplicate check with composite key."""
        from src.data_quality.checks import check_no_duplicates

        data = [
            (1, "A", 100),
            (1, "B", 200),  # Same order_id, different region - OK
            (2, "A", 150),
        ]
        df = spark.createDataFrame(data, ["order_id", "region", "amount"])

        # Single column - would fail
        check_single = check_no_duplicates(["order_id"])
        assert check_single.check_fn(df) is False

        # Composite key - should pass
        check_composite = check_no_duplicates(["order_id", "region"])
        assert check_composite.check_fn(df) is True


class TestValueChecks:
    """Tests for value validation checks."""

    def test_values_in_set_passes(self, sample_orders):
        """Test values_in_set passes with valid values."""
        from src.data_quality.checks import check_values_in_set

        valid_statuses = {"pending", "completed", "shipped", "cancelled"}
        check = check_values_in_set("status", valid_statuses)
        result = check.check_fn(sample_orders)

        assert result is True

    def test_values_in_set_fails(self, sample_orders):
        """Test values_in_set fails with invalid values."""
        from src.data_quality.checks import check_values_in_set

        # Missing "shipped" from valid set
        valid_statuses = {"pending", "completed", "cancelled"}
        check = check_values_in_set("status", valid_statuses)
        result = check.check_fn(sample_orders)

        assert result is False

    def test_no_negative_passes(self, sample_orders):
        """Test no_negative passes with positive values."""
        from src.data_quality.checks import check_no_negative

        check = check_no_negative("amount")
        result = check.check_fn(sample_orders)

        assert result is True

    def test_no_negative_fails(self, spark):
        """Test no_negative fails with negative values."""
        from src.data_quality.checks import check_no_negative

        data = [(1, 100.0), (2, -50.0), (3, 75.0)]
        df = spark.createDataFrame(data, ["id", "amount"])

        check = check_no_negative("amount")
        result = check.check_fn(df)

        assert result is False


class TestRowCountChecks:
    """Tests for row count checks."""

    def test_not_empty_passes(self, sample_orders):
        """Test not_empty passes on non-empty DataFrame."""
        from src.data_quality.checks import check_not_empty

        check = check_not_empty()
        result = check.check_fn(sample_orders)

        assert result is True

    def test_not_empty_fails(self, spark):
        """Test not_empty fails on empty DataFrame."""
        from src.data_quality.checks import check_not_empty

        empty_df = spark.createDataFrame([], StructType([
            StructField("id", IntegerType())
        ]))

        check = check_not_empty()
        result = check.check_fn(empty_df)

        assert result is False

    def test_row_count_in_range(self, sample_orders):
        """Test row_count_in_range check."""
        from src.data_quality.checks import check_row_count_in_range

        # Sample has 4 rows
        check_pass = check_row_count_in_range(1, 10)
        assert check_pass.check_fn(sample_orders) is True

        check_fail = check_row_count_in_range(10, 20)
        assert check_fail.check_fn(sample_orders) is False


class TestStandardChecks:
    """Tests for standard check generation."""

    def test_get_standard_checks(self, sample_orders):
        """Test standard checks generation."""
        from src.data_quality.checks import get_standard_checks

        checks = get_standard_checks(
            primary_key_columns=["order_id"],
            required_columns=["customer_id", "order_date"]
        )

        # Should include: not_empty, no_duplicates, 3 null checks
        assert len(checks) >= 4

        # All checks should pass on clean data
        for check in checks:
            assert check.check_fn(sample_orders) is True


class TestFramework:
    """Tests for the DQ framework execution."""

    def test_run_dq_checks_all_pass(self, sample_orders):
        """Test framework execution with all passing checks."""
        from src.data_quality.framework import run_dq_checks
        from src.data_quality.checks import check_not_empty, check_no_nulls

        checks = [
            check_not_empty(),
            check_no_nulls("order_id")
        ]

        # Don't persist or alert in tests
        results = run_dq_checks(
            sample_orders,
            checks,
            "test.orders",
            fail_on_error=False,
            alert_on_failure=False,
            persist_results=False
        )

        assert len(results["passed"]) == 2
        assert len(results["failed"]) == 0
        assert results["pass_rate"] == 100.0

    def test_run_dq_checks_with_failures(self, orders_with_nulls):
        """Test framework execution with failing checks."""
        from src.data_quality.framework import run_dq_checks
        from src.data_quality.checks import check_no_nulls

        checks = [
            check_no_nulls("order_id"),
            check_no_nulls("customer_id")
        ]

        results = run_dq_checks(
            orders_with_nulls,
            checks,
            "test.orders",
            fail_on_error=False,
            alert_on_failure=False,
            persist_results=False
        )

        assert len(results["failed"]) == 2
        assert results["pass_rate"] == 0.0

    def test_run_dq_checks_raises_on_error(self, orders_with_nulls):
        """Test framework raises exception when fail_on_error=True."""
        from src.data_quality.framework import run_dq_checks
        from src.data_quality.checks import check_no_nulls

        checks = [check_no_nulls("order_id")]

        with pytest.raises(ValueError) as exc_info:
            run_dq_checks(
                orders_with_nulls,
                checks,
                "test.orders",
                fail_on_error=True,
                alert_on_failure=False,
                persist_results=False
            )

        assert "Data quality checks failed" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
