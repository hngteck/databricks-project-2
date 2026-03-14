"""
PySpark-based tests for src/transformation.py and src/utils.py.

A local SparkSession is created once per test session via the ``spark``
fixture so that all tests share the same JVM instance.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.transformation import (
    add_date_parts,
    add_revenue_columns,
    aggregate_sales_by_category,
    aggregate_sales_by_region,
    cast_and_clean,
    customer_summary,
    top_products_by_revenue,
)
from src.utils import (
    add_ingestion_timestamp,
    deduplicate,
    get_row_count,
    rename_columns,
    validate_not_null,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("region", StringType(), True),
    ]
)

SAMPLE_ROWS = [
    ("ORD-001", "2024-01-05", "C1", "P1", "Widget", "Electronics", 2, 50.0, 0.1, "North"),
    ("ORD-002", "2024-01-07", "C2", "P2", "Gadget", "Electronics", 1, 100.0, None, "South"),
    ("ORD-003", "2024-02-10", "C1", "P3", "Gizmo", "Clothing", 3, 20.0, 0.05, "East"),
    ("ORD-004", "2024-02-12", "C3", "P1", "Widget", "Electronics", -1, 50.0, 0.0, "West"),  # invalid qty
    ("ORD-005", "2024-03-01", "C2", "P2", "Gadget", "Electronics", 2, 0.0, 0.0, "South"),  # invalid price
    ("ORD-001", "2024-01-05", "C1", "P1", "Widget", "Electronics", 2, 50.0, 0.1, "North"),  # duplicate
]


@pytest.fixture(scope="module")
def df_raw(spark):
    return spark.createDataFrame(SAMPLE_ROWS, schema=RAW_SCHEMA)


@pytest.fixture(scope="module")
def df_clean(df_raw):
    return cast_and_clean(df_raw)


@pytest.fixture(scope="module")
def df_enriched(df_clean):
    df = add_revenue_columns(df_clean)
    return add_date_parts(df)


# ---------------------------------------------------------------------------
# cast_and_clean
# ---------------------------------------------------------------------------

class TestCastAndClean:
    def test_removes_nonpositive_quantity(self, df_clean):
        assert all(r.quantity > 0 for r in df_clean.select("quantity").collect())

    def test_removes_nonpositive_price(self, df_clean):
        assert all(r.unit_price > 0 for r in df_clean.select("unit_price").collect())

    def test_fills_null_discount(self, df_clean):
        nulls = df_clean.filter(F.col("discount").isNull()).count()
        assert nulls == 0

    def test_order_date_is_date_type(self, df_clean):
        field = next(f for f in df_clean.schema.fields if f.name == "order_date")
        assert isinstance(field.dataType, DateType)


# ---------------------------------------------------------------------------
# add_revenue_columns
# ---------------------------------------------------------------------------

class TestAddRevenueColumns:
    def test_gross_revenue_column_exists(self, df_enriched):
        assert "gross_revenue" in df_enriched.columns

    def test_net_revenue_column_exists(self, df_enriched):
        assert "net_revenue" in df_enriched.columns

    def test_gross_revenue_value(self, df_enriched):
        row = df_enriched.filter(F.col("order_id") == "ORD-001").first()
        assert abs(row.gross_revenue - 100.0) < 1e-9

    def test_net_revenue_applies_discount(self, df_enriched):
        row = df_enriched.filter(F.col("order_id") == "ORD-001").first()
        expected = 100.0 * (1.0 - 0.1)
        assert abs(row.net_revenue - expected) < 1e-9


# ---------------------------------------------------------------------------
# add_date_parts
# ---------------------------------------------------------------------------

class TestAddDateParts:
    def test_year_column_exists(self, df_enriched):
        assert "year" in df_enriched.columns

    def test_month_column_exists(self, df_enriched):
        assert "month" in df_enriched.columns

    def test_day_column_exists(self, df_enriched):
        assert "day" in df_enriched.columns

    def test_year_value(self, df_enriched):
        row = df_enriched.filter(F.col("order_id") == "ORD-001").first()
        assert row.year == 2024

    def test_month_value(self, df_enriched):
        row = df_enriched.filter(F.col("order_id") == "ORD-001").first()
        assert row.month == 1


# ---------------------------------------------------------------------------
# aggregate_sales_by_category
# ---------------------------------------------------------------------------

class TestAggregateSalesByCategory:
    def test_has_expected_columns(self, df_enriched):
        df = aggregate_sales_by_category(df_enriched)
        expected = {"year", "month", "category", "total_net_revenue", "total_orders", "avg_net_revenue"}
        assert expected.issubset(set(df.columns))

    def test_row_count_is_reasonable(self, df_enriched):
        df = aggregate_sales_by_category(df_enriched)
        assert df.count() > 0


# ---------------------------------------------------------------------------
# aggregate_sales_by_region
# ---------------------------------------------------------------------------

class TestAggregateSalesByRegion:
    def test_has_expected_columns(self, df_enriched):
        df = aggregate_sales_by_region(df_enriched)
        expected = {"region", "total_net_revenue", "total_orders", "avg_net_revenue"}
        assert expected.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# top_products_by_revenue
# ---------------------------------------------------------------------------

class TestTopProductsByRevenue:
    def test_returns_at_most_top_n(self, df_enriched):
        df = top_products_by_revenue(df_enriched, top_n=2)
        assert df.count() <= 2

    def test_has_rank_column(self, df_enriched):
        df = top_products_by_revenue(df_enriched, top_n=5)
        assert "rank" in df.columns


# ---------------------------------------------------------------------------
# customer_summary
# ---------------------------------------------------------------------------

class TestCustomerSummary:
    def test_has_expected_columns(self, df_enriched):
        df = customer_summary(df_enriched)
        expected = {
            "customer_id",
            "total_orders",
            "total_net_revenue",
            "avg_order_value",
            "first_order_date",
            "last_order_date",
        }
        assert expected.issubset(set(df.columns))

    def test_one_row_per_customer(self, df_enriched):
        df = customer_summary(df_enriched)
        customers = df_enriched.select("customer_id").distinct().count()
        assert df.count() == customers


# ---------------------------------------------------------------------------
# utils
# ---------------------------------------------------------------------------

class TestValidateNotNull:
    def test_removes_null_rows(self, spark):
        schema = StructType(
            [StructField("a", StringType(), True), StructField("b", StringType(), True)]
        )
        data = [("x", "y"), (None, "z"), ("p", None)]
        df = spark.createDataFrame(data, schema)
        result = validate_not_null(df, ["a", "b"])
        assert result.count() == 1

    def test_no_rows_removed_when_no_nulls(self, spark):
        schema = StructType([StructField("a", StringType(), True)])
        data = [("x",), ("y",)]
        df = spark.createDataFrame(data, schema)
        result = validate_not_null(df, ["a"])
        assert result.count() == 2


class TestDeduplicate:
    def test_removes_exact_duplicates(self, df_raw, spark):
        schema = StructType([StructField("id", StringType(), True)])
        data = [("1",), ("1",), ("2",)]
        df = spark.createDataFrame(data, schema)
        assert deduplicate(df).count() == 2

    def test_subset_dedup(self, spark):
        schema = StructType(
            [StructField("id", StringType(), True), StructField("val", IntegerType(), True)]
        )
        data = [("A", 1), ("A", 2), ("B", 1)]
        df = spark.createDataFrame(data, schema)
        assert deduplicate(df, subset=["id"]).count() == 2


class TestRenameColumns:
    def test_renames_column(self, spark):
        schema = StructType([StructField("old", StringType(), True)])
        df = spark.createDataFrame([("v",)], schema)
        result = rename_columns(df, {"old": "new"})
        assert "new" in result.columns
        assert "old" not in result.columns


class TestAddIngestionTimestamp:
    def test_column_added(self, spark):
        schema = StructType([StructField("a", StringType(), True)])
        df = spark.createDataFrame([("x",)], schema)
        result = add_ingestion_timestamp(df)
        assert "ingestion_timestamp" in result.columns


class TestGetRowCount:
    def test_correct_count(self, spark):
        schema = StructType([StructField("a", StringType(), True)])
        df = spark.createDataFrame([("x",), ("y",), ("z",)], schema)
        assert get_row_count(df) == 3
