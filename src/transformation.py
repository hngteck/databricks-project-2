"""
Data transformation logic for the sales data pipeline.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def cast_and_clean(df: DataFrame) -> DataFrame:
    """Cast columns to their proper types and remove invalid rows.

    - Parses ``order_date`` as a ``DateType`` column named ``order_date``.
    - Fills ``null`` discount values with ``0.0``.
    - Drops rows where ``quantity`` or ``unit_price`` is non-positive.

    Args:
        df: Raw ingested DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    return (
        df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
    )


def add_revenue_columns(df: DataFrame) -> DataFrame:
    """Derive revenue columns from existing data.

    Adds:
    - ``gross_revenue``: ``quantity * unit_price``
    - ``net_revenue``:   ``gross_revenue * (1 - discount)``

    Args:
        df: DataFrame that has already been cleaned by :func:`cast_and_clean`.

    Returns:
        DataFrame with additional revenue columns.
    """
    return df.withColumn(
        "gross_revenue", F.col("quantity") * F.col("unit_price")
    ).withColumn(
        "net_revenue", F.col("gross_revenue") * (F.lit(1.0) - F.col("discount"))
    )


def add_date_parts(df: DataFrame) -> DataFrame:
    """Extract year, month, and day columns from ``order_date``.

    Args:
        df: DataFrame containing an ``order_date`` date column.

    Returns:
        DataFrame with ``year``, ``month``, and ``day`` integer columns added.
    """
    return (
        df.withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
        .withColumn("day", F.dayofmonth("order_date"))
    )


def aggregate_sales_by_category(df: DataFrame) -> DataFrame:
    """Aggregate net revenue and order count grouped by category and month.

    Args:
        df: Enriched DataFrame produced by :func:`add_revenue_columns` and
            :func:`add_date_parts`.

    Returns:
        Aggregated DataFrame with columns:
        ``year``, ``month``, ``category``,
        ``total_net_revenue``, ``total_orders``, ``avg_net_revenue``.
    """
    return df.groupBy("year", "month", "category").agg(
        F.sum("net_revenue").alias("total_net_revenue"),
        F.count("order_id").alias("total_orders"),
        F.avg("net_revenue").alias("avg_net_revenue"),
    )


def aggregate_sales_by_region(df: DataFrame) -> DataFrame:
    """Aggregate net revenue and order count grouped by region.

    Args:
        df: Enriched DataFrame produced by :func:`add_revenue_columns`.

    Returns:
        Aggregated DataFrame with columns:
        ``region``, ``total_net_revenue``, ``total_orders``, ``avg_net_revenue``.
    """
    return df.groupBy("region").agg(
        F.sum("net_revenue").alias("total_net_revenue"),
        F.count("order_id").alias("total_orders"),
        F.avg("net_revenue").alias("avg_net_revenue"),
    )


def top_products_by_revenue(df: DataFrame, top_n: int = 10) -> DataFrame:
    """Return the top *n* products ranked by total net revenue.

    Args:
        df: Enriched DataFrame produced by :func:`add_revenue_columns`.
        top_n: Number of top products to return (default: 10).

    Returns:
        DataFrame with columns: ``product_id``, ``product_name``,
        ``total_net_revenue``, ``total_orders``, ``rank``.
    """
    product_agg = df.groupBy("product_id", "product_name").agg(
        F.sum("net_revenue").alias("total_net_revenue"),
        F.count("order_id").alias("total_orders"),
    )
    window = Window.orderBy(F.desc("total_net_revenue"))
    return (
        product_agg.withColumn("rank", F.rank().over(window)).filter(
            F.col("rank") <= top_n
        )
    )


def customer_summary(df: DataFrame) -> DataFrame:
    """Summarise purchasing behaviour per customer.

    Args:
        df: Enriched DataFrame produced by :func:`add_revenue_columns`.

    Returns:
        DataFrame with columns: ``customer_id``, ``total_orders``,
        ``total_net_revenue``, ``avg_order_value``,
        ``first_order_date``, ``last_order_date``.
    """
    return df.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("net_revenue").alias("total_net_revenue"),
        F.avg("net_revenue").alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
