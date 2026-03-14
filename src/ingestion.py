"""
Data ingestion utilities for the sales data pipeline.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SALES_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("order_date", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DoubleType(), nullable=False),
        StructField("discount", DoubleType(), nullable=True),
        StructField("region", StringType(), nullable=True),
    ]
)


def read_csv(spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
    """Read a CSV file into a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        path: Path to the CSV file or directory.
        schema: Optional schema to enforce. Uses SALES_SCHEMA when *None*.

    Returns:
        DataFrame with the CSV contents.
    """
    if schema is None:
        schema = SALES_SCHEMA

    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .schema(schema)
        .load(path)
    )


def read_json(spark: SparkSession, path: str) -> DataFrame:
    """Read a JSON file (or directory of JSON files) into a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        path: Path to the JSON file or directory.

    Returns:
        DataFrame with the JSON contents.
    """
    return spark.read.format("json").option("multiLine", "true").load(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table into a Spark DataFrame.

    Args:
        spark: Active SparkSession.
        path: Path to the Delta table directory.

    Returns:
        DataFrame backed by the Delta table.
    """
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a Spark DataFrame to a Delta table.

    Args:
        df: DataFrame to persist.
        path: Destination path for the Delta table.
        mode: Write mode – ``"overwrite"`` (default) or ``"append"``.
    """
    df.write.format("delta").mode(mode).save(path)


def write_delta_partitioned(
    df: DataFrame, path: str, partition_cols: list, mode: str = "overwrite"
) -> None:
    """Write a Spark DataFrame to a partitioned Delta table.

    Args:
        df: DataFrame to persist.
        path: Destination path for the Delta table.
        partition_cols: Column names used for partitioning.
        mode: Write mode – ``"overwrite"`` (default) or ``"append"``.
    """
    df.write.format("delta").mode(mode).partitionBy(*partition_cols).save(path)
