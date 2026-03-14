"""
Utility functions shared across the sales data pipeline.
"""
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_not_null(df: DataFrame, columns: List[str]) -> DataFrame:
    """Remove rows that have a null value in any of *columns*.

    Args:
        df: Input DataFrame.
        columns: Column names that must not be null.

    Returns:
        Filtered DataFrame.
    """
    for col in columns:
        df = df.filter(F.col(col).isNotNull())
    return df


def deduplicate(df: DataFrame, subset: List[str] = None) -> DataFrame:
    """Drop duplicate rows, optionally considering only *subset* columns.

    Args:
        df: Input DataFrame.
        subset: Column names used for deduplication. When *None*, all columns
                are considered.

    Returns:
        DataFrame without duplicate rows.
    """
    return df.dropDuplicates(subset)


def rename_columns(df: DataFrame, mapping: dict) -> DataFrame:
    """Rename columns according to *mapping*.

    Args:
        df: Input DataFrame.
        mapping: ``{old_name: new_name}`` dictionary.

    Returns:
        DataFrame with renamed columns.
    """
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def add_ingestion_timestamp(df: DataFrame) -> DataFrame:
    """Add an ``ingestion_timestamp`` column set to the current UTC timestamp.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with the additional ``ingestion_timestamp`` column.
    """
    return df.withColumn("ingestion_timestamp", F.current_timestamp())


def get_row_count(df: DataFrame) -> int:
    """Return the number of rows in *df*.

    Args:
        df: Input DataFrame.

    Returns:
        Row count as an integer.
    """
    return df.count()


def log_dataframe_info(df: DataFrame, label: str = "DataFrame") -> None:
    """Print schema and row count to stdout (useful for pipeline debugging).

    Args:
        df: DataFrame to inspect.
        label: Human-readable name used in the printed output.
    """
    print(f"[{label}] Schema:")
    df.printSchema()
    print(f"[{label}] Row count: {df.count()}")
