# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Data Transformation
# MAGIC
# MAGIC This notebook reads from the bronze Delta layer, applies cleaning and enrichment
# MAGIC transformations, and writes the results to the **silver** layer.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Delta table at `storage.bronze_path/sales`
# MAGIC
# MAGIC **Outputs**
# MAGIC - Cleaned and enriched Delta table at `storage.silver_path/sales`
# MAGIC - Category-level aggregation at `storage.silver_path/sales_by_category`
# MAGIC - Region-level aggregation at `storage.silver_path/sales_by_region`

# COMMAND ----------

import sys
sys.path.insert(0, "../")

from src.config import load_config
from src.ingestion import read_delta, write_delta, write_delta_partitioned
from src.transformation import (
    cast_and_clean,
    add_revenue_columns,
    add_date_parts,
    aggregate_sales_by_category,
    aggregate_sales_by_region,
)
from src.utils import deduplicate, log_dataframe_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration

# COMMAND ----------

config = load_config("../config/settings.yaml")

BRONZE_DELTA_PATH   = config["storage"]["bronze_path"] + "/sales"
SILVER_SALES_PATH   = config["storage"]["silver_path"] + "/sales"
BY_CATEGORY_PATH    = config["storage"]["silver_path"] + "/sales_by_category"
BY_REGION_PATH      = config["storage"]["silver_path"] + "/sales_by_region"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Delta Data

# COMMAND ----------

df_raw = read_delta(spark, BRONZE_DELTA_PATH)
log_dataframe_info(df_raw, "bronze_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean & Enrich

# COMMAND ----------

df_clean    = cast_and_clean(df_raw)
df_deduped  = deduplicate(df_clean, subset=["order_id"])
df_enriched = add_revenue_columns(df_deduped)
df_enriched = add_date_parts(df_enriched)

log_dataframe_info(df_enriched, "enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Silver Sales (partitioned by year / month)

# COMMAND ----------

write_delta_partitioned(df_enriched, SILVER_SALES_PATH, ["year", "month"], mode="overwrite")
print(f"Silver sales written to {SILVER_SALES_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Aggregations

# COMMAND ----------

df_by_category = aggregate_sales_by_category(df_enriched)
write_delta(df_by_category, BY_CATEGORY_PATH, mode="overwrite")
print(f"Category aggregation written to {BY_CATEGORY_PATH}")

df_by_region = aggregate_sales_by_region(df_enriched)
write_delta(df_by_region, BY_REGION_PATH, mode="overwrite")
print(f"Region aggregation written to {BY_REGION_PATH}")
