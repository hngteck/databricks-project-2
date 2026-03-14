# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Data Ingestion
# MAGIC
# MAGIC This notebook reads raw sales data from CSV files into a Delta table in the
# MAGIC **bronze** layer of the medallion architecture.
# MAGIC
# MAGIC **Inputs**
# MAGIC - `config/settings.yaml` – pipeline configuration
# MAGIC - `bronze_path/csv/sales/*.csv` – raw CSV files
# MAGIC
# MAGIC **Outputs**
# MAGIC - Delta table at the path defined by `storage.bronze_path` in the config

# COMMAND ----------

import sys
sys.path.insert(0, "../")

from src.config import load_config
from src.ingestion import read_csv, write_delta
from src.utils import log_dataframe_info, validate_not_null, add_ingestion_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration

# COMMAND ----------

config = load_config("../config/settings.yaml")

BRONZE_CSV_PATH    = config["storage"]["bronze_path"] + "/csv/sales"
BRONZE_DELTA_PATH  = config["storage"]["bronze_path"] + "/sales"

print(f"Reading CSV from : {BRONZE_CSV_PATH}")
print(f"Writing Delta to : {BRONZE_DELTA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Raw CSV Data

# COMMAND ----------

df_raw = read_csv(spark, BRONZE_CSV_PATH)
log_dataframe_info(df_raw, "bronze_csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Basic Validation

# COMMAND ----------

required_columns = ["order_id", "order_date", "customer_id", "product_id", "quantity", "unit_price"]
df_validated = validate_not_null(df_raw, required_columns)
log_dataframe_info(df_validated, "after_validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Add Ingestion Metadata

# COMMAND ----------

df_with_meta = add_ingestion_timestamp(df_validated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta (Bronze Layer)

# COMMAND ----------

write_delta(df_with_meta, BRONZE_DELTA_PATH, mode="overwrite")
print(f"Successfully wrote bronze Delta table to {BRONZE_DELTA_PATH}")
