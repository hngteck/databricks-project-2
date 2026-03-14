# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Data Ingestion
# MAGIC
# MAGIC This notebook reads raw sales data from CSV files into a Delta table in the
# MAGIC **raw** layer of the medallion architecture.
# MAGIC
# MAGIC **Inputs**
# MAGIC - `config/settings.yaml` – pipeline configuration
# MAGIC - `/data/raw/sales/*.csv` – raw CSV files
# MAGIC
# MAGIC **Outputs**
# MAGIC - Delta table at the path defined by `storage.raw_path` in the config

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

RAW_CSV_PATH    = config["storage"]["raw_path"] + "/sales"
RAW_DELTA_PATH  = config["storage"]["delta_path"] + "/raw/sales"

print(f"Reading CSV from : {RAW_CSV_PATH}")
print(f"Writing Delta to : {RAW_DELTA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Raw CSV Data

# COMMAND ----------

df_raw = read_csv(spark, RAW_CSV_PATH)
log_dataframe_info(df_raw, "raw_csv")

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
# MAGIC ## 5. Write to Delta (Raw Layer)

# COMMAND ----------

write_delta(df_with_meta, RAW_DELTA_PATH, mode="overwrite")
print(f"Successfully wrote raw Delta table to {RAW_DELTA_PATH}")
