# Databricks notebook source
# MAGIC %md
# MAGIC # 03 – Data Analysis
# MAGIC
# MAGIC This notebook explores the processed sales data and produces key business
# MAGIC insights:
# MAGIC
# MAGIC 1. Monthly revenue trend
# MAGIC 2. Top 10 products by net revenue
# MAGIC 3. Revenue breakdown by region
# MAGIC 4. Customer purchase summary

# COMMAND ----------

import sys
sys.path.insert(0, "../")

from src.config import load_config
from src.ingestion import read_delta
from src.transformation import top_products_by_revenue, customer_summary
from src.utils import log_dataframe_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration & Read Processed Data

# COMMAND ----------

config = load_config("../config/settings.yaml")

PROCESSED_SALES_PATH = config["storage"]["delta_path"] + "/processed/sales"
BY_CATEGORY_PATH     = config["storage"]["delta_path"] + "/processed/sales_by_category"
BY_REGION_PATH       = config["storage"]["delta_path"] + "/processed/sales_by_region"

df_sales      = read_delta(spark, PROCESSED_SALES_PATH)
df_category   = read_delta(spark, BY_CATEGORY_PATH)
df_region     = read_delta(spark, BY_REGION_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Monthly Revenue Trend

# COMMAND ----------

df_monthly = (
    df_category
    .groupBy("year", "month")
    .sum("total_net_revenue")
    .withColumnRenamed("sum(total_net_revenue)", "monthly_net_revenue")
    .orderBy("year", "month")
)

display(df_monthly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top 10 Products by Net Revenue

# COMMAND ----------

df_top_products = top_products_by_revenue(df_sales, top_n=10)
display(df_top_products.orderBy("rank"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Revenue by Region

# COMMAND ----------

display(df_region.orderBy("total_net_revenue", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Customer Summary

# COMMAND ----------

df_customers = customer_summary(df_sales)
log_dataframe_info(df_customers, "customer_summary")
display(df_customers.orderBy("total_net_revenue", ascending=False).limit(20))
