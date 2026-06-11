# Databricks notebook source
# gold: dim_origem

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_STG_ORIGEM = f"{CATALOG}.{SILVER}.stg_origem"
GOLD_DIM_ORIGEM = f"{CATALOG}.{GOLD}.dim_origem"

w = Window.orderBy("origem_id")

df_gold = (
    spark.table(SILVER_STG_ORIGEM)
    .withColumn("sk_origem", F.row_number().over(w).cast("bigint"))
    .select(
        "sk_origem",
        F.col("origem_id").cast("string").alias("origem_id"),
        F.col("nm_origem").cast("string").alias("nm_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )
)

write_overwrite(df_gold, GOLD_DIM_ORIGEM)

