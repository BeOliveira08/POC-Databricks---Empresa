# Databricks notebook source
# gold: dim_cargo

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_STG_CARGO = f"{CATALOG}.{SILVER}.stg_cargo"
GOLD_DIM_CARGO = f"{CATALOG}.{GOLD}.dim_cargo"

w = Window.orderBy("cargo_id")

df_gold = (
    spark.table(SILVER_STG_CARGO)
    .withColumn("sk_cargo", F.row_number().over(w).cast("bigint"))
    .select(
        "sk_cargo",
        F.col("cargo_id").cast("string").alias("cargo_id"),
        F.col("nm_cargo").cast("string").alias("nm_cargo"),
        F.col("senioridade").cast("string").alias("senioridade"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )
)

write_overwrite(df_gold, GOLD_DIM_CARGO)

