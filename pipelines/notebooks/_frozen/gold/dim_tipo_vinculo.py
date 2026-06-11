# Databricks notebook source
# gold: dim_tipo_vinculo

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_STG_TIPO_VINCULO = f"{CATALOG}.{SILVER}.stg_tipo_vinculo"
GOLD_DIM_TIPO_VINCULO = f"{CATALOG}.{GOLD}.dim_tipo_vinculo"

w = Window.orderBy("tipo_vinculo_id")

df_gold = (
    spark.table(SILVER_STG_TIPO_VINCULO)
    .withColumn("sk_tipo_vinculo", F.row_number().over(w).cast("bigint"))
    .select(
        "sk_tipo_vinculo",
        F.col("tipo_vinculo_id").cast("string").alias("tipo_vinculo_id"),
        F.col("nm_tipo_vinculo").cast("string").alias("nm_tipo_vinculo"),
        F.col("tipo_pessoa").cast("string").alias("tipo_pessoa"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )
)

write_overwrite(df_gold, GOLD_DIM_TIPO_VINCULO)

