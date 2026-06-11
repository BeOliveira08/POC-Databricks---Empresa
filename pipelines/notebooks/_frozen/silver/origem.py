# Databricks notebook source
# silver: origem

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_STG_VINCULO = f"{CATALOG}.{SILVER}.stg_vinculo"
SILVER_STG_ORIGEM = f"{CATALOG}.{SILVER}.stg_origem"

df_final = (
    spark.table(SILVER_STG_VINCULO)
    .select(F.col("origem_id").cast("string").alias("origem_id"))
    .dropDuplicates(["origem_id"])
    .withColumn("nm_origem", F.regexp_replace(F.col("origem_id"), r"^ORIGEM:", ""))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
)

write_overwrite(df_final, SILVER_STG_ORIGEM, catalog_schema=f"{CATALOG}.{SILVER}")

