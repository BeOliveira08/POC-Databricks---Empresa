# Databricks notebook source
# silver: tipo_vinculo

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_STG_VINCULO = f"{CATALOG}.{SILVER}.stg_vinculo"
SILVER_STG_TIPO_VINCULO = f"{CATALOG}.{SILVER}.stg_tipo_vinculo"

df_final = (
    spark.table(SILVER_STG_VINCULO)
    .select(
        F.col("tipo_vinculo_id").cast("string").alias("tipo_vinculo_id"),
        F.coalesce(F.col("tipo_contrato"), F.lit("NAO_INFORMADO")).cast("string").alias("nm_tipo_vinculo")
    )
    .dropDuplicates(["tipo_vinculo_id"])
    .withColumn("tipo_pessoa", F.when(F.upper(F.col("nm_tipo_vinculo")).like("%PJ%"), F.lit("PJ")).when(F.upper(F.col("nm_tipo_vinculo")).like("%CLT%"), F.lit("CLT")).otherwise(F.lit("NAO_IDENTIFICADO")))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
)

write_overwrite(df_final, SILVER_STG_TIPO_VINCULO, catalog_schema=f"{CATALOG}.{SILVER}")

