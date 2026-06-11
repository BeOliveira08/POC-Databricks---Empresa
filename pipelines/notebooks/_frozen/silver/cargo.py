# Databricks notebook source
# silver: cargo

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------
# MAGIC %run ../../../utils/transforms

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_CARGO = f"{CATALOG}.{SILVER}.tb_cargo"
BRONZE_CONTRATOS = f"{CATALOG}.{BRONZE}.contratos_geral"

if not table_exists(BRONZE_CONTRATOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_CONTRATOS}")

df_contratos_raw = spark.table(BRONZE_CONTRATOS)
cols_contratos = set(df_contratos_raw.columns)

df_cargos_base = (
    df_contratos_raw
    .select(
        norm_text("cargo_tratado").alias("cargo"),
        norm_text("senioridade_tratada").alias("senioridade"),
        first_cast_expr(cols_contratos, "string", "pipeline_run_id").alias("pipeline_run_id_origem"),
        first_cast_expr(cols_contratos, "timestamp", "ingested_at").alias("ingested_at_origem"),
    )
    .withColumn("cargo", F.when(F.col("cargo") != "", F.col("cargo")))
    .withColumn("senioridade", F.when(F.col("senioridade") != "", F.col("senioridade")))
    .filter(F.col("cargo").isNotNull())
    .dropDuplicates(["cargo", "senioridade"])
)

df_final = (
    df_cargos_base
    .withColumn(
        "id_cargo",
        F.row_number().over(
            Window.orderBy(
                F.col("cargo").asc(),
                F.col("senioridade").asc_nulls_last(),
            )
        ).cast("bigint")
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_cargo").cast("bigint").alias("id_cargo"),
        F.col("cargo").cast("string").alias("cargo"),
        F.col("senioridade").cast("string").alias("senioridade"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

write_overwrite(df_final, SILVER_TB_CARGO, catalog_schema=f"{CATALOG}.{SILVER}")

