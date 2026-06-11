# Databricks notebook source
# gold: dim_empresa

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_STG_EMPRESA = f"{CATALOG}.{SILVER}.stg_empresa"
GOLD_DIM_EMPRESA = f"{CATALOG}.{GOLD}.dim_empresa"

df_base = spark.table(SILVER_STG_EMPRESA)
w = Window.orderBy("empresa_id")

df_gold = (
    df_base
    .withColumn("sk_empresa", F.row_number().over(w).cast("bigint"))
    .select(
        "sk_empresa",
        F.col("empresa_id").cast("string").alias("empresa_id"),
        F.col("id_empresa_origem").cast("bigint").alias("id_empresa_origem"),
        F.col("pk_empresa").cast("string").alias("pk_empresa"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
        F.col("nm_empresa").cast("string").alias("nm_empresa"),
        F.col("nome_legal_canonico").cast("string").alias("nome_legal_canonico"),
        F.col("nome_operacional").cast("string").alias("nome_operacional"),
        F.col("nome_normalizado").cast("string").alias("nome_normalizado"),
        F.col("grupo_empresa").cast("string").alias("grupo_empresa"),
        F.col("qualidade_match").cast("string").alias("qualidade_match"),
        F.col("empresa_match").cast("boolean").alias("empresa_match"),
        F.col("fl_plano_saude").cast("int").alias("fl_plano_saude"),
        F.col("fl_plano_odontologico").cast("int").alias("fl_plano_odontologico"),
        F.col("fl_wellhub").cast("int").alias("fl_wellhub"),
        F.col("fl_lazer").cast("int").alias("fl_lazer"),
        F.col("fl_day_off_aniversario").cast("int").alias("fl_day_off_aniversario"),
        F.col("fl_seguro_vida").cast("int").alias("fl_seguro_vida"),
        F.col("fl_bem_estar").cast("int").alias("fl_bem_estar"),
        F.col("fl_idiomas").cast("int").alias("fl_idiomas"),
        F.col("vr_va_valor").cast("double").alias("vr_va_valor"),
        F.col("plr_valor").cast("double").alias("plr_valor"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )
)

write_overwrite(df_gold, GOLD_DIM_EMPRESA)

