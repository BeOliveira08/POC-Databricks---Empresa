# Databricks notebook source
# gold: fato_vinculo

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_STG_VINCULO = f"{CATALOG}.{SILVER}.stg_vinculo"
GOLD_DIM_EMPRESA = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_DIM_CARGO = f"{CATALOG}.{GOLD}.dim_cargo"
GOLD_DIM_TIPO_VINCULO = f"{CATALOG}.{GOLD}.dim_tipo_vinculo"
GOLD_DIM_ORIGEM = f"{CATALOG}.{GOLD}.dim_origem"
GOLD_FATO_VINCULO = f"{CATALOG}.{GOLD}.fato_vinculo_empresa"

df_base = spark.table(SILVER_STG_VINCULO)

df_gold = (
    df_base.alias("f")
    .join(spark.table(GOLD_DIM_EMPRESA).select("empresa_id", "sk_empresa").alias("e"), on="empresa_id", how="left")
    .join(spark.table(GOLD_DIM_PESSOA).select("pessoa_id", "sk_pessoa").alias("p"), on="pessoa_id", how="left")
    .join(spark.table(GOLD_DIM_CARGO).select("cargo_id", "sk_cargo").alias("c"), on="cargo_id", how="left")
    .join(spark.table(GOLD_DIM_TIPO_VINCULO).select("tipo_vinculo_id", "sk_tipo_vinculo").alias("tv"), on="tipo_vinculo_id", how="left")
    .join(spark.table(GOLD_DIM_ORIGEM).select("origem_id", "sk_origem").alias("o"), on="origem_id", how="left")
    .select(
        F.col("vinculo_id").cast("string").alias("sk_vinculo"),
        F.col("vinculo_id").cast("string").alias("vinculo_id"),
        F.col("empresa_id").cast("string").alias("empresa_id"),
        F.col("pessoa_id").cast("string").alias("pessoa_id"),
        F.col("cargo_id").cast("string").alias("cargo_id"),
        F.col("tipo_vinculo_id").cast("string").alias("tipo_vinculo_id"),
        F.col("origem_id").cast("string").alias("origem_id"),
        F.col("sk_empresa").cast("bigint").alias("sk_empresa"),
        F.col("sk_pessoa").cast("bigint").alias("sk_pessoa"),
        F.col("sk_cargo").cast("bigint").alias("sk_cargo"),
        F.col("sk_tipo_vinculo").cast("bigint").alias("sk_tipo_vinculo"),
        F.col("sk_origem").cast("bigint").alias("sk_origem"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.coalesce(F.col("vl_remuneracao"), F.lit(0.0)).cast("double").alias("vl_remuneracao_base"),
        F.when(F.col("status_vinculo") == "ATIVO", F.lit(1)).otherwise(F.lit(0)).cast("int").alias("fl_ativo_atual"),
        F.col("status_vinculo").cast("string").alias("status_vinculo"),
        F.col("fl_tem_ctps").cast("int").alias("fl_tem_ctps"),
        F.col("fl_clt").cast("int").alias("fl_clt"),
        F.col("fl_pj").cast("int").alias("fl_pj"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )
)

write_overwrite(df_gold, GOLD_FATO_VINCULO)

