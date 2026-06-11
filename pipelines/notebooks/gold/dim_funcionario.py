# Databricks notebook source
# gold: dim_funcionario

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_FUNCIONARIO = f"{CATALOG}.{SILVER}.tb_funcionario"
GOLD_DIM_FUNCIONARIO = f"{CATALOG}.{GOLD}.dim_funcionario"

if not table_exists(SILVER_TB_FUNCIONARIO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_FUNCIONARIO}")


def clean_text_expr(col_name: str):
    txt = F.trim(F.col(col_name).cast("string"))
    return F.when(
        txt.isNull() | (txt == "") | F.upper(txt).isin("N/A", "NA", "NAO_INFORMADO"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def clean_phone_expr(col_name: str):
    digits = F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^0-9]", "")
    return F.when(F.length(digits) >= 10, digits).otherwise(F.lit(None).cast("string"))


w = Window.orderBy(
    F.col("id_funcionario").asc_nulls_last(),
    F.col("nome_funcionario").asc_nulls_last(),
)

df_gold = (
    spark.table(SILVER_TB_FUNCIONARIO)
    .select(
        F.col("id_funcionario").cast("bigint").alias("id_funcionario"),
        clean_text_expr("nome_funcionario").alias("nome_funcionario"),
        clean_phone_expr("telefone").alias("telefone"),
        clean_text_expr("senioridade").alias("senioridade"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .filter(F.col("id_funcionario").isNotNull() & F.col("nome_funcionario").isNotNull())
    .dropDuplicates(["id_funcionario"])
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_funcionario").cast("bigint").alias("id_funcionario"),
        F.col("nome_funcionario").cast("string").alias("nome_funcionario"),
        F.col("telefone").cast("string").alias("telefone"),
        F.col("senioridade").cast("string").alias("senioridade"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_FUNCIONARIO} (
    id_funcionario BIGINT,
    nome_funcionario STRING,
    telefone STRING,
    senioridade STRING,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_FUNCIONARIO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_FUNCIONARIO)
