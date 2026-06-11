# Databricks notebook source
# gold: fato_rescisoes

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_RESCISOES = f"{CATALOG}.{SILVER}.tb_rescisoes"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_FATO_RESCISOES = f"{CATALOG}.{GOLD}.fato_rescisoes"

if not table_exists(SILVER_TB_RESCISOES):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_RESCISOES}")
if not table_exists(GOLD_DIM_PESSOA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_PESSOA}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")

df_resc = spark.table(SILVER_TB_RESCISOES)
cols_resc = set(df_resc.columns)

df_pessoa = spark.table(GOLD_DIM_PESSOA).select(
    F.col("id_pessoa").cast("bigint").alias("sk_pessoa"),
)
df_data = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_rescisao"),
    F.col("dt_referencia").alias("dt_data_dim"),
)

file_name_order_expr = (
    F.col("file_name").asc_nulls_last()
    if "file_name" in cols_resc
    else F.col("id_rescisao").asc_nulls_last()
)

w = Window.orderBy(
    F.col("dt_rescisao").asc_nulls_last(),
    F.col("cpf").asc_nulls_last(),
    file_name_order_expr,
    F.col("id_rescisao").asc_nulls_last(),
)

df_gold = (
    df_resc
    .join(df_pessoa, F.regexp_replace(F.col("cpf").cast("string"), r"[^0-9]", "").cast("bigint") == F.col("sk_pessoa"), "left")
    .join(df_data, F.col("dt_rescisao") == F.col("dt_data_dim"), "left")
    .withColumn("sk_rescisao", F.row_number().over(w).cast("bigint"))
    .select(
        F.col("sk_rescisao").cast("bigint").alias("sk_rescisao"),
        F.col("id_rescisao").cast("bigint").alias("id_rescisao"),
        F.col("sk_pessoa").cast("bigint").alias("sk_pessoa"),
        F.col("sk_data_rescisao").cast("bigint").alias("sk_data_rescisao"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("tipo_rescisao").cast("string").alias("tipo_rescisao"),
        F.col("dt_rescisao").cast("date").alias("dt_rescisao"),
        (
            F.col("file_name").cast("string")
            if "file_name" in cols_resc
            else F.lit(None).cast("string")
        ).alias("file_name"),
        (
            F.col("pipeline_run_id").cast("string")
            if "pipeline_run_id" in cols_resc
            else F.lit(None).cast("string")
        ).alias("pipeline_run_id_origem"),
        (
            F.col("ingested_at").cast("timestamp")
            if "ingested_at" in cols_resc
            else F.lit(None).cast("timestamp")
        ).alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_RESCISOES} (
    sk_rescisao BIGINT,
    id_rescisao BIGINT,
    sk_pessoa BIGINT,
    sk_data_rescisao BIGINT,
    nome_empresa STRING,
    tipo_rescisao STRING,
    dt_rescisao DATE,
    file_name STRING,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_RESCISOES)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_RESCISOES)
