# Databricks notebook source
# gold: fato_informe_rendimentos

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../config/domain_config

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_INFORME_RENDIMENTOS = f"{CATALOG}.{SILVER}.tb_informe_rendimentos"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_FATO_INFORME_RENDIMENTOS = f"{CATALOG}.{GOLD}.fato_informe_rendimentos"

if not table_exists(SILVER_TB_INFORME_RENDIMENTOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_INFORME_RENDIMENTOS}")
if not table_exists(GOLD_DIM_PESSOA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_PESSOA}")

df_pessoa = spark.table(GOLD_DIM_PESSOA).select(
    F.col("sk_pessoa").cast("bigint").alias("sk_pessoa"),
    F.col("pessoa_key").cast("string").alias("pessoa_key"),
)
df_pessoa_informe = (
    df_pessoa
    .filter(F.col("pessoa_key") == F.concat(F.lit("DOC|"), F.lit(TITULAR_CPF)))
    .select(
        F.col("sk_pessoa").cast("bigint").alias("sk_pessoa_informe"),
        F.col("pessoa_key").cast("string").alias("nu_cpf_informe"),
    )
)

w = Window.orderBy(
    F.col("cpf").asc_nulls_last(),
    F.col("ano_calendario").asc_nulls_last(),
    F.col("exercicio").asc_nulls_last(),
    F.col("id_informe_rendimentos").asc_nulls_last(),
)

df_gold = (
    spark.table(SILVER_TB_INFORME_RENDIMENTOS)
    .withColumn("nu_cpf_informe", F.lit(TITULAR_CPF).cast("string"))
    .join(df_pessoa_informe, on="nu_cpf_informe", how="left")
    .withColumn("sk_informe_rendimentos", F.row_number().over(w).cast("bigint"))
    .select(
        F.col("sk_informe_rendimentos").cast("bigint").alias("sk_informe_rendimentos"),
        F.col("id_informe_rendimentos").cast("bigint").alias("id_informe_rendimentos"),
        F.col("sk_pessoa_informe").cast("bigint").alias("sk_pessoa"),
        F.col("exercicio").cast("int").alias("exercicio"),
        F.col("ano_calendario").cast("int").alias("ano_calendario"),
        F.col("rendimentos_tributaveis").cast("double").alias("rendimentos_tributaveis"),
        F.col("imposto_retido_total").cast("double").alias("imposto_retido_total"),
        F.col("imposto_restituir").cast("double").alias("imposto_restituir"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_INFORME_RENDIMENTOS} (
    sk_informe_rendimentos BIGINT,
    id_informe_rendimentos BIGINT,
    sk_pessoa BIGINT,
    exercicio INT,
    ano_calendario INT,
    rendimentos_tributaveis DOUBLE,
    imposto_retido_total DOUBLE,
    imposto_restituir DOUBLE,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_INFORME_RENDIMENTOS)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_INFORME_RENDIMENTOS)
