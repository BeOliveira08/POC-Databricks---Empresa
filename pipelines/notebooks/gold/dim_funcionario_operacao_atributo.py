# Databricks notebook source
# gold: dim_funcionario_operacao_atributo

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_FUNCIONARIO_EMPRESA = f"{CATALOG}.{SILVER}.tb_func_emp"
SILVER_TB_EMPRESA_CONTRATACAO = f"{CATALOG}.{SILVER}.tb_empresa_contratacao"
GOLD_DIM_FUNCIONARIO_OPERACAO_ATRIBUTO = f"{CATALOG}.{GOLD}.dim_funcionario_operacao_atributo"

if not table_exists(SILVER_TB_EMPRESA_CONTRATACAO) and not table_exists(SILVER_TB_FUNCIONARIO_EMPRESA):
    raise ValueError(
        f"Tabela obrigatoria nao encontrada. Esperado ao menos uma de: {[SILVER_TB_EMPRESA_CONTRATACAO, SILVER_TB_FUNCIONARIO_EMPRESA]}"
    )

if table_exists(SILVER_TB_EMPRESA_CONTRATACAO):
    df_base = spark.table(SILVER_TB_EMPRESA_CONTRATACAO).select(
        F.coalesce(F.col("tipo_contratacao").cast("string"), F.lit("NAO_INFORMADO")).alias("tipo_contratacao"),
        F.coalesce(F.col("motivo_saida").cast("string"), F.lit("NAO_INFORMADO")).alias("motivo_saida"),
        F.coalesce(F.col("status_contratacao").cast("string"), F.lit("NAO_INFORMADO")).alias("status_operacao"),
    )
else:
    df_base = spark.table(SILVER_TB_FUNCIONARIO_EMPRESA).select(
        F.lit("NAO_INFORMADO").cast("string").alias("tipo_contratacao"),
        F.lit("NAO_INFORMADO").cast("string").alias("motivo_saida"),
        F.lit("NAO_INFORMADO").cast("string").alias("status_operacao"),
    )

w = Window.orderBy(
    F.col("tipo_contratacao").asc_nulls_last(),
    F.col("motivo_saida").asc_nulls_last(),
    F.col("status_operacao").asc_nulls_last(),
)

df_gold = (
    df_base
    .dropDuplicates()
    .withColumn("consultoria", F.lit("NAO_INFORMADO"))
    .withColumn("cliente", F.lit("NAO_INFORMADO"))
    .withColumn("sk_funcionario_operacao_atributo", F.row_number().over(w).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_funcionario_operacao_atributo",
        "consultoria",
        "cliente",
        "tipo_contratacao",
        "motivo_saida",
        "status_operacao",
        "pipeline_run_id",
        "ingested_at",
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_FUNCIONARIO_OPERACAO_ATRIBUTO} (
    sk_funcionario_operacao_atributo BIGINT,
    consultoria STRING,
    cliente STRING,
    tipo_contratacao STRING,
    motivo_saida STRING,
    status_operacao STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_FUNCIONARIO_OPERACAO_ATRIBUTO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_FUNCIONARIO_OPERACAO_ATRIBUTO)
