# Databricks notebook source
# gold: fato_funcionarios

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TB_FUNCIONARIO_EMPRESA = f"{CATALOG}.{SILVER}.tb_func_emp"
SILVER_TB_EMPRESA_CONTRATACAO = f"{CATALOG}.{SILVER}.tb_empresa_contratacao"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_FATO_FUNCIONARIOS = f"{CATALOG}.{GOLD}.fato_funcionarios"

if not table_exists(SILVER_TB_EMPRESA_CONTRATACAO) and not table_exists(SILVER_TB_FUNCIONARIO_EMPRESA):
    raise ValueError(
        f"Tabela obrigatoria nao encontrada. Esperado ao menos uma de: {[SILVER_TB_EMPRESA_CONTRATACAO, SILVER_TB_FUNCIONARIO_EMPRESA]}"
    )
if not table_exists(GOLD_DIM_PESSOA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_PESSOA}")

df_funcionario_pessoa = spark.table(GOLD_DIM_PESSOA).select(
    F.col("id_pessoa").cast("bigint").alias("sk_funcionario"),
)

if table_exists(SILVER_TB_EMPRESA_CONTRATACAO):
    df_base = spark.table(SILVER_TB_EMPRESA_CONTRATACAO).select(
        F.col("id_empresa_contratacao").cast("bigint").alias("id_funcionario_interno"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("id_funcionario").cast("bigint").alias("id_funcionario"),
    )
else:
    df_base = spark.table(SILVER_TB_FUNCIONARIO_EMPRESA).select(
        F.col("id_funcionario_empresa").cast("bigint").alias("id_funcionario_interno"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("id_funcionario").cast("bigint").alias("id_funcionario"),
    )

df_gold = (
    df_base
    .filter(F.col("empresa_id").isNotNull() & F.col("id_funcionario").isNotNull())
    .join(df_funcionario_pessoa, F.col("id_funcionario") == F.col("sk_funcionario"), "left")
    .select(
        F.col("id_funcionario_interno").cast("bigint").alias("id_funcionario_interno"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_funcionario").cast("bigint").alias("sk_funcionario"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_FUNCIONARIOS} (
    id_funcionario_interno BIGINT,
    empresa_id BIGINT,
    sk_funcionario BIGINT,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_FUNCIONARIOS)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_FUNCIONARIOS)
