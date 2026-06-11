# Databricks notebook source
# gold: dim_rescisao

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_RESCISOES = f"{CATALOG}.{SILVER}.tb_rescisoes"
GOLD_DIM_EMPRESA = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_DIM_RESCISAO = f"{CATALOG}.{GOLD}.dim_rescisao"

if not table_exists(SILVER_TB_RESCISOES):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_RESCISOES}")

df_empresa = empty_df_for_schema("empresa_id bigint, nu_cnpj string, nm_empresa string")
if table_exists(GOLD_DIM_EMPRESA):
    df_empresa = (
        spark.table(GOLD_DIM_EMPRESA)
        .select(
            F.col("empresa_id").cast("bigint").alias("empresa_id"),
            F.regexp_replace(F.col("nu_cnpj").cast("string"), r"[^0-9]", "").alias("nu_cnpj"),
            F.col("nm_empresa").cast("string").alias("nm_empresa"),
        )
    )

df_base = (
    spark.table(SILVER_TB_RESCISOES)
    .filter(F.upper(F.coalesce(F.col("tipo_vinculo").cast("string"), F.lit(""))) == F.lit("CLT"))
    .select(
        F.col("id_rescisao").cast("bigint").alias("id_documento_rescisao"),
        F.col("file_name").cast("string").alias("file_name"),
        F.regexp_replace(F.col("cpf").cast("string"), r"[^0-9]", "").alias("cpf"),
        F.regexp_replace(F.col("cnpj").cast("string"), r"[^0-9]", "").alias("cnpj"),
        F.col("nome_profissional").cast("string").alias("nome_profissional"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("tipo_documento").cast("string").alias("tipo_documento"),
        F.col("tipo_rescisao").cast("string").alias("tipo_rescisao"),
        F.col("dt_rescisao").cast("date").alias("dt_rescisao"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .filter(F.col("dt_rescisao").isNotNull())
)

df_eventos = (
    df_base.alias("r")
    .join(
        F.broadcast(df_empresa).alias("e"),
        F.col("r.cnpj") == F.col("e.nu_cnpj"),
        "left",
    )
    .groupBy(
        F.col("r.cpf"),
        F.col("r.cnpj"),
        F.col("r.dt_rescisao"),
        F.coalesce(F.col("e.empresa_id"), F.lit(None).cast("bigint")).alias("empresa_id"),
    )
    .agg(
        F.max(F.coalesce(F.col("e.nm_empresa"), F.col("r.nome_empresa"))).alias("nome_empresa"),
        F.max(F.col("r.nome_profissional")).alias("nome_profissional"),
        F.max(F.col("r.tipo_rescisao")).alias("tipo_rescisao"),
        F.countDistinct("id_documento_rescisao").cast("int").alias("qt_documentos_origem"),
        F.max(F.when(F.col("r.tipo_documento") == "COMPROVANTE_PAGAMENTO", F.lit(1)).otherwise(F.lit(0))).cast("int").alias("fl_tem_documento_pagamento"),
        F.max(F.when(F.col("r.tipo_documento") == "MULTA_RESCISORIA", F.lit(1)).otherwise(F.lit(0))).cast("int").alias("fl_tem_multa_rescisoria"),
        F.max("pipeline_run_id_origem").alias("pipeline_run_id_origem"),
        F.max("ingested_at_origem").alias("ingested_at_origem"),
    )
)

w = Window.orderBy(
    F.col("dt_rescisao").asc_nulls_last(),
    F.col("cnpj").asc_nulls_last(),
    F.col("cpf").asc_nulls_last(),
    F.col("empresa_id").asc_nulls_last(),
)

df_gold = (
    df_eventos
    .filter(F.col("cpf").isNotNull() & F.col("dt_rescisao").isNotNull())
    .withColumn("id_rescisao", F.row_number().over(w).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_rescisao").cast("bigint").alias("id_rescisao"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("cpf").cast("string").alias("cpf"),
        F.col("cnpj").cast("string").alias("cnpj"),
        F.col("nome_profissional").cast("string").alias("nome_profissional"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("tipo_rescisao").cast("string").alias("tipo_rescisao"),
        F.col("dt_rescisao").cast("date").alias("dt_rescisao"),
        F.col("qt_documentos_origem").cast("int").alias("qt_documentos_origem"),
        F.col("fl_tem_documento_pagamento").cast("int").alias("fl_tem_documento_pagamento"),
        F.col("fl_tem_multa_rescisoria").cast("int").alias("fl_tem_multa_rescisoria"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_RESCISAO} (
    id_rescisao BIGINT,
    empresa_id BIGINT,
    cpf STRING,
    cnpj STRING,
    nome_profissional STRING,
    nome_empresa STRING,
    tipo_rescisao STRING,
    dt_rescisao DATE,
    qt_documentos_origem INT,
    fl_tem_documento_pagamento INT,
    fl_tem_multa_rescisoria INT,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_RESCISAO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_RESCISAO)
