# Databricks notebook source
# gold: fato_das

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TB_DAS = f"{CATALOG}.{SILVER}.tb_das"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_FATO_DAS = f"{CATALOG}.{GOLD}.fato_das"


def empresa_nome_key(col_name: str):
    return F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", "")

if not table_exists(SILVER_TB_DAS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_DAS}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")

df_das = spark.table(SILVER_TB_DAS)
df_data_competencia = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_competencia"),
    F.col("dt_referencia").alias("competencia_ref"),
)
df_data_vencimento = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_vencimento"),
    F.col("dt_referencia").alias("dt_vencimento_ref"),
)
df_data_arrecadacao = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_arrecadacao"),
    F.col("dt_referencia").alias("dt_arrecadacao_ref"),
)
if table_exists(SILVER_TB_EMPRESA):
    df_empresa = spark.table(SILVER_TB_EMPRESA).select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj_empresa"),
        F.col("nm_empresa").cast("string").alias("nm_empresa"),
    )
else:
    df_empresa = spark.createDataFrame(
        [],
        "empresa_id bigint, nu_cnpj_empresa string, nm_empresa string",
    )

df_empresa_nome_match = (
    df_empresa
    .select(
        "empresa_id",
        F.explode(
            F.array_distinct(
                F.array(
                    empresa_nome_key("nm_empresa"),
                )
            )
        ).alias("empresa_nome_key_match"),
    )
    .filter(F.col("empresa_nome_key_match") != "")
    .groupBy("empresa_nome_key_match")
    .agg(
        F.countDistinct("empresa_id").alias("qtd_empresa_match"),
        F.max("empresa_id").cast("bigint").alias("empresa_id_by_nome"),
    )
    .filter(F.col("qtd_empresa_match") == 1)
    .drop("qtd_empresa_match")
)

df_gold = (
    df_das
    .withColumn("nm_prestador_key", empresa_nome_key("nm_prestador"))
    .join(df_empresa.alias("e_cnpj"), df_das["nu_cnpj_prestador"] == F.col("e_cnpj.nu_cnpj_empresa"), "left")
    .join(df_empresa_nome_match, F.col("nm_prestador_key") == F.col("empresa_nome_key_match"), "left")
    .withColumn("empresa_id_resolvida", F.coalesce(F.col("e_cnpj.empresa_id"), F.col("empresa_id_by_nome")))
    .join(df_data_competencia, F.col("competencia").cast("date") == F.col("competencia_ref"), "left")
    .join(df_data_vencimento, F.col("dt_vencimento").cast("date") == F.col("dt_vencimento_ref"), "left")
    .join(df_data_arrecadacao, F.col("dt_arrecadacao").cast("date") == F.col("dt_arrecadacao_ref"), "left")
    .select(
        F.col("id_das").cast("bigint").alias("sk_das"),
        F.col("empresa_id_resolvida").cast("bigint").alias("empresa_id"),
        F.col("sk_data_competencia").cast("int").alias("sk_data_competencia"),
        F.col("competencia").cast("date").alias("competencia"),
        F.col("nr_documento").cast("string").alias("nr_documento"),
        F.col("sk_data_vencimento").cast("int").alias("sk_data_vencimento"),
        F.col("dt_vencimento").cast("date").alias("dt_vencimento"),
        F.col("sk_data_arrecadacao").cast("int").alias("sk_data_arrecadacao"),
        F.col("dt_arrecadacao").cast("date").alias("dt_arrecadacao"),
        F.col("vl_principal").cast("double").alias("vl_principal"),
        F.col("vl_multa").cast("double").alias("vl_multa"),
        F.col("vl_juros").cast("double").alias("vl_juros"),
        F.col("vl_total").cast("double").alias("vl_total"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_DAS} (
    sk_das BIGINT,
    empresa_id BIGINT,
    sk_data_competencia INT,
    competencia DATE,
    nr_documento STRING,
    sk_data_vencimento INT,
    dt_vencimento DATE,
    sk_data_arrecadacao INT,
    dt_arrecadacao DATE,
    vl_principal DOUBLE,
    vl_multa DOUBLE,
    vl_juros DOUBLE,
    vl_total DOUBLE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_DAS)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_DAS)
