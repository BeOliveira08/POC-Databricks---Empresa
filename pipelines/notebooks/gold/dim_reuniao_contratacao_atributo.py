# Databricks notebook source
# gold: dim_reuniao_contratacao_atributo

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_REUNIOES_CONTRATACAO = f"{CATALOG}.{SILVER}.tb_reunioes_contratacao"
GOLD_DIM_REUNIAO_CONTRATACAO_ATRIBUTO = f"{CATALOG}.{GOLD}.dim_reuniao_contratacao_atributo"

if not table_exists(SILVER_TB_REUNIOES_CONTRATACAO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_REUNIOES_CONTRATACAO}")

df_src = spark.table(SILVER_TB_REUNIOES_CONTRATACAO)
cols_src = set(df_src.columns)


def src_str_or_default(col_name: str, default_value: str = "NAO_INFORMADO"):
    if col_name in cols_src:
        return F.coalesce(F.col(col_name).cast("string"), F.lit(default_value))
    return F.lit(default_value).cast("string")


w = Window.orderBy(
    F.col("tipo_evento").asc_nulls_last(),
    F.col("plataforma").asc_nulls_last(),
    F.col("status_reuniao").asc_nulls_last(),
    F.col("empresa_match_origem").asc_nulls_last(),
)

df_gold = (
    df_src
    .select(
        F.when(F.upper(src_str_or_default("assunto")).contains("ONBOARD"), F.lit("ONBOARDING"))
         .when(F.upper(src_str_or_default("assunto")).contains("TREINAMENTO"), F.lit("TREINAMENTO"))
         .when(F.upper(src_str_or_default("assunto")).contains("ENTREVISTA"), F.lit("ENTREVISTA"))
         .when(F.upper(src_str_or_default("assunto")).contains("BATE PAPO"), F.lit("BATE_PAPO"))
         .when(F.upper(src_str_or_default("assunto")).contains("REUNIAO"), F.lit("REUNIAO"))
         .otherwise(F.lit("OUTRO")).alias("tipo_evento"),
        src_str_or_default("plataforma").alias("plataforma"),
        F.when(F.trim(src_str_or_default("link_reuniao", "")) != "", F.lit("AGENDADA")).otherwise(F.lit("NAO_INFORMADO")).alias("status_reuniao"),
        F.lit("EMPRESA").cast("string").alias("empresa_match_origem"),
        F.when(F.upper(src_str_or_default("status_match_empresa")).contains("MATCH"), F.lit("ALTA")).otherwise(F.lit("NAO_INFORMADO")).alias("confianca_match_empresa"),
        src_str_or_default("status_match_empresa").alias("status_mapeamento_final"),
    )
    .dropDuplicates()
    .withColumn("sk_reuniao_atributo", F.row_number().over(w).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_reuniao_atributo",
        "tipo_evento",
        "plataforma",
        "status_reuniao",
        "empresa_match_origem",
        "confianca_match_empresa",
        "status_mapeamento_final",
        "pipeline_run_id",
        "ingested_at",
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_REUNIAO_CONTRATACAO_ATRIBUTO} (
    sk_reuniao_atributo BIGINT,
    tipo_evento STRING,
    plataforma STRING,
    status_reuniao STRING,
    empresa_match_origem STRING,
    confianca_match_empresa STRING,
    status_mapeamento_final STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_REUNIAO_CONTRATACAO_ATRIBUTO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_REUNIAO_CONTRATACAO_ATRIBUTO)
