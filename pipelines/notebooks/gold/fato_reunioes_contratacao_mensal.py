# Databricks notebook source
# gold: fato_reunioes_contratacao_mensal

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_FATO_REUNIOES_CONTRATACAO = f"{CATALOG}.{GOLD}.fato_reunioes_contratacao"
GOLD_FATO_REUNIOES_CONTRATACAO_MENSAL = f"{CATALOG}.{GOLD}.fato_reunioes_contratacao_mensal"

if not table_exists(GOLD_FATO_REUNIOES_CONTRATACAO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {GOLD_FATO_REUNIOES_CONTRATACAO}")

df_mensal = (
    spark.table(GOLD_FATO_REUNIOES_CONTRATACAO)
    .withColumn("sk_mes", F.floor(F.col("sk_data_reuniao") / F.lit(100)).cast("int"))
    .groupBy("sk_mes")
    .agg(
        F.countDistinct("sk_reuniao_contratacao").alias("qt_eventos"),
        F.countDistinct(F.when(F.col("empresa_id").isNotNull(), F.col("sk_reuniao_contratacao"))).alias("qt_eventos_com_empresa_match"),
        F.countDistinct(F.when(F.col("processo_seletivo_key").isNotNull(), F.col("sk_reuniao_contratacao"))).alias("qt_eventos_com_processo"),
        F.countDistinct(F.when(F.col("empresa_id").isNotNull(), F.col("empresa_id"))).alias("qt_empresas_distintas"),
        F.countDistinct("plataforma").alias("qt_plataformas_distintas"),
    )
    .withColumn(
        "pc_eventos_com_empresa_match",
        F.when(
            F.col("qt_eventos") > 0,
            F.round((F.col("qt_eventos_com_empresa_match") / F.col("qt_eventos")) * F.lit(100.0), 2),
        ).otherwise(F.lit(0.0))
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.concat(F.col("sk_mes").cast("string"), F.lit("01")).cast("int").alias("sk_data_mes"),
        F.col("qt_eventos").cast("bigint").alias("qt_eventos"),
        F.col("qt_eventos_com_empresa_match").cast("bigint").alias("qt_eventos_com_empresa_match"),
        F.col("qt_eventos_com_processo").cast("bigint").alias("qt_eventos_com_processo"),
        F.col("pc_eventos_com_empresa_match").cast("double").alias("pc_eventos_com_empresa_match"),
        F.col("qt_empresas_distintas").cast("bigint").alias("qt_empresas_distintas"),
        F.col("qt_plataformas_distintas").cast("bigint").alias("qt_plataformas_distintas"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_REUNIOES_CONTRATACAO_MENSAL} (
    sk_data_mes INT,
    qt_eventos BIGINT,
    qt_eventos_com_empresa_match BIGINT,
    qt_eventos_com_processo BIGINT,
    pc_eventos_com_empresa_match DOUBLE,
    qt_empresas_distintas BIGINT,
    qt_plataformas_distintas BIGINT,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_mensal, GOLD_FATO_REUNIOES_CONTRATACAO_MENSAL)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_REUNIOES_CONTRATACAO_MENSAL)
