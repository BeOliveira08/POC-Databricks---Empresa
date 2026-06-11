# Databricks notebook source
# gold: fato_empresa_mensal

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_BRIDGE_VINCULO_MES = f"{CATALOG}.{GOLD}.bridge_vinculo_mes"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"
GOLD_FATO_EMPRESA_MENSAL = f"{CATALOG}.{GOLD}.fato_empresa_mensal"

if not table_exists(GOLD_BRIDGE_VINCULO_MES):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {GOLD_BRIDGE_VINCULO_MES}")
if not table_exists(GOLD_DIM_TIPO_CONTRATO):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_TIPO_CONTRATO}")

df_tipo = spark.table(GOLD_DIM_TIPO_CONTRATO).select(F.col("id_tipo_contrato").alias("sk_tipo_contrato"), "tipo_pessoa")

df_base = (
    spark.table(GOLD_BRIDGE_VINCULO_MES)
    .join(df_tipo, on="sk_tipo_contrato", how="left")
)

w_empresa_mes = Window.orderBy(
    F.col("empresa_id").asc_nulls_last(),
    F.col("sk_tipo_contrato").asc_nulls_last(),
    F.col("sk_data_mes").asc_nulls_last(),
)

df_gold = (
    df_base
    .groupBy("empresa_id", "sk_tipo_contrato", "sk_data_mes", "ano", "mes", "tipo_pessoa")
    .agg(
        F.countDistinct("sk_vinculo").alias("qt_vinculos_ativos"),
        F.countDistinct("sk_pessoa").alias("qt_pessoas_ativas"),
        F.sum(F.coalesce(F.col("dias_ativos_mes"), F.lit(0))).cast("bigint").alias("qt_dias_ativos_total"),
        F.round(F.sum(F.coalesce(F.col("vl_remuneracao"), F.lit(0.0))), 2).alias("vl_valor_total_mensal"),
        F.round(F.sum(F.coalesce(F.col("vl_remuneracao_mes_proporcional"), F.lit(0.0))), 2).alias("vl_faturamento_mes"),
    )
    .withColumn(
        "qt_notas_fiscais_mes",
        F.lit(0).cast("bigint"),
    )
    .withColumn("fl_empresa_ativa_mes", F.when(F.col("qt_vinculos_ativos") > 0, F.lit(1)).otherwise(F.lit(0)).cast("int"))
    .withColumn("sk_empresa_mes", F.row_number().over(w_empresa_mes).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("sk_empresa_mes").cast("bigint").alias("sk_empresa_mes"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_tipo_contrato").cast("bigint").alias("sk_tipo_contrato"),
        F.col("sk_data_mes").cast("int").alias("sk_data_mes"),
        F.col("ano").cast("int").alias("ano"),
        F.col("mes").cast("int").alias("mes"),
        F.col("qt_vinculos_ativos").cast("bigint").alias("qt_vinculos_ativos"),
        F.col("qt_pessoas_ativas").cast("bigint").alias("qt_pessoas_ativas"),
        F.col("qt_dias_ativos_total").cast("bigint").alias("qt_dias_ativos_total"),
        F.col("fl_empresa_ativa_mes").cast("int").alias("fl_empresa_ativa_mes"),
        F.col("vl_valor_total_mensal").cast("double").alias("vl_valor_total_mensal"),
        F.col("vl_faturamento_mes").cast("double").alias("vl_faturamento_mes"),
        F.col("qt_notas_fiscais_mes").cast("bigint").alias("qt_notas_fiscais_mes"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_EMPRESA_MENSAL} (
    sk_empresa_mes BIGINT,
    empresa_id BIGINT,
    sk_tipo_contrato BIGINT,
    sk_data_mes INT,
    ano INT,
    mes INT,
    qt_vinculos_ativos BIGINT,
    qt_pessoas_ativas BIGINT,
    qt_dias_ativos_total BIGINT,
    fl_empresa_ativa_mes INT,
    vl_valor_total_mensal DOUBLE,
    vl_faturamento_mes DOUBLE,
    qt_notas_fiscais_mes BIGINT,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_EMPRESA_MENSAL)
optimize_table(
    GOLD_FATO_EMPRESA_MENSAL,
    zorder_cols=["empresa_id", "sk_tipo_contrato", "sk_data_mes"],
)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_EMPRESA_MENSAL)
