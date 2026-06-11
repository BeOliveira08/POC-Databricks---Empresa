# Databricks notebook source
# gold: fato_resultado_contratacao

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_FATO_REUNIOES_CONTRATACAO = f"{CATALOG}.{GOLD}.fato_reunioes_contratacao"
GOLD_FATO_VINCULO_EMPRESA = f"{CATALOG}.{GOLD}.fato_vinculo_empresa"
GOLD_FATO_RESULTADO_CONTRATACAO = f"{CATALOG}.{GOLD}.fato_resultado_contratacao"

if not table_exists(GOLD_FATO_REUNIOES_CONTRATACAO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {GOLD_FATO_REUNIOES_CONTRATACAO}")

df_reunioes = spark.table(GOLD_FATO_REUNIOES_CONTRATACAO)

df_processos = (
    df_reunioes
    .groupBy("processo_seletivo_key", "sk_empresa_contratacao", "empresa_id")
    .agg(
        F.min("evento_ref_ts").cast("timestamp").alias("ts_primeiro_evento"),
        F.max("evento_ref_ts").cast("timestamp").alias("ts_ultimo_evento"),
        F.min("sk_data_reuniao").cast("int").alias("sk_data_primeiro_evento"),
        F.max("sk_data_reuniao").cast("int").alias("sk_data_ultimo_evento"),
        F.countDistinct("sk_reuniao_contratacao").alias("qt_eventos_processo"),
        F.max(F.when(F.col("tipo_evento").isin("ONBOARDING", "TREINAMENTO"), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_onboarding_ou_treinamento"),
    )
    .filter(F.col("processo_seletivo_key").isNotNull())
)

if table_exists(GOLD_FATO_VINCULO_EMPRESA):
    df_vinculos = (
        spark.table(GOLD_FATO_VINCULO_EMPRESA)
        .select(
            F.col("empresa_id").cast("bigint").alias("empresa_id_vinculo"),
            F.col("dt_inicio").cast("date").alias("dt_inicio_vinculo"),
            F.col("sk_vinculo").cast("bigint").alias("sk_vinculo"),
        )
        .filter(F.col("empresa_id_vinculo").isNotNull() & F.col("dt_inicio_vinculo").isNotNull())
    )
else:
    df_vinculos = spark.createDataFrame([], "empresa_id_vinculo bigint, dt_inicio_vinculo date, sk_vinculo bigint")

df_vinculo_match = (
    df_processos.alias("p")
    .join(
        df_vinculos.alias("v"),
        (F.col("p.empresa_id") == F.col("v.empresa_id_vinculo"))
        & F.col("p.empresa_id").isNotNull()
        & (F.col("v.dt_inicio_vinculo") >= F.to_date(F.col("p.ts_primeiro_evento")))
        & (F.col("v.dt_inicio_vinculo") <= F.date_add(F.to_date(F.col("p.ts_ultimo_evento")), 180)),
        "left",
    )
    .groupBy("p.processo_seletivo_key", "p.sk_empresa_contratacao", "p.empresa_id")
    .agg(
        F.min("v.dt_inicio_vinculo").cast("date").alias("dt_primeiro_vinculo_match"),
        F.max(F.when(F.col("v.sk_vinculo").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_vinculo_match"),
    )
)

df_gold = (
    df_processos.alias("p")
    .join(
        df_vinculo_match.alias("m"),
        on=["processo_seletivo_key", "sk_empresa_contratacao", "empresa_id"],
        how="left",
    )
    .withColumn(
        "status_resultado",
        F.when((F.col("fl_tem_onboarding_ou_treinamento") == 1) | (F.col("fl_tem_vinculo_match") == 1), F.lit("PASSOU"))
        .when(F.to_date(F.col("ts_ultimo_evento")) <= F.date_add(F.current_date(), -60), F.lit("NAO_PASSOU"))
        .otherwise(F.lit("EM_ANDAMENTO")),
    )
    .withColumn(
        "criterio_resultado",
        F.when(F.col("fl_tem_onboarding_ou_treinamento") == 1, F.lit("EVENTO_ONBOARDING_TREINAMENTO"))
        .when(F.col("fl_tem_vinculo_match") == 1, F.lit("VINCULO_POSTERIOR_COM_EMPRESA"))
        .when(F.to_date(F.col("ts_ultimo_evento")) <= F.date_add(F.current_date(), -60), F.lit("SEM_DESFECHO_APOS_60_DIAS"))
        .otherwise(F.lit("PROCESSO_RECENTE_OU_ABERTO")),
    )
    .withColumn("fl_passou", F.when(F.col("status_resultado") == "PASSOU", F.lit(1)).otherwise(F.lit(0)))
    .withColumn("fl_nao_passou", F.when(F.col("status_resultado") == "NAO_PASSOU", F.lit(1)).otherwise(F.lit(0)))
    .withColumn("fl_em_andamento", F.when(F.col("status_resultado") == "EM_ANDAMENTO", F.lit(1)).otherwise(F.lit(0)))
    .withColumn(
        "sk_resultado_contratacao",
        F.row_number().over(
            Window.orderBy(
                F.col("ts_primeiro_evento").asc_nulls_last(),
                F.col("processo_seletivo_key").asc_nulls_last(),
            )
        ).cast("bigint"),
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.col("sk_resultado_contratacao").cast("bigint").alias("sk_resultado_contratacao"),
        F.col("processo_seletivo_key").cast("string").alias("processo_seletivo_key"),
        F.col("sk_empresa_contratacao").cast("bigint").alias("sk_empresa_contratacao"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_data_primeiro_evento").cast("int").alias("sk_data_primeiro_evento"),
        F.col("sk_data_ultimo_evento").cast("int").alias("sk_data_ultimo_evento"),
        F.col("qt_eventos_processo").cast("bigint").alias("qt_eventos_processo"),
        F.col("fl_passou").cast("int").alias("fl_passou"),
        F.col("fl_nao_passou").cast("int").alias("fl_nao_passou"),
        F.col("fl_em_andamento").cast("int").alias("fl_em_andamento"),
        F.col("status_resultado").cast("string").alias("status_resultado"),
        F.col("criterio_resultado").cast("string").alias("criterio_resultado"),
        F.col("dt_primeiro_vinculo_match").cast("date").alias("dt_primeiro_vinculo_match"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_RESULTADO_CONTRATACAO} (
    sk_resultado_contratacao BIGINT,
    processo_seletivo_key STRING,
    sk_empresa_contratacao BIGINT,
    empresa_id BIGINT,
    sk_data_primeiro_evento INT,
    sk_data_ultimo_evento INT,
    qt_eventos_processo BIGINT,
    fl_passou INT,
    fl_nao_passou INT,
    fl_em_andamento INT,
    status_resultado STRING,
    criterio_resultado STRING,
    dt_primeiro_vinculo_match DATE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_RESULTADO_CONTRATACAO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_RESULTADO_CONTRATACAO)
