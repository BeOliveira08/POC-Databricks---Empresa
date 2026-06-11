# Databricks notebook source
# gold: fato_empresa_indicadores

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_FATO_VINCULO_EMPRESA = f"{CATALOG}.{GOLD}.fato_vinculo_empresa"
GOLD_FATO_EMPRESA_MENSAL = f"{CATALOG}.{GOLD}.fato_empresa_mensal"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_FATO_EMPRESA_INDICADORES = f"{CATALOG}.{GOLD}.fato_empresa_indicadores"

for table_name in [GOLD_FATO_VINCULO_EMPRESA, GOLD_FATO_EMPRESA_MENSAL, GOLD_DIM_TIPO_CONTRATO, GOLD_DIM_DATA]:
    if not table_exists(table_name):
        raise ValueError(f"Tabela obrigatoria nao encontrada: {table_name}")

df_tipo = spark.table(GOLD_DIM_TIPO_CONTRATO).select(F.col("id_tipo_contrato").alias("sk_tipo_contrato"), "tipo_pessoa")
df_data_inicio = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_primeiro_inicio"),
    F.col("dt_referencia").alias("dt_primeiro_inicio_ref"),
)
df_data_fim = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_ultimo_fim"),
    F.col("dt_referencia").alias("dt_ultimo_fim_ref"),
)

df_vinculos = (
    spark.table(GOLD_FATO_VINCULO_EMPRESA)
    .filter(F.col("dt_inicio").isNotNull())
    .withColumn("dt_fim_calculo", F.greatest(F.coalesce(F.col("dt_fim"), F.current_date()), F.col("dt_inicio")))
    .join(df_tipo, on="sk_tipo_contrato", how="left")
)

df_vinculos_agg = (
    df_vinculos
    .groupBy("empresa_id", "sk_tipo_contrato")
    .agg(
        F.countDistinct("sk_vinculo").alias("qt_vinculos"),
        F.countDistinct("sk_pessoa").alias("qt_pessoas_distintas"),
        F.min("dt_inicio").alias("dt_primeiro_inicio"),
        F.max("dt_fim_calculo").alias("dt_ultimo_fim"),
        F.sum(F.datediff(F.col("dt_fim_calculo"), F.col("dt_inicio")) + F.lit(1)).cast("bigint").alias("qt_dias_permanencia_total"),
        F.round(F.avg(F.datediff(F.col("dt_fim_calculo"), F.col("dt_inicio")) + F.lit(1)), 2).alias("qt_dias_permanencia_media"),
        F.round(F.avg(F.col("vl_remuneracao")), 2).alias("vl_remuneracao_media_contrato"),
    )
    .join(df_data_inicio, F.col("dt_primeiro_inicio") == F.col("dt_primeiro_inicio_ref"), "left")
    .join(df_data_fim, F.col("dt_ultimo_fim") == F.col("dt_ultimo_fim_ref"), "left")
)

w_pico = Window.partitionBy("empresa_id", "sk_tipo_contrato").orderBy(
    F.col("qt_pessoas_ativas").desc_nulls_last(),
    F.col("qt_vinculos_ativos").desc_nulls_last(),
    F.col("sk_data_mes").asc_nulls_last(),
)

df_pico_mes = (
    spark.table(GOLD_FATO_EMPRESA_MENSAL)
    .withColumn("rn_pico", F.row_number().over(w_pico))
    .filter(F.col("rn_pico") == 1)
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_tipo_contrato").cast("bigint").alias("sk_tipo_contrato"),
        F.col("sk_data_mes").cast("int").alias("sk_data_mes_pico"),
        F.col("qt_pessoas_ativas").cast("bigint").alias("qt_pessoas_pico"),
        F.col("qt_vinculos_ativos").cast("bigint").alias("qt_vinculos_pico"),
        F.col("vl_faturamento_mes").cast("double").alias("vl_faturamento_mes_pico"),
    )
)

df_mensal_agg = (
    spark.table(GOLD_FATO_EMPRESA_MENSAL)
    .groupBy("empresa_id", "sk_tipo_contrato")
    .agg(
        F.countDistinct("sk_data_mes").alias("qt_meses_ativos"),
        F.round(F.avg("qt_pessoas_ativas"), 2).alias("qt_pessoas_ativas_media"),
        F.round(F.sum(F.coalesce(F.col("vl_faturamento_mes"), F.lit(0.0))), 2).alias("vl_faturamento_total"),
        F.round(F.avg(F.coalesce(F.col("vl_faturamento_mes"), F.lit(0.0))), 2).alias("vl_faturamento_medio_mensal"),
        F.sum(F.coalesce(F.col("qt_notas_fiscais_mes"), F.lit(0))).cast("bigint").alias("qt_notas_fiscais"),
    )
)

df_gold = (
    df_vinculos_agg
    .join(df_mensal_agg, on=["empresa_id", "sk_tipo_contrato"], how="left")
    .join(df_pico_mes, on=["empresa_id", "sk_tipo_contrato"], how="left")
    .withColumn("qt_meses_permanencia_media", F.round(F.col("qt_dias_permanencia_media") / F.lit(30.0), 2))
    .withColumn(
        "fl_empresa_ativa_atual",
        F.when(F.col("dt_ultimo_fim") >= F.current_date(), F.lit(1)).otherwise(F.lit(0)).cast("int"),
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn(
        "sk_empresa_indicador",
        F.row_number().over(
            Window.orderBy(
                F.col("empresa_id").asc_nulls_last(),
                F.col("sk_tipo_contrato").asc_nulls_last(),
            )
        ).cast("bigint")
    )
    .select(
        F.col("sk_empresa_indicador").cast("bigint").alias("sk_empresa_indicador"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_tipo_contrato").cast("bigint").alias("sk_tipo_contrato"),
        F.col("sk_data_primeiro_inicio").cast("int").alias("sk_data_primeiro_inicio"),
        F.col("sk_data_ultimo_fim").cast("int").alias("sk_data_ultimo_fim"),
        F.col("sk_data_mes_pico").cast("int").alias("sk_data_mes_pico"),
        F.col("qt_vinculos").cast("bigint").alias("qt_vinculos"),
        F.col("qt_pessoas_distintas").cast("bigint").alias("qt_pessoas_distintas"),
        F.col("qt_meses_ativos").cast("bigint").alias("qt_meses_ativos"),
        F.col("qt_dias_permanencia_total").cast("bigint").alias("qt_dias_permanencia_total"),
        F.col("qt_dias_permanencia_media").cast("double").alias("qt_dias_permanencia_media"),
        F.col("qt_meses_permanencia_media").cast("double").alias("qt_meses_permanencia_media"),
        F.col("qt_pessoas_ativas_media").cast("double").alias("qt_pessoas_ativas_media"),
        F.col("qt_pessoas_pico").cast("bigint").alias("qt_pessoas_pico"),
        F.col("qt_vinculos_pico").cast("bigint").alias("qt_vinculos_pico"),
        F.col("fl_empresa_ativa_atual").cast("int").alias("fl_empresa_ativa_atual"),
        F.col("vl_remuneracao_media_contrato").cast("double").alias("vl_remuneracao_media_contrato"),
        F.col("vl_faturamento_total").cast("double").alias("vl_faturamento_total"),
        F.col("vl_faturamento_medio_mensal").cast("double").alias("vl_faturamento_medio_mensal"),
        F.col("vl_faturamento_mes_pico").cast("double").alias("vl_faturamento_mes_pico"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_EMPRESA_INDICADORES} (
    sk_empresa_indicador BIGINT,
    empresa_id BIGINT,
    sk_tipo_contrato BIGINT,
    sk_data_primeiro_inicio INT,
    sk_data_ultimo_fim INT,
    sk_data_mes_pico INT,
    qt_vinculos BIGINT,
    qt_pessoas_distintas BIGINT,
    qt_meses_ativos BIGINT,
    qt_dias_permanencia_total BIGINT,
    qt_dias_permanencia_media DOUBLE,
    qt_meses_permanencia_media DOUBLE,
    qt_pessoas_ativas_media DOUBLE,
    qt_pessoas_pico BIGINT,
    qt_vinculos_pico BIGINT,
    fl_empresa_ativa_atual INT,
    vl_remuneracao_media_contrato DOUBLE,
    vl_faturamento_total DOUBLE,
    vl_faturamento_medio_mensal DOUBLE,
    vl_faturamento_mes_pico DOUBLE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_EMPRESA_INDICADORES)
optimize_table(
    GOLD_FATO_EMPRESA_INDICADORES,
    zorder_cols=["empresa_id", "sk_tipo_contrato", "sk_data_primeiro_inicio"],
)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_EMPRESA_INDICADORES)
