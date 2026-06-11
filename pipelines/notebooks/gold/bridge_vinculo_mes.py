# Databricks notebook source
# gold: bridge_vinculo_mes

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_FATO_VINCULO_EMPRESA = f"{CATALOG}.{GOLD}.fato_vinculo_empresa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_DIM_EMPRESA = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_BRIDGE_VINCULO_MES = f"{CATALOG}.{GOLD}.bridge_vinculo_mes"

if not table_exists(GOLD_FATO_VINCULO_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {GOLD_FATO_VINCULO_EMPRESA}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")
if not table_exists(GOLD_DIM_EMPRESA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_EMPRESA}")

df_mes = (
    spark.table(GOLD_DIM_DATA)
    .filter(F.dayofmonth("dt_referencia") == 1)
    .select(
        F.col("id_data").cast("int").alias("sk_data_mes"),
        F.col("dt_referencia").cast("date").alias("dt_mes_inicio"),
        F.last_day("dt_referencia").cast("date").alias("dt_mes_fim"),
        F.col("nr_ano").cast("int").alias("ano"),
        F.col("nr_mes").cast("int").alias("mes"),
    )
)

df_empresa = spark.table(GOLD_DIM_EMPRESA).select(
    F.col("empresa_id").cast("bigint").alias("empresa_id"),
    F.col("data_fim").cast("date").alias("dt_fim_empresa_dim"),
)

df_vinculo = (
    spark.table(GOLD_FATO_VINCULO_EMPRESA)
    .join(df_empresa, on="empresa_id", how="left")
    .filter(F.col("dt_inicio").isNotNull())
    .withColumn(
        "dt_fim_referencia",
        F.least(
            F.coalesce(F.col("dt_fim"), F.current_date()),
            F.coalesce(F.col("dt_fim_empresa_dim"), F.current_date()),
        ),
    )
    .withColumn("dt_fim_calculo", F.col("dt_fim_referencia"))
    .filter(F.col("dt_fim_calculo") >= F.col("dt_inicio"))
    .withColumn("dt_mes_inicio_min", F.trunc(F.col("dt_inicio"), "month").cast("date"))
    .withColumn("dt_mes_inicio_max", F.trunc(F.col("dt_fim_calculo"), "month").cast("date"))
    .withColumn(
        "dt_mes_inicio",
        F.explode(F.sequence(F.col("dt_mes_inicio_min"), F.col("dt_mes_inicio_max"), F.expr("interval 1 month"))),
    )
)

w_bridge = Window.orderBy(
    F.col("sk_vinculo").asc_nulls_last(),
    F.col("sk_data_mes").asc_nulls_last(),
)

df_bridge = (
    df_vinculo
    .join(df_mes, on="dt_mes_inicio", how="inner")
    .withColumn("dt_inicio_ativo_mes", F.greatest(F.col("dt_inicio"), F.col("dt_mes_inicio")))
    .withColumn("dt_fim_ativo_mes", F.least(F.col("dt_fim_calculo"), F.col("dt_mes_fim")))
    .withColumn("dias_mes", (F.datediff(F.col("dt_mes_fim"), F.col("dt_mes_inicio")) + F.lit(1)).cast("int"))
    .withColumn("dias_ativos_mes", (F.datediff(F.col("dt_fim_ativo_mes"), F.col("dt_inicio_ativo_mes")) + F.lit(1)).cast("int"))
    .filter(F.col("dias_ativos_mes") > 0)
    .withColumn("fl_ativo_mes", F.lit(1).cast("int"))
    .withColumn(
        "vl_remuneracao_dia_ponderada",
        F.when(
            F.col("vl_remuneracao").isNotNull(),
            F.col("vl_remuneracao") * F.col("dias_ativos_mes"),
        ).cast("double"),
    )
    .withColumn(
        "vl_remuneracao_mes_proporcional",
        F.when(
            F.col("vl_remuneracao").isNotNull(),
            F.col("vl_remuneracao") * F.col("dias_ativos_mes") / F.col("dias_mes"),
        ).cast("double"),
    )
    .withColumn("sk_vinculo_mes", F.row_number().over(w_bridge).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("sk_vinculo_mes").cast("bigint").alias("sk_vinculo_mes"),
        F.col("sk_vinculo").cast("bigint").alias("sk_vinculo"),
        F.col("sk_pessoa").cast("bigint").alias("sk_pessoa"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_cargo").cast("bigint").alias("sk_cargo"),
        F.col("sk_tipo_contrato").cast("bigint").alias("sk_tipo_contrato"),
        F.col("sk_data_mes").cast("int").alias("sk_data_mes"),
        F.col("ano").cast("int").alias("ano"),
        F.col("mes").cast("int").alias("mes"),
        F.col("sk_data_inicio").cast("int").alias("sk_data_inicio"),
        F.col("sk_data_fim").cast("int").alias("sk_data_fim"),
        F.col("dias_mes").cast("int").alias("dias_mes"),
        F.col("dias_ativos_mes").cast("int").alias("dias_ativos_mes"),
        F.col("fl_ativo_mes").cast("int").alias("fl_ativo_mes"),
        F.col("vl_remuneracao").cast("double").alias("vl_remuneracao"),
        F.round(F.col("vl_remuneracao_dia_ponderada"), 2).cast("double").alias("vl_remuneracao_dia_ponderada"),
        F.round(F.col("vl_remuneracao_mes_proporcional"), 2).cast("double").alias("vl_remuneracao_mes_proporcional"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_BRIDGE_VINCULO_MES} (
    sk_vinculo_mes BIGINT,
    sk_vinculo BIGINT,
    sk_pessoa BIGINT,
    empresa_id BIGINT,
    sk_cargo BIGINT,
    sk_tipo_contrato BIGINT,
    sk_data_mes INT,
    ano INT,
    mes INT,
    sk_data_inicio INT,
    sk_data_fim INT,
    dias_mes INT,
    dias_ativos_mes INT,
    fl_ativo_mes INT,
    vl_remuneracao DOUBLE,
    vl_remuneracao_dia_ponderada DOUBLE,
    vl_remuneracao_mes_proporcional DOUBLE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_bridge, GOLD_BRIDGE_VINCULO_MES)
optimize_table(
    GOLD_BRIDGE_VINCULO_MES,
    zorder_cols=["empresa_id", "sk_pessoa", "sk_tipo_contrato", "sk_data_mes"],
)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_BRIDGE_VINCULO_MES)
