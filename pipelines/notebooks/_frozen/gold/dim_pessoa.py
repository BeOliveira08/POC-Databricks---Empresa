# Databricks notebook source
# gold: dim_pessoa

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_STG_PESSOA = f"{CATALOG}.{SILVER}.stg_pessoa"
SILVER_STG_VINCULO = f"{CATALOG}.{SILVER}.stg_vinculo"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"

df_pessoa = spark.table(SILVER_STG_PESSOA)
df_vinc = spark.table(SILVER_STG_VINCULO)

w_pessoa = Window.partitionBy("pessoa_id").orderBy(
    F.col("dt_inicio").desc_nulls_last(),
    F.col("vl_remuneracao").desc_nulls_last()
)

df_agg = (
    df_vinc
    .withColumn("rn_ult", F.row_number().over(w_pessoa))
    .groupBy("pessoa_id")
    .agg(
        F.countDistinct("vinculo_id").alias("qt_vinculos"),
        F.countDistinct("empresa_id").alias("qt_empresas"),
        F.min("dt_inicio").alias("dt_primeiro_vinculo"),
        F.max("dt_fim").alias("dt_ultimo_vinculo"),
        F.max(F.when(F.col("rn_ult") == 1, F.col("empresa_id"))).alias("empresa_id_atual"),
        F.max(F.when(F.col("rn_ult") == 1, F.col("cargo_id"))).alias("cargo_id_atual"),
        F.max(F.when(F.col("rn_ult") == 1, F.col("tipo_vinculo_id"))).alias("tipo_vinculo_id_atual"),
        F.max(F.when(F.col("status_vinculo") == "ATIVO", F.lit(1)).otherwise(F.lit(0))).alias("fl_vinculo_ativo")
    )
)

w = Window.orderBy("pessoa_id")

df_gold = (
    df_pessoa.alias("p")
    .join(df_agg.alias("a"), on="pessoa_id", how="left")
    .withColumn("sk_pessoa", F.row_number().over(w).cast("bigint"))
    .select(
        "sk_pessoa",
        F.col("p.pessoa_id").cast("string").alias("pessoa_id"),
        F.col("p.id_pessoa").cast("string").alias("id_pessoa"),
        F.col("p.pk_pessoa").cast("string").alias("pk_pessoa"),
        F.col("p.nu_cpf").cast("string").alias("nu_cpf"),
        F.col("p.nome_pessoa").cast("string").alias("nm_pessoa"),
        F.col("p.tipo_pessoa").cast("string").alias("tipo_pessoa"),
        F.col("p.fl_tem_clt").cast("int").alias("fl_tem_clt"),
        F.col("p.fl_tem_pj").cast("int").alias("fl_tem_pj"),
        F.coalesce(F.col("a.qt_vinculos"), F.lit(0)).cast("bigint").alias("qt_vinculos"),
        F.coalesce(F.col("a.qt_empresas"), F.lit(0)).cast("bigint").alias("qt_empresas"),
        F.col("a.dt_primeiro_vinculo").cast("date").alias("dt_primeiro_vinculo"),
        F.col("a.dt_ultimo_vinculo").cast("date").alias("dt_ultimo_vinculo"),
        F.col("a.empresa_id_atual").cast("string").alias("empresa_id_atual"),
        F.col("a.cargo_id_atual").cast("string").alias("cargo_id_atual"),
        F.col("a.tipo_vinculo_id_atual").cast("string").alias("tipo_vinculo_id_atual"),
        F.coalesce(F.col("a.fl_vinculo_ativo"), F.lit(0)).cast("int").alias("fl_vinculo_ativo"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )
)

write_overwrite(df_gold, GOLD_DIM_PESSOA)

