# Databricks notebook source
# gold: dim_cargo

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
GOLD_DIM_CARGO = f"{CATALOG}.{GOLD}.dim_cargo"


def empty_cargo_df():
    return spark.createDataFrame([], "cargo string, senioridade string")


def clean_text_expr(col_name: str):
    txt = F.trim(F.col(col_name).cast("string"))
    return F.when(
        txt.isNull() | (txt == "") | F.upper(txt).isin("N/A", "NA", "NAO_INFORMADO"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


dfs = []

if table_exists(SILVER_TB_CTPS):
    dfs.append(
        spark.table(SILVER_TB_CTPS)
        .select(
            clean_text_expr("cargo").alias("cargo"),
            F.lit(None).cast("string").alias("senioridade"),
        )
    )

if table_exists(SILVER_TB_CONTRATOS):
    dfs.append(
        spark.table(SILVER_TB_CONTRATOS)
        .select(
            clean_text_expr("cargo").alias("cargo"),
            clean_text_expr("senioridade").alias("senioridade"),
        )
    )

if not dfs:
    raise ValueError(f"Tabela obrigatoria nao encontrada. Esperado ao menos uma de: {[SILVER_TB_CTPS, SILVER_TB_CONTRATOS]}")

df_base = empty_cargo_df()
for df in dfs:
    df_base = df_base.unionByName(df)

w_senioridade = Window.partitionBy("cargo").orderBy(
    F.when(F.col("senioridade").isNotNull(), F.lit(0)).otherwise(F.lit(1)).asc(),
    F.col("qt_registros").desc_nulls_last(),
    F.col("senioridade").asc_nulls_last(),
)
w_cargo = Window.orderBy(F.col("cargo").asc_nulls_last())

df_gold = (
    df_base
    .filter(F.col("cargo").isNotNull())
    .groupBy("cargo", "senioridade")
    .agg(F.count(F.lit(1)).alias("qt_registros"))
    .withColumn("rn_senioridade", F.row_number().over(w_senioridade))
    .filter(F.col("rn_senioridade") == 1)
    .drop("qt_registros", "rn_senioridade")
    .withColumn("id_cargo", F.row_number().over(w_cargo).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_cargo").cast("bigint").alias("id_cargo"),
        F.col("cargo").cast("string").alias("cargo"),
        F.col("senioridade").cast("string").alias("senioridade"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_CARGO} (
    id_cargo BIGINT,
    cargo STRING,
    senioridade STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_CARGO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_CARGO)
