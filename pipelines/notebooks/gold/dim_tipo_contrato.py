# Databricks notebook source
# gold: dim_tipo_contrato

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"


def empty_tipo_df():
    return spark.createDataFrame([], "tipo_contrato string, tipo_pessoa string")


def normalized_tipo_contrato_expr(col_expr):
    txt = F.upper(F.trim(F.coalesce(col_expr.cast("string"), F.lit(""))))
    return (
        F.when((txt == "") | (txt == "NAO_INFORMADO"), F.lit("NAO_INFORMADO"))
        .when(txt.contains("COOPER"), F.lit("COOPERATIVA"))
        .when(txt.contains("CLT"), F.lit("CLT"))
        .when(txt.contains("PJ"), F.lit("PJ"))
        .otherwise(txt)
    )


def tipo_pessoa_expr(col_name: str):
    return (
        F.when(F.col(col_name) == "COOPERATIVA", F.lit("COOPERATIVA"))
        .when(F.col(col_name) == "CLT", F.lit("CLT"))
        .when(F.col(col_name) == "PJ", F.lit("PJ"))
        .otherwise(F.lit("NAO_INFORMADO"))
    )


dfs = [
    spark.createDataFrame(
        [
            ("NAO_INFORMADO", "NAO_INFORMADO"),
            ("CLT", "CLT"),
            ("PJ", "PJ"),
            ("COOPERATIVA", "COOPERATIVA"),
        ],
        ["tipo_contrato", "tipo_pessoa"],
    )
]

if table_exists(SILVER_TB_CONTRATOS):
    dfs.append(
        spark.table(SILVER_TB_CONTRATOS)
        .select(
            normalized_tipo_contrato_expr(F.col("tp_vinculo")).alias("tipo_contrato"),
        )
        .withColumn("tipo_pessoa", tipo_pessoa_expr("tipo_contrato"))
    )

df_base = empty_tipo_df()
for df in dfs:
    df_base = df_base.unionByName(df)

w = Window.orderBy(
    F.when(F.col("tipo_contrato") == "NAO_INFORMADO", F.lit(0)).otherwise(F.lit(1)).asc(),
    F.col("tipo_contrato").asc_nulls_last(),
    F.col("tipo_pessoa").asc_nulls_last(),
)

df_gold = (
    df_base
    .filter(F.col("tipo_contrato").isNotNull() | F.col("tipo_pessoa").isNotNull())
    .dropDuplicates(["tipo_contrato", "tipo_pessoa"])
    .withColumn("id_tipo_contrato", F.row_number().over(w).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "id_tipo_contrato",
        "tipo_contrato",
        "tipo_pessoa",
        "pipeline_run_id",
        "ingested_at",
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_TIPO_CONTRATO} (
    id_tipo_contrato BIGINT,
    tipo_contrato STRING,
    tipo_pessoa STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_TIPO_CONTRATO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_TIPO_CONTRATO)
