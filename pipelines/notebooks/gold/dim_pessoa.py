# Databricks notebook source
# gold: dim_pessoa

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_DIM_FUNCIONARIO = f"{CATALOG}.{GOLD}.dim_funcionario"
SILVER_TB_IRPF = f"{CATALOG}.{SILVER}.tb_irpf"
SILVER_TB_INSS = f"{CATALOG}.{SILVER}.tb_inss"
SILVER_TB_RESCISOES = f"{CATALOG}.{SILVER}.tb_rescisoes"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"

if not table_exists(GOLD_DIM_FUNCIONARIO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {GOLD_DIM_FUNCIONARIO}")


def clean_name_expr(col_expr):
    txt = F.trim(F.coalesce(col_expr.cast("string"), F.lit("")))
    return F.when(txt == "", F.lit(None).cast("string")).otherwise(txt)


df_funcionario = (
    spark.table(GOLD_DIM_FUNCIONARIO)
    .select(
        F.col("id_funcionario").cast("bigint").alias("id_pessoa"),
        F.col("nome_funcionario").cast("string").alias("nome_pessoa"),
        F.lit("FUNCIONARIO").cast("string").alias("tipo_pessoa"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
    )
)

df_docs = None

if table_exists(SILVER_TB_IRPF):
    df_irpf = (
        spark.table(SILVER_TB_IRPF)
        .select(
            F.col("nu_cpf").cast("string").alias("documento_ref"),
            clean_name_expr(F.col("nome")).alias("nome_pessoa"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        )
        .filter(F.col("documento_ref").isNotNull())
    )
    df_docs = df_irpf if df_docs is None else df_docs.unionByName(df_irpf)

if table_exists(SILVER_TB_INSS):
    df_inss = (
        spark.table(SILVER_TB_INSS)
        .select(
            F.col("nu_cpf").cast("string").alias("documento_ref"),
            F.lit(None).cast("string").alias("nome_pessoa"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        )
        .filter(F.col("documento_ref").isNotNull())
    )
    df_docs = df_inss if df_docs is None else df_docs.unionByName(df_inss)

if table_exists(SILVER_TB_RESCISOES):
    df_resc = (
        spark.table(SILVER_TB_RESCISOES)
        .select(
            F.col("cpf").cast("string").alias("documento_ref"),
            clean_name_expr(F.col("nome_profissional")).alias("nome_pessoa"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        )
        .filter(F.col("documento_ref").isNotNull())
    )
    df_docs = df_resc if df_docs is None else df_docs.unionByName(df_resc)

if df_docs is None:
    df_docs = empty_df_for_schema(
        "documento_ref string, nome_pessoa string, pipeline_run_id_origem string, ingested_at_origem timestamp"
    )

w_doc = Window.partitionBy("documento_ref").orderBy(
    F.col("nome_pessoa").isNull().asc(),
    F.length(F.coalesce(F.col("nome_pessoa"), F.lit(""))).desc(),
    F.col("ingested_at_origem").desc_nulls_last(),
)

func_count = df_funcionario.count()

df_doc_final = (
    df_docs
    .withColumn("rn", F.row_number().over(w_doc))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn(
        "id_pessoa",
        F.regexp_replace(F.col("documento_ref").cast("string"), r"[^0-9]", "").cast("bigint"),
    )
    .withColumn("tipo_pessoa", F.lit("DOCUMENTAL"))
    .select(
        "id_pessoa",
        "nome_pessoa",
        "tipo_pessoa",
        "pipeline_run_id_origem",
        "ingested_at_origem",
    )
)

w_final = Window.orderBy(
    F.col("id_pessoa").asc_nulls_last(),
)

df_gold = (
    df_funcionario
    .unionByName(df_doc_final)
    .dropDuplicates(["id_pessoa"])
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_pessoa").cast("bigint").alias("id_pessoa"),
        F.col("nome_pessoa").cast("string").alias("nome_pessoa"),
        F.col("tipo_pessoa").cast("string").alias("tipo_pessoa"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_PESSOA} (
    id_pessoa BIGINT,
    nome_pessoa STRING,
    tipo_pessoa STRING,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_PESSOA)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_PESSOA)
