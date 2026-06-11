# Databricks notebook source
# gold: dim_empresa

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_EMPRESA = f"{CATALOG}.{GOLD}.dim_empresa"

if not table_exists(SILVER_TB_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_EMPRESA}")


def clean_text_expr(col_name: str):
    txt = F.trim(F.col(col_name).cast("string"))
    return F.when(
        txt.isNull() | (txt == "") | F.upper(txt).isin("N/A", "NA", "NAO_INFORMADO"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def clean_cnpj_expr(col_name: str):
    digits = F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^0-9]", "")
    return F.when(F.length(digits) == 14, digits).otherwise(F.lit(None).cast("string"))


def empresa_key_expr(col_name: str):
    txt = F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(txt, r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU)\b", " ")
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


w_nome = Window.partitionBy("empresa_nome_key")
w_best = Window.partitionBy("empresa_dedup_key").orderBy(
    F.when(F.col("nu_cnpj").isNotNull(), F.lit(1)).otherwise(F.lit(0)).desc(),
    F.length(F.coalesce(F.col("nm_empresa"), F.lit(""))).desc(),
    F.col("dt_primeira_evidencia").asc_nulls_last(),
    F.col("empresa_id").asc_nulls_last(),
)

df_empresa_base = (
    spark.table(SILVER_TB_EMPRESA)
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        clean_cnpj_expr("nu_cnpj").alias("nu_cnpj"),
        clean_text_expr("nm_empresa").alias("nm_empresa"),
        clean_text_expr("tp_empresa").alias("tp_empresa"),
        clean_text_expr("tp_empresa_papeis").alias("tp_empresa_papeis"),
        F.col("fl_tem_entrevista").cast("int").alias("fl_tem_entrevista"),
        F.col("fl_empresa_apenas_entrevista").cast("int").alias("fl_empresa_apenas_entrevista"),
        F.col("dt_primeira_evidencia").cast("date").alias("dt_primeira_evidencia"),
        F.col("dt_ultima_evidencia").cast("date").alias("dt_ultima_evidencia"),
        F.col("data_inicio").cast("date").alias("data_inicio"),
        F.col("data_fim").cast("date").alias("data_fim_raw"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .filter(F.col("dt_primeira_evidencia").cast("date") >= F.to_date(F.lit("2021-01-01")))
    .withColumn("empresa_nome_key", empresa_key_expr("nm_empresa"))
    .withColumn(
        "data_fim",
        F.when(
            F.col("data_inicio").isNotNull()
            & F.col("data_fim_raw").isNotNull()
            & (F.col("data_fim_raw") < F.col("data_inicio")),
            F.lit(None).cast("date"),
        ).otherwise(F.col("data_fim_raw"))
    )
    .drop("data_fim_raw")
)

df_empresa_filtrada = (
    df_empresa_base
    .withColumn(
        "fl_grupo_tem_cnpj",
        F.max(F.when(F.col("nu_cnpj").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(w_nome),
    )
    .filter(
        ~(
            F.col("nu_cnpj").isNull()
            & F.col("empresa_nome_key").isNotNull()
            & (F.col("fl_grupo_tem_cnpj") == 1)
        )
    )
    .withColumn(
        "empresa_dedup_key",
        F.coalesce(
            F.concat(F.lit("CNPJ|"), F.col("nu_cnpj")),
            F.concat(F.lit("NOME|"), F.col("empresa_nome_key")),
        ),
    )
)

df_empresa_best = (
    df_empresa_filtrada
    .withColumn("rn_empresa_best", F.row_number().over(w_best))
    .filter(F.col("rn_empresa_best") == 1)
    .select(
        "empresa_dedup_key",
        F.col("nm_empresa").alias("nm_empresa_canonica"),
        F.col("tp_empresa").alias("tp_empresa_canonico"),
        F.col("tp_empresa_papeis").alias("tp_empresa_papeis_canonico"),
    )
)

df_gold = (
    df_empresa_filtrada
    .groupBy("empresa_dedup_key")
    .agg(
        F.min("empresa_id").cast("bigint").alias("empresa_id"),
        F.first("nu_cnpj", ignorenulls=True).alias("nu_cnpj"),
        F.max("fl_tem_entrevista").cast("int").alias("fl_tem_entrevista"),
        F.max("fl_empresa_apenas_entrevista").cast("int").alias("fl_empresa_apenas_entrevista"),
        F.min("dt_primeira_evidencia").alias("dt_primeira_evidencia"),
        F.max("dt_ultima_evidencia").alias("dt_ultima_evidencia"),
        F.min("data_inicio").alias("data_inicio"),
        F.max("data_fim").alias("data_fim"),
        F.max("pipeline_run_id_origem").alias("pipeline_run_id_origem"),
        F.max("ingested_at_origem").alias("ingested_at_origem"),
    )
    .join(df_empresa_best, on="empresa_dedup_key", how="left")
    .drop("empresa_dedup_key")
    .withColumn("nm_empresa", F.col("nm_empresa_canonica"))
    .withColumn("tp_empresa", F.col("tp_empresa_canonico"))
    .withColumn("tp_empresa_papeis", F.col("tp_empresa_papeis_canonico"))
    .drop("nm_empresa_canonica", "tp_empresa_canonico", "tp_empresa_papeis_canonico")
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
        F.col("nm_empresa").cast("string").alias("nm_empresa"),
        F.col("tp_empresa").cast("string").alias("tp_empresa"),
        F.col("tp_empresa_papeis").cast("string").alias("tp_empresa_papeis"),
        F.col("fl_tem_entrevista").cast("int").alias("fl_tem_entrevista"),
        F.col("fl_empresa_apenas_entrevista").cast("int").alias("fl_empresa_apenas_entrevista"),
        F.col("dt_primeira_evidencia").cast("date").alias("dt_primeira_evidencia"),
        F.col("dt_ultima_evidencia").cast("date").alias("dt_ultima_evidencia"),
        F.col("data_inicio").cast("date").alias("data_inicio"),
        F.col("data_fim").cast("date").alias("data_fim"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_EMPRESA} (
    empresa_id BIGINT,
    nu_cnpj STRING,
    nm_empresa STRING,
    tp_empresa STRING,
    tp_empresa_papeis STRING,
    fl_tem_entrevista INT,
    fl_empresa_apenas_entrevista INT,
    dt_primeira_evidencia DATE,
    dt_ultima_evidencia DATE,
    data_inicio DATE,
    data_fim DATE,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_EMPRESA)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_EMPRESA)
