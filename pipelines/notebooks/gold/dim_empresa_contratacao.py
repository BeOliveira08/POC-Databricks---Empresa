# Databricks notebook source
# gold: dim_empresa_contratacao

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_REUNIOES_CONTRATACAO = f"{CATALOG}.{SILVER}.tb_reunioes_contratacao"
GOLD_DIM_EMPRESA = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_DIM_EMPRESA_CONTRATACAO = f"{CATALOG}.{GOLD}.dim_empresa_contratacao"

if not table_exists(SILVER_TB_REUNIOES_CONTRATACAO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_REUNIOES_CONTRATACAO}")


def empresa_name_key_expr(col_expr):
    txt = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(txt, r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU)\b", " ")
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def empresa_base_key_expr(col_expr):
    txt = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(
        txt,
        r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU|CONSULTORIA|CONSULTING|SERVICOS|TECNOLOGIA|INFORMATICA|DESENVOLVIMENTO|SOLUCOES|ENGENHARIA|ASSESSORIA|EMPRESARIAL|DIGITAL|SOFTWARE|SISTEMAS|DO|DA|DE|E)\b",
        " ",
    )
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


df_base = (
    spark.table(SILVER_TB_REUNIOES_CONTRATACAO)
    .select(
        F.col("empresa_contratacao").cast("string").alias("nm_empresa_contratacao"),
        F.col("consultoria_match").cast("string").alias("nm_consultoria_match"),
        F.col("consultoria").cast("string").alias("nm_consultoria_origem"),
        F.col("empresa_cliente_match").cast("string").alias("nm_empresa_cliente_match"),
        F.col("empresa_portfolio_mapeada").cast("string").alias("nm_empresa_portfolio_mapeada"),
        F.col("status_match_empresa").cast("string").alias("status_match_empresa"),
    )
    .filter(F.trim(F.coalesce(F.col("nm_empresa_contratacao"), F.lit(""))) != "")
    .withColumn("empresa_contratacao_key", empresa_name_key_expr(F.col("nm_empresa_contratacao")))
    .withColumn("empresa_contratacao_base_key", empresa_base_key_expr(F.col("nm_empresa_contratacao")))
    .withColumn("empresa_cliente_key", empresa_name_key_expr(F.col("nm_empresa_cliente_match")))
    .withColumn("empresa_portfolio_key", empresa_name_key_expr(F.col("nm_empresa_portfolio_mapeada")))
)

if table_exists(GOLD_DIM_EMPRESA):
    df_empresa = (
        spark.table(GOLD_DIM_EMPRESA)
        .select(
            F.col("empresa_id").cast("bigint").alias("empresa_id"),
            F.col("nm_empresa").cast("string").alias("nm_empresa_dim"),
        )
        .withColumn("empresa_key_dim", empresa_name_key_expr(F.col("nm_empresa_dim")))
        .withColumn("empresa_base_key_dim", empresa_base_key_expr(F.col("nm_empresa_dim")))
    )
else:
    df_empresa = spark.createDataFrame([], "empresa_id bigint, nm_empresa_dim string, empresa_key_dim string, empresa_base_key_dim string")

df_match_exato = (
    df_base.alias("b")
    .join(
        df_empresa.alias("e"),
        (
            (F.col("b.empresa_portfolio_key").isNotNull() & (F.col("b.empresa_portfolio_key") == F.col("e.empresa_key_dim")))
            | (F.col("b.empresa_cliente_key").isNotNull() & (F.col("b.empresa_cliente_key") == F.col("e.empresa_key_dim")))
            | (F.col("b.empresa_contratacao_key").isNotNull() & (F.col("b.empresa_contratacao_key") == F.col("e.empresa_key_dim")))
        ),
        "left",
    )
    .withColumn("match_priority", F.lit(1))
)

df_match_base = (
    df_base.alias("b")
    .join(
        df_empresa.alias("e"),
        F.col("b.empresa_contratacao_base_key").isNotNull()
        & (F.col("b.empresa_contratacao_base_key") == F.col("e.empresa_base_key_dim")),
        "left",
    )
    .withColumn("match_priority", F.lit(2))
)

w_match = Window.partitionBy("empresa_contratacao_key").orderBy(
    F.col("match_priority").asc(),
    F.col("empresa_id").asc_nulls_last(),
)

df_gold = (
    df_match_exato
    .unionByName(df_match_base)
    .withColumn("rn_match", F.row_number().over(w_match))
    .filter(F.col("rn_match") == 1)
    .drop("rn_match", "match_priority", "empresa_key_dim", "empresa_base_key_dim")
    .dropDuplicates(["empresa_contratacao_key"])
    .withColumn(
        "tp_empresa_contratacao",
        F.when(F.trim(F.coalesce(F.col("nm_consultoria_match"), F.lit(""))) != "", F.lit("CONSULTORIA"))
        .when(F.trim(F.coalesce(F.col("nm_consultoria_origem"), F.lit(""))) != "", F.lit("CONSULTORIA"))
        .when(F.trim(F.coalesce(F.col("nm_empresa_cliente_match"), F.lit(""))) != "", F.lit("EMPRESA_CLIENTE"))
        .otherwise(F.lit("OUTRA")),
    )
    .withColumn(
        "id_empresa_contratacao",
        F.row_number().over(
            Window.orderBy(
                F.col("nm_empresa_contratacao").asc_nulls_last(),
                F.col("empresa_contratacao_key").asc_nulls_last(),
            )
        ).cast("bigint"),
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.col("id_empresa_contratacao").cast("bigint").alias("id_empresa_contratacao"),
        F.col("nm_empresa_contratacao").cast("string").alias("nm_empresa_contratacao"),
        F.col("tp_empresa_contratacao").cast("string").alias("tp_empresa_contratacao"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nm_empresa_dim").cast("string").alias("nm_empresa_match"),
        F.col("nm_consultoria_match").cast("string").alias("nm_consultoria_match"),
        F.col("nm_consultoria_origem").cast("string").alias("nm_consultoria_origem"),
        F.col("nm_empresa_cliente_match").cast("string").alias("nm_empresa_cliente_match"),
        F.col("nm_empresa_portfolio_mapeada").cast("string").alias("nm_empresa_portfolio_mapeada"),
        F.col("status_match_empresa").cast("string").alias("status_match_empresa"),
        F.col("empresa_contratacao_key").cast("string").alias("empresa_contratacao_key"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_EMPRESA_CONTRATACAO} (
    id_empresa_contratacao BIGINT,
    nm_empresa_contratacao STRING,
    tp_empresa_contratacao STRING,
    empresa_id BIGINT,
    nm_empresa_match STRING,
    nm_consultoria_match STRING,
    nm_consultoria_origem STRING,
    nm_empresa_cliente_match STRING,
    nm_empresa_portfolio_mapeada STRING,
    status_match_empresa STRING,
    empresa_contratacao_key STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_EMPRESA_CONTRATACAO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_EMPRESA_CONTRATACAO)
