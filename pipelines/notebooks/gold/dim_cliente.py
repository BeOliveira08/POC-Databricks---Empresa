# Databricks notebook source
# gold: dim_cliente

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------
# MAGIC %run ../../utils/company

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_CLIENTE_REFERENCIA = f"{CATALOG}.{SILVER}.tb_cliente_referencia"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_CLIENTE = f"{CATALOG}.{GOLD}.dim_cliente"

if not table_exists(SILVER_TB_CLIENTE_REFERENCIA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_CLIENTE_REFERENCIA}")


def clean_text_expr(col_name: str):
    txt = F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    return F.when(
        txt.isNull() | (txt == "") | F.upper(txt).isin("N/A", "NA", "NAO_INFORMADO"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


resolve_cliente_nome_udf = F.udf(
    lambda nome: (resolve_empresa_alias(nome, None)[0] if nome else None),
    "string",
)


def canonical_cliente_expr(col_name: str):
    nome = clean_text_expr(col_name)
    resolved = resolve_cliente_nome_udf(nome)
    return F.when(
        resolved.isNull() | (F.trim(resolved) == "") | (resolved == F.lit("NAO_IDENTIFICADA")),
        nome,
    ).otherwise(resolved)


def cliente_key_expr(col_name: str):
    txt = F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9]", "")
    return F.when(txt != "", txt)


def clean_date_expr(col_name: str):
    return F.when(
        F.col(col_name).cast("date") <= F.to_date(F.lit("1900-01-01")),
        F.lit(None).cast("date"),
    ).otherwise(F.col(col_name).cast("date"))


w_cliente_best = Window.partitionBy("cliente_nome_key").orderBy(
    F.length(F.coalesce(F.col("nm_cliente"), F.lit(""))).asc(),
    F.col("nm_cliente").asc_nulls_last(),
    F.col("dt_inicio").asc_nulls_last(),
)
w = Window.orderBy(
    F.col("nm_cliente").asc_nulls_last(),
    F.col("dt_primeira_evidencia").asc_nulls_last(),
)

df_clientes_base = (
    spark.table(SILVER_TB_CLIENTE_REFERENCIA)
    .select(
        canonical_cliente_expr("nm_cliente").alias("nm_cliente"),
        clean_date_expr("dt_inicio").alias("dt_inicio"),
        clean_date_expr("dt_fim").alias("dt_fim"),
        F.col("fl_periodo_provisorio").cast("int").alias("fl_periodo_provisorio"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .withColumn("cliente_nome_key", cliente_key_expr("nm_cliente"))
    .filter(F.col("nm_cliente").isNotNull())
    .filter(F.col("cliente_nome_key").isNotNull())
)

df_consultorias_referencia = (
    spark.table(SILVER_TB_CLIENTE_REFERENCIA)
    .select(canonical_cliente_expr("nm_consultoria").alias("nm_entidade"))
    .withColumn("cliente_nome_key", cliente_key_expr("nm_entidade"))
    .filter(F.col("cliente_nome_key").isNotNull())
    .select("cliente_nome_key")
    .distinct()
)

df_clientes_observados = (
    spark.table(SILVER_TB_CLIENTE_REFERENCIA)
    .select(canonical_cliente_expr("nm_cliente").alias("nm_entidade"))
    .withColumn("cliente_nome_key", cliente_key_expr("nm_entidade"))
    .filter(F.col("cliente_nome_key").isNotNull())
    .select("cliente_nome_key")
    .distinct()
)

if table_exists(SILVER_TB_EMPRESA):
    df_consultorias_empresa = (
        spark.table(SILVER_TB_EMPRESA)
        .select(
            canonical_cliente_expr("nm_empresa").alias("nm_entidade"),
            F.col("dt_primeira_evidencia").cast("date").alias("dt_primeira_evidencia"),
            clean_text_expr("tp_empresa").alias("tp_empresa"),
            clean_text_expr("tp_empresa_papeis").alias("tp_empresa_papeis"),
        )
        .withColumn("cliente_nome_key", cliente_key_expr("nm_entidade"))
        .filter(F.col("nm_entidade").isNotNull())
        .filter(F.col("cliente_nome_key").isNotNull())
        .filter(F.col("dt_primeira_evidencia") >= F.to_date(F.lit("2021-01-01")))
        .filter(
            F.upper(F.coalesce(F.col("tp_empresa"), F.lit(""))).contains("CONSULTORIA")
            | F.upper(F.coalesce(F.col("tp_empresa_papeis"), F.lit(""))).contains("CONSULTORIA")
        )
        .select("cliente_nome_key")
        .distinct()
    )
    df_consultorias_excluir = df_consultorias_referencia.unionByName(df_consultorias_empresa).distinct()
else:
    df_consultorias_excluir = df_consultorias_referencia

df_consultorias_excluir = (
    df_consultorias_excluir
    .join(df_clientes_observados, on="cliente_nome_key", how="left_anti")
)

df_clientes_filtrados = (
    df_clientes_base
    .join(df_consultorias_excluir, on="cliente_nome_key", how="left_anti")
)

df_clientes_best = (
    df_clientes_filtrados
    .withColumn("rn_cliente_best", F.row_number().over(w_cliente_best))
    .filter(F.col("rn_cliente_best") == 1)
    .select(
        "cliente_nome_key",
        F.col("nm_cliente").alias("nm_cliente_canonico"),
    )
)

df_gold = (
    df_clientes_filtrados
    .groupBy("cliente_nome_key")
    .agg(
        F.min("dt_inicio").alias("dt_primeira_evidencia"),
        F.max("dt_fim").alias("dt_ultima_evidencia"),
        F.max("fl_periodo_provisorio").cast("int").alias("fl_periodo_provisorio"),
        F.max("pipeline_run_id_origem").alias("pipeline_run_id_origem"),
        F.max("ingested_at_origem").alias("ingested_at_origem"),
    )
    .join(df_clientes_best, on="cliente_nome_key", how="left")
    .withColumn("nm_cliente", F.col("nm_cliente_canonico"))
    .drop("cliente_nome_key", "nm_cliente_canonico")
    .withColumn(
        "dt_ultima_evidencia",
        F.when(
            F.col("dt_primeira_evidencia").isNotNull()
            & F.col("dt_ultima_evidencia").isNotNull()
            & (F.col("dt_ultima_evidencia") < F.col("dt_primeira_evidencia")),
            F.lit(None).cast("date"),
        ).otherwise(F.col("dt_ultima_evidencia"))
    )
    .withColumn("cliente_id", F.row_number().over(w).cast("bigint"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.col("cliente_id").cast("bigint").alias("cliente_id"),
        F.col("nm_cliente").cast("string").alias("nm_cliente"),
        F.col("fl_periodo_provisorio").cast("int").alias("fl_periodo_provisorio"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_CLIENTE} (
    cliente_id BIGINT,
    nm_cliente STRING,
    fl_periodo_provisorio INT,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_DIM_CLIENTE)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_CLIENTE)
