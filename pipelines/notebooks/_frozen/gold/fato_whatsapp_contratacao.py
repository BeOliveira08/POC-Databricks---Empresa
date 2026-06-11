# Databricks notebook source
# frozen gold: fato_whatsapp_contratacao

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_WHATSAPP_CONTATOS = f"{CATALOG}.{SILVER}.tb_whatsapp_contatos"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_DIM_EMPRESA_CONTRATACAO = f"{CATALOG}.{GOLD}.dim_empresa_contratacao"
GOLD_FATO_REUNIOES_CONTRATACAO = f"{CATALOG}.{GOLD}.fato_reunioes_contratacao"
GOLD_FATO_WHATSAPP_CONTRATACAO = f"{CATALOG}.{GOLD}.fato_whatsapp_contratacao"

if not table_exists(SILVER_TB_WHATSAPP_CONTATOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_WHATSAPP_CONTATOS}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")


def key_expr(col_expr):
    return F.upper(F.regexp_replace(F.coalesce(col_expr.cast("string"), F.lit("")), r"[^A-Z0-9]", ""))


df_data = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").cast("int").alias("sk_data_contato"),
    F.col("dt_referencia").cast("date").alias("dt_contato_ref"),
)

if table_exists(GOLD_DIM_EMPRESA_CONTRATACAO):
    df_empresa_contratacao = spark.table(GOLD_DIM_EMPRESA_CONTRATACAO).select(
        F.col("id_empresa_contratacao").cast("bigint").alias("sk_empresa_contratacao"),
        F.col("empresa_contratacao_key").cast("string").alias("empresa_contratacao_key_dim"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
    )
else:
    df_empresa_contratacao = spark.createDataFrame([], "sk_empresa_contratacao bigint, empresa_contratacao_key_dim string, empresa_id bigint")

if table_exists(GOLD_FATO_REUNIOES_CONTRATACAO):
    df_reunioes = (
        spark.table(GOLD_FATO_REUNIOES_CONTRATACAO)
        .select(
            F.col("sk_reuniao_contratacao").cast("bigint").alias("sk_reuniao_contratacao"),
            F.col("sk_empresa_contratacao").cast("bigint").alias("sk_empresa_contratacao_reuniao"),
            F.col("processo_seletivo_key").cast("string").alias("processo_seletivo_key"),
            F.col("evento_ref_ts").cast("timestamp").alias("evento_ref_ts"),
        )
        .withColumn("dt_reuniao", F.to_date("evento_ref_ts"))
    )
else:
    df_reunioes = spark.createDataFrame([], "sk_reuniao_contratacao bigint, sk_empresa_contratacao_reuniao bigint, processo_seletivo_key string, evento_ref_ts timestamp, dt_reuniao date")

df_base = (
    spark.table(SILVER_TB_WHATSAPP_CONTATOS)
    .withColumn("empresa_contratacao_key_join", key_expr(F.col("empresa_contratacao")))
    .join(
        df_empresa_contratacao,
        F.col("empresa_contratacao_key_join") == F.col("empresa_contratacao_key_dim"),
        "left",
    )
    .join(df_data, F.col("data_mensagem").cast("date") == F.col("dt_contato_ref"), "left")
)

w_reuniao = Window.partitionBy("id_whatsapp_contato").orderBy(
    F.abs(F.datediff(F.col("dt_reuniao"), F.col("data_mensagem"))).asc_nulls_last(),
    F.col("sk_reuniao_contratacao").asc_nulls_last(),
)

df_match_reuniao = (
    df_base.alias("w")
    .join(
        df_reunioes.alias("r"),
        (F.col("w.sk_empresa_contratacao") == F.col("r.sk_empresa_contratacao_reuniao"))
        & F.col("w.sk_empresa_contratacao").isNotNull()
        & F.col("w.data_mensagem").isNotNull()
        & F.col("r.dt_reuniao").between(F.date_add(F.col("w.data_mensagem"), -30), F.date_add(F.col("w.data_mensagem"), 30)),
        "left",
    )
    .withColumn("rn_reuniao", F.row_number().over(w_reuniao))
    .filter(F.col("rn_reuniao") == 1)
    .drop("rn_reuniao")
)

df_gold = (
    df_match_reuniao
    .withColumn(
        "fl_match_reuniao",
        F.when(F.col("sk_reuniao_contratacao").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )
    .withColumn(
        "sk_whatsapp_contato",
        F.row_number().over(
            Window.orderBy(
                F.col("data_mensagem_ts").asc_nulls_last(),
                F.col("id_whatsapp_contato").asc_nulls_last(),
            )
        ).cast("bigint"),
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.col("sk_whatsapp_contato").cast("bigint").alias("sk_whatsapp_contato"),
        F.col("id_whatsapp_contato").cast("bigint").alias("id_whatsapp_contato"),
        F.col("sk_data_contato").cast("int").alias("sk_data_contato"),
        F.col("sk_empresa_contratacao").cast("bigint").alias("sk_empresa_contratacao"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_reuniao_contratacao").cast("bigint").alias("sk_reuniao_contratacao"),
        F.col("processo_seletivo_key").cast("string").alias("processo_seletivo_key"),
        F.col("telefone").cast("string").alias("telefone"),
        F.col("nome_contato").cast("string").alias("nome_contato"),
        F.col("email_contato").cast("string").alias("email_contato"),
        F.col("consultoria").cast("string").alias("consultoria"),
        F.col("empresa").cast("string").alias("empresa"),
        F.col("empresa_contratacao").cast("string").alias("empresa_contratacao"),
        F.col("tp_evento_whatsapp").cast("string").alias("tp_evento_whatsapp"),
        F.col("direcao").cast("string").alias("direcao"),
        F.col("origem").cast("string").alias("origem"),
        F.col("mensagem").cast("string").alias("mensagem"),
        F.col("fl_match_reuniao").cast("int").alias("fl_match_reuniao"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_WHATSAPP_CONTRATACAO} (
    sk_whatsapp_contato BIGINT,
    id_whatsapp_contato BIGINT,
    sk_data_contato INT,
    sk_empresa_contratacao BIGINT,
    empresa_id BIGINT,
    sk_reuniao_contratacao BIGINT,
    processo_seletivo_key STRING,
    telefone STRING,
    nome_contato STRING,
    email_contato STRING,
    consultoria STRING,
    empresa STRING,
    empresa_contratacao STRING,
    tp_evento_whatsapp STRING,
    direcao STRING,
    origem STRING,
    mensagem STRING,
    fl_match_reuniao INT,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_WHATSAPP_CONTRATACAO)
