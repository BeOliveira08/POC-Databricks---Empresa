# Databricks notebook source
# frozen silver: tb_whatsapp_contatos

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------
# MAGIC %run ../../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../../utils/text

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_WHATSAPP_CONTATOS = f"{CATALOG}.{BRONZE}.raw_whatsapp_contatos"
SILVER_TB_WHATSAPP_CONTATOS = f"{CATALOG}.{SILVER}.tb_whatsapp_contatos"

if not table_exists(BRONZE_RAW_WHATSAPP_CONTATOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_RAW_WHATSAPP_CONTATOS}")


def normalized_text_expr(col_name: str):
    return F.upper(
        F.trim(
            F.regexp_replace(
                F.coalesce(repair_text(col_name), F.lit("")),
                r"\s+",
                " ",
            )
        )
    )


def normalized_phone_expr(col_name: str):
    phone = F.regexp_replace(F.coalesce(repair_text(col_name), F.lit("")), r"\D+", "")
    phone = F.trim(phone)
    return F.when(F.length(phone) >= 10, phone).otherwise(F.lit(None).cast("string"))


def key_expr(col_name: str):
    return F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", "")


df_base = (
    spark.table(BRONZE_RAW_WHATSAPP_CONTATOS)
    .select(
        F.col("mensagem_id_raw").cast("string").alias("mensagem_id_origem"),
        F.col("data_mensagem_raw").cast("string").alias("data_mensagem_raw"),
        normalized_phone_expr("telefone_raw").alias("telefone"),
        repair_text("nome_contato_raw").cast("string").alias("nome_contato"),
        normalized_text_expr("consultoria_raw").alias("consultoria"),
        normalized_text_expr("empresa_raw").alias("empresa"),
        F.lower(F.trim(F.col("email_contato_raw").cast("string"))).alias("email_contato"),
        repair_text("mensagem_raw").cast("string").alias("mensagem"),
        normalized_text_expr("direcao_raw").alias("direcao"),
        normalized_text_expr("origem_raw").alias("origem"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
    )
    .withColumn(
        "data_mensagem_ts",
        F.coalesce(
            F.expr("try_to_timestamp(data_mensagem_raw, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(data_mensagem_raw, 'yyyy-MM-dd''T''HH:mm:ssXXX')"),
            F.expr("try_to_timestamp(data_mensagem_raw, 'dd/MM/yyyy HH:mm:ss')"),
            F.expr("try_to_timestamp(data_mensagem_raw)"),
        ),
    )
    .withColumn("data_mensagem", F.to_date("data_mensagem_ts"))
    .withColumn(
        "empresa_contratacao",
        F.coalesce(
            F.when(F.trim(F.coalesce(F.col("consultoria"), F.lit(""))) != "", F.col("consultoria")),
            F.when(F.trim(F.coalesce(F.col("empresa"), F.lit(""))) != "", F.col("empresa")),
            F.when(F.trim(F.coalesce(F.col("nome_contato"), F.lit(""))) != "", F.col("nome_contato")),
        ),
    )
    .withColumn(
        "tp_evento_whatsapp",
        F.when(F.upper(F.coalesce(F.col("mensagem"), F.lit(""))).contains("ENTREVISTA"), F.lit("ENTREVISTA"))
        .when(F.upper(F.coalesce(F.col("mensagem"), F.lit(""))).contains("FIT CULTURAL"), F.lit("FIT_CULTURAL"))
        .when(F.upper(F.coalesce(F.col("mensagem"), F.lit(""))).contains("ONBOARD"), F.lit("ONBOARDING"))
        .when(F.upper(F.coalesce(F.col("mensagem"), F.lit(""))).contains("TREINAMENTO"), F.lit("TREINAMENTO"))
        .when(F.upper(F.coalesce(F.col("mensagem"), F.lit(""))).contains("PROPOSTA"), F.lit("PROPOSTA"))
        .otherwise(F.lit("OUTRO"))
    )
    .withColumn(
        "mensagem_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("telefone"), F.lit("")),
            F.coalesce(F.col("empresa_contratacao"), F.lit("")),
            F.coalesce(F.col("mensagem"), F.lit("")),
            F.coalesce(F.date_format(F.col("data_mensagem_ts"), "yyyy-MM-dd HH:mm:ss"), F.lit("")),
        ),
    )
)

w_msg = Window.orderBy(F.coalesce(F.col("mensagem_key"), F.lit("")))

df_final = (
    df_base
    .filter(
        (F.trim(F.coalesce(F.col("mensagem"), F.lit(""))) != "")
        | F.col("telefone").isNotNull()
        | (F.trim(F.coalesce(F.col("empresa_contratacao"), F.lit(""))) != "")
    )
    .dropDuplicates(["mensagem_key"])
    .withColumn("id_whatsapp_contato", F.row_number().over(w_msg))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_whatsapp_contato").cast("bigint").alias("id_whatsapp_contato"),
        F.col("mensagem_id_origem").cast("string").alias("mensagem_id_origem"),
        F.col("data_mensagem").cast("date").alias("data_mensagem"),
        F.col("data_mensagem_ts").cast("timestamp").alias("data_mensagem_ts"),
        F.col("telefone").cast("string").alias("telefone"),
        F.col("nome_contato").cast("string").alias("nome_contato"),
        F.col("consultoria").cast("string").alias("consultoria"),
        F.col("empresa").cast("string").alias("empresa"),
        F.col("empresa_contratacao").cast("string").alias("empresa_contratacao"),
        F.col("email_contato").cast("string").alias("email_contato"),
        F.col("mensagem").cast("string").alias("mensagem"),
        F.col("direcao").cast("string").alias("direcao"),
        F.col("origem").cast("string").alias("origem"),
        F.col("tp_evento_whatsapp").cast("string").alias("tp_evento_whatsapp"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_WHATSAPP_CONTATOS, catalog_schema=f"{CATALOG}.{SILVER}")
