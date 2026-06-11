# Databricks notebook source
# silver: tb_reunioes_contratacao

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_REUNIOES_CONTRATACAO = f"{CATALOG}.{BRONZE}.raw_entrevistas"
SILVER_TB_REUNIOES_CONTRATACAO = f"{CATALOG}.{SILVER}.tb_reunioes_contratacao"

if not table_exists(BRONZE_REUNIOES_CONTRATACAO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_REUNIOES_CONTRATACAO}")


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


def non_empty_expr(col_name: str):
    return F.when(F.trim(F.coalesce(F.col(col_name), F.lit(""))) != "", F.col(col_name))


def key_expr(col_name: str):
    return F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", "")


df_src = (
    spark.table(BRONZE_REUNIOES_CONTRATACAO)
    .select(
        F.col("event_id_raw").cast("string").alias("event_id_origem"),
        F.col("data_entrevista_raw").cast("string").alias("data_entrevista_raw"),
        normalized_text_expr("consultoria_raw").alias("consultoria"),
        normalized_text_expr("empresa_origem_raw").alias("empresa_origem"),
        normalized_text_expr("empresa_relacionada_raw").alias("empresa_relacionada"),
        normalized_text_expr("empresa_chave_raw").alias("empresa_chave"),
        repair_text("responsavel_raw").cast("string").alias("responsavel"),
        F.lower(F.trim(F.col("email_responsavel_raw").cast("string"))).alias("email_responsavel"),
        repair_text("em_copia_raw").cast("string").alias("em_copia"),
        repair_text("assunto_raw").cast("string").alias("assunto"),
        normalized_text_expr("plataforma_raw").alias("plataforma"),
        F.col("link_reuniao_raw").cast("string").alias("link_reuniao"),
        normalized_text_expr("status_match_empresa_raw").alias("status_match_empresa"),
        normalized_text_expr("consultoria_match_raw").alias("consultoria_match"),
        normalized_text_expr("empresa_cliente_match_raw").alias("empresa_cliente_match"),
        normalized_text_expr("empresa_portfolio_mapeada_raw").alias("empresa_portfolio_mapeada"),
        F.col("best_consultoria_score_raw").cast("double").alias("best_consultoria_score"),
        normalized_text_expr("best_consultoria_source_raw").alias("best_consultoria_source"),
        F.col("best_cliente_score_raw").cast("double").alias("best_cliente_score"),
        normalized_text_expr("best_cliente_source_raw").alias("best_cliente_source"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
    )
    .withColumn(
        "data_entrevista_ts",
        F.coalesce(
            F.expr("try_to_timestamp(data_entrevista_raw, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(data_entrevista_raw, 'dd/MM/yyyy HH:mm:ss')"),
            F.expr("try_to_timestamp(data_entrevista_raw)"),
            F.to_timestamp(
                F.concat(F.col("data_entrevista_raw"), F.lit(" 00:00:00")),
                "dd/MM/yyyy HH:mm:ss",
            ),
        ),
    )
    .withColumn("data_entrevista", F.to_date("data_entrevista_ts"))
    .withColumn(
        "empresa",
        F.coalesce(
            non_empty_expr("empresa_chave"),
            non_empty_expr("empresa_relacionada"),
            non_empty_expr("empresa_origem"),
        ),
    )
    .withColumn(
        "empresa_contratacao",
        F.coalesce(
            non_empty_expr("consultoria_match"),
            non_empty_expr("consultoria"),
            non_empty_expr("empresa_portfolio_mapeada"),
            non_empty_expr("empresa_relacionada"),
            non_empty_expr("empresa_origem"),
            non_empty_expr("empresa_chave"),
        ),
    )
    .withColumn("consultoria_key", key_expr("consultoria"))
    .withColumn("empresa_key", key_expr("empresa"))
    .withColumn("empresa_contratacao_key", key_expr("empresa_contratacao"))
    .withColumn("assunto_key", key_expr("assunto"))
    .withColumn("email_responsavel_key", key_expr("email_responsavel"))
    .filter(
        (F.trim(F.coalesce(F.col("assunto"), F.lit(""))) != "")
        | (F.trim(F.coalesce(F.col("consultoria"), F.lit(""))) != "")
        | (F.trim(F.coalesce(F.col("empresa"), F.lit(""))) != "")
    )
)

df_src = (
    df_src
    .withColumn(
        "consultoria_id_seed",
        F.when(F.coalesce(F.col("consultoria_key"), F.lit("")) != "", F.col("consultoria_key"))
    )
    .withColumn(
        "reuniao_dedup_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("consultoria_key"), F.lit("")),
            F.coalesce(F.col("empresa_key"), F.lit("")),
            F.coalesce(F.date_format(F.col("data_entrevista"), "yyyy-MM-dd"), F.lit("")),
            F.coalesce(F.col("assunto_key"), F.lit("")),
            F.coalesce(F.col("email_responsavel_key"), F.lit("")),
        ),
    )
)

w_consultoria_id = Window.orderBy(F.coalesce(F.col("consultoria_id_seed"), F.lit("")))
w_reuniao_id = Window.orderBy(F.coalesce(F.col("reuniao_dedup_key"), F.lit("")))

df_final = (
    df_src
    .groupBy("reuniao_dedup_key")
    .agg(
        F.max("consultoria_id_seed").alias("consultoria_id_seed"),
        F.max("event_id_origem").alias("event_id_origem"),
        F.max("data_entrevista").alias("data_entrevista"),
        F.max("data_entrevista_ts").alias("data_entrevista_ts"),
        F.first("consultoria", ignorenulls=True).alias("consultoria"),
        F.first("empresa", ignorenulls=True).alias("empresa"),
        F.first("responsavel", ignorenulls=True).alias("responsavel"),
        F.first("email_responsavel", ignorenulls=True).alias("email_responsavel"),
        F.first("em_copia", ignorenulls=True).alias("em_copia"),
        F.first("assunto", ignorenulls=True).alias("assunto"),
        F.first("plataforma", ignorenulls=True).alias("plataforma"),
        F.first("link_reuniao", ignorenulls=True).alias("link_reuniao"),
        F.first("status_match_empresa", ignorenulls=True).alias("status_match_empresa"),
        F.first("consultoria_match", ignorenulls=True).alias("consultoria_match"),
        F.first("empresa_cliente_match", ignorenulls=True).alias("empresa_cliente_match"),
        F.first("empresa_portfolio_mapeada", ignorenulls=True).alias("empresa_portfolio_mapeada"),
        F.max("best_consultoria_score").alias("best_consultoria_score"),
        F.first("best_consultoria_source", ignorenulls=True).alias("best_consultoria_source"),
        F.max("best_cliente_score").alias("best_cliente_score"),
        F.first("best_cliente_source", ignorenulls=True).alias("best_cliente_source"),
        F.first("empresa_contratacao", ignorenulls=True).alias("empresa_contratacao"),
        F.first("arquivo_origem", ignorenulls=True).alias("arquivo_origem"),
    )
    .withColumn(
        "consultoria_id",
        F.when(
            F.coalesce(F.col("consultoria_id_seed"), F.lit("")) != "",
            F.dense_rank().over(w_consultoria_id),
        ).otherwise(F.lit(None).cast("int"))
    )
    .withColumn(
        "consultoria",
        F.when(
            F.trim(F.coalesce(F.col("consultoria"), F.lit(""))) == "",
            F.lit("NAO ENCONTRADO"),
        ).otherwise(F.col("consultoria"))
    )
    .withColumn(
        "id_reuniao_contratacao",
        F.row_number().over(w_reuniao_id),
    )
    .drop("consultoria_id_seed")
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_reuniao_contratacao").cast("int").alias("id_reuniao_contratacao"),
        F.col("consultoria_id").cast("int").alias("consultoria_id"),
        F.col("event_id_origem").cast("string").alias("event_id_origem"),
        F.col("data_entrevista").cast("date").alias("data_entrevista"),
        F.col("data_entrevista_ts").cast("timestamp").alias("data_entrevista_ts"),
        F.col("consultoria").cast("string").alias("consultoria"),
        F.col("empresa").cast("string").alias("empresa"),
        F.col("responsavel").cast("string").alias("responsavel"),
        F.col("email_responsavel").cast("string").alias("email_responsavel"),
        F.col("em_copia").cast("string").alias("em_copia"),
        F.col("assunto").cast("string").alias("assunto"),
        F.col("plataforma").cast("string").alias("plataforma"),
        F.col("link_reuniao").cast("string").alias("link_reuniao"),
        F.col("status_match_empresa").cast("string").alias("status_match_empresa"),
        F.col("consultoria_match").cast("string").alias("consultoria_match"),
        F.col("empresa_cliente_match").cast("string").alias("empresa_cliente_match"),
        F.col("empresa_portfolio_mapeada").cast("string").alias("empresa_portfolio_mapeada"),
        F.col("best_consultoria_score").cast("double").alias("best_consultoria_score"),
        F.col("best_consultoria_source").cast("string").alias("best_consultoria_source"),
        F.col("best_cliente_score").cast("double").alias("best_cliente_score"),
        F.col("best_cliente_source").cast("string").alias("best_cliente_source"),
        F.col("empresa_contratacao").cast("string").alias("empresa_contratacao"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_REUNIOES_CONTRATACAO, catalog_schema=f"{CATALOG}.{SILVER}")
