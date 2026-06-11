# Databricks notebook source
# bronze: reunioes_contratacao_planilha

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

import os

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

BRONZE_REUNIOES_CONTRATACAO = f"{CATALOG}.{BRONZE}.raw_entrevistas"
BASE_REUNIOES_PATH = f"{REUNIOES_CONTRATACAO_DIR}/Base_Reunioes_Contratacao.csv"
BASE_REUNIOES_MATCHES_PATH = f"{REUNIOES_CONTRATACAO_DIR}/Base_Reunioes_Contratacao.matches.csv"


def _read_csv_local(path: str):
    separators = [",", ";", "|", "\t"]
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    last_error = None

    for encoding in encodings:
        for sep in separators:
            try:
                df_try = (
                    spark.read
                    .option("header", True)
                    .option("sep", sep)
                    .option("encoding", encoding)
                    .option("quote", '"')
                    .option("escape", '"')
                    .csv(path)
                )
                if len(df_try.columns) > 1:
                    renamed = df_try
                    for col_name in renamed.columns:
                        cleaned = str(col_name).replace("\ufeff", "").strip()
                        if cleaned != col_name:
                            renamed = renamed.withColumnRenamed(col_name, cleaned)
                    return renamed
            except Exception as exc:
                last_error = exc

    if last_error:
        raise last_error
    raise ValueError(f"Nao foi possivel ler CSV: {path}")


schema = StructType(
    [
        StructField("event_id_raw", StringType(), True),
        StructField("data_entrevista_raw", StringType(), True),
        StructField("consultoria_raw", StringType(), True),
        StructField("empresa_origem_raw", StringType(), True),
        StructField("empresa_relacionada_raw", StringType(), True),
        StructField("empresa_chave_raw", StringType(), True),
        StructField("responsavel_raw", StringType(), True),
        StructField("email_responsavel_raw", StringType(), True),
        StructField("em_copia_raw", StringType(), True),
        StructField("assunto_raw", StringType(), True),
        StructField("plataforma_raw", StringType(), True),
        StructField("link_reuniao_raw", StringType(), True),
        StructField("status_match_empresa_raw", StringType(), True),
        StructField("consultoria_match_raw", StringType(), True),
        StructField("empresa_cliente_match_raw", StringType(), True),
        StructField("empresa_portfolio_mapeada_raw", StringType(), True),
        StructField("best_consultoria_score_raw", StringType(), True),
        StructField("best_consultoria_source_raw", StringType(), True),
        StructField("best_cliente_score_raw", StringType(), True),
        StructField("best_cliente_source_raw", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

if os.path.basename(BASE_REUNIOES_PATH):
    df_raw = _read_csv_local(BASE_REUNIOES_PATH)
    if os.path.exists(BASE_REUNIOES_MATCHES_PATH):
        df_matches = (
            _read_csv_local(BASE_REUNIOES_MATCHES_PATH)
            .select(
                F.col("event_id").cast("string").alias("event_id_match"),
                F.col("consultoria_match").cast("string").alias("consultoria_match_raw"),
                F.col("empresa_cliente_match").cast("string").alias("empresa_cliente_match_raw"),
                F.col("empresa_portfolio_mapeada").cast("string").alias("empresa_portfolio_mapeada_raw"),
                F.col("best_consultoria_score").cast("string").alias("best_consultoria_score_raw"),
                F.col("best_consultoria_source").cast("string").alias("best_consultoria_source_raw"),
                F.col("best_cliente_score").cast("string").alias("best_cliente_score_raw"),
                F.col("best_cliente_source").cast("string").alias("best_cliente_source_raw"),
                F.col("status_match_empresa").cast("string").alias("status_match_empresa_match_raw"),
            )
        )
        df_raw = df_raw.join(df_matches, df_raw["event_id"] == df_matches["event_id_match"], "left")
    else:
        for col_name in [
            "consultoria_match_raw",
            "empresa_cliente_match_raw",
            "empresa_portfolio_mapeada_raw",
            "best_consultoria_score_raw",
            "best_consultoria_source_raw",
            "best_cliente_score_raw",
            "best_cliente_source_raw",
            "status_match_empresa_match_raw",
        ]:
            df_raw = df_raw.withColumn(col_name, F.lit(None).cast("string"))
    df_bronze = (
        df_raw.select(
            F.col("event_id").cast("string").alias("event_id_raw"),
            F.col("data_entrevista").cast("string").alias("data_entrevista_raw"),
            F.col("consultoria").cast("string").alias("consultoria_raw"),
            F.col("empresa_origem").cast("string").alias("empresa_origem_raw"),
            F.col("empresa_relacionada").cast("string").alias("empresa_relacionada_raw"),
            F.col("empresa_chave").cast("string").alias("empresa_chave_raw"),
            F.col("responsavel").cast("string").alias("responsavel_raw"),
            F.col("email_responsavel").cast("string").alias("email_responsavel_raw"),
            F.col("em_copia").cast("string").alias("em_copia_raw"),
            F.col("assunto").cast("string").alias("assunto_raw"),
            F.col("plataforma").cast("string").alias("plataforma_raw"),
            F.col("link_reuniao").cast("string").alias("link_reuniao_raw"),
            F.coalesce(F.col("status_match_empresa_match_raw"), F.col("status_match_empresa")).cast("string").alias("status_match_empresa_raw"),
            F.col("consultoria_match_raw").cast("string").alias("consultoria_match_raw"),
            F.col("empresa_cliente_match_raw").cast("string").alias("empresa_cliente_match_raw"),
            F.col("empresa_portfolio_mapeada_raw").cast("string").alias("empresa_portfolio_mapeada_raw"),
            F.col("best_consultoria_score_raw").cast("string").alias("best_consultoria_score_raw"),
            F.col("best_consultoria_source_raw").cast("string").alias("best_consultoria_source_raw"),
            F.col("best_cliente_score_raw").cast("string").alias("best_cliente_score_raw"),
            F.col("best_cliente_source_raw").cast("string").alias("best_cliente_source_raw"),
            F.lit(os.path.basename(BASE_REUNIOES_PATH)).cast("string").alias("arquivo_origem"),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_REUNIOES_CONTRATACAO, catalog_schema=f"{CATALOG}.{BRONZE}")
