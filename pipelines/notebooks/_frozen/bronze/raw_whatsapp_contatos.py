# Databricks notebook source
# frozen bronze: raw_whatsapp_contatos

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

import os
import re
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

BRONZE_RAW_WHATSAPP_CONTATOS = f"{CATALOG}.{BRONZE}.raw_whatsapp_contatos"
WHATSAPP_CONTATOS_DIR = f"{REFERENCIA_DIR}/whatsapp_contatos"
WHATSAPP_CONTATOS_BASE_FILE = f"{WHATSAPP_CONTATOS_DIR}/Base_Whatsapp_Contatos.csv"


def _read_csv_local(path: str):
    best_df = None
    best_width = 0
    for sep in [",", ";", "|", "\t"]:
        for enc in ["utf-8", "utf-8-sig", "cp1252", "latin-1"]:
            try:
                df = (
                    spark.read
                    .option("header", "true")
                    .option("sep", sep)
                    .option("encoding", enc)
                    .option("quote", '"')
                    .option("escape", '"')
                    .csv(path)
                )
                cols = [c.replace("\ufeff", "").strip() for c in df.columns]
                if len(cols) > best_width:
                    best_df = df.toDF(*cols)
                    best_width = len(cols)
                if len(cols) > 1:
                    return df.toDF(*cols)
            except Exception:
                pass
    if best_df is not None:
        return best_df
    raise ValueError(f"Falha ao ler CSV {path}")


def _canon(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\ufeff", "").strip().lower()
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text)


def _pick_col(columns, *candidates):
    canon_map = {_canon(col): col for col in columns}
    for candidate in candidates:
        picked = canon_map.get(_canon(candidate))
        if picked:
            return picked
    return None


def _col_or_null(columns, *candidates):
    picked = _pick_col(columns, *candidates)
    return F.col(picked).cast("string") if picked else F.lit(None).cast("string")


def _project(df_raw, arquivo_origem: str):
    cols = df_raw.columns
    return (
        df_raw.select(
            _col_or_null(cols, "ID_MENSAGEM", "MESSAGE_ID", "MSG_ID", "ID").alias("mensagem_id_raw"),
            _col_or_null(cols, "DATA_MENSAGEM", "DATA", "DT_MENSAGEM", "TIMESTAMP", "DATA_HORA").alias("data_mensagem_raw"),
            _col_or_null(cols, "TELEFONE", "CELULAR", "WHATSAPP", "NUMERO_TELEFONE").alias("telefone_raw"),
            _col_or_null(cols, "NOME_CONTATO", "CONTATO", "NOME", "REMETENTE").alias("nome_contato_raw"),
            _col_or_null(cols, "CONSULTORIA", "EMPRESA_CONTRATACAO", "EMPRESA_RECRUTADORA").alias("consultoria_raw"),
            _col_or_null(cols, "EMPRESA", "EMPRESA_CLIENTE", "CLIENTE").alias("empresa_raw"),
            _col_or_null(cols, "EMAIL", "EMAIL_CONTATO", "EMAIL_RESPONSAVEL").alias("email_contato_raw"),
            _col_or_null(cols, "MENSAGEM", "TEXTO", "BODY", "CONTEUDO").alias("mensagem_raw"),
            _col_or_null(cols, "DIRECAO", "SENTIDO", "TIPO_DIRECAO").alias("direcao_raw"),
            _col_or_null(cols, "ORIGEM", "FONTE", "CANAL_ORIGEM").alias("origem_raw"),
        )
        .withColumn("arquivo_origem", F.lit(arquivo_origem).cast("string"))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(
            F.col("data_mensagem_raw").isNotNull()
            | F.col("telefone_raw").isNotNull()
            | F.col("mensagem_raw").isNotNull()
            | F.col("consultoria_raw").isNotNull()
            | F.col("empresa_raw").isNotNull()
        )
    )


schema = StructType(
    [
        StructField("mensagem_id_raw", StringType(), True),
        StructField("data_mensagem_raw", StringType(), True),
        StructField("telefone_raw", StringType(), True),
        StructField("nome_contato_raw", StringType(), True),
        StructField("consultoria_raw", StringType(), True),
        StructField("empresa_raw", StringType(), True),
        StructField("email_contato_raw", StringType(), True),
        StructField("mensagem_raw", StringType(), True),
        StructField("direcao_raw", StringType(), True),
        StructField("origem_raw", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

if os.path.exists(WHATSAPP_CONTATOS_BASE_FILE):
    df_bronze = _project(_read_csv_local(WHATSAPP_CONTATOS_BASE_FILE), os.path.basename(WHATSAPP_CONTATOS_BASE_FILE))
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_RAW_WHATSAPP_CONTATOS, catalog_schema=f"{CATALOG}.{BRONZE}")
