# Databricks notebook source
# bronze: empresas_planilha

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/readers

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

import os
import re
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

BRONZE_EMPRESAS_PLANILHA = f"{CATALOG}.{BRONZE}.raw_empresas"
EMPRESAS_GERAL_DIR = f"{EMPRESAS_DIR}/Geral"


def _read_csv_local(path: str):
    best_df = None
    best_width = 0
    for sep in [",", ";", "|", "\t"]:
        df = (
            spark.read
            .option("header", "true")
            .option("sep", sep)
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .csv(path)
        )
        cols = [c.strip() for c in df.columns]
        if len(cols) > best_width:
            best_df = df.toDF(*cols)
            best_width = len(cols)
        if len(cols) > 1:
            return df.toDF(*cols)
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


def _project_geral(df_raw, arquivo_origem: str):
    cols = df_raw.columns
    return (
        df_raw.select(
            _col_or_null(cols, "CONSULTORIA").alias("consultoria_raw"),
            _col_or_null(cols, "EMPRESA_CLIENTE").alias("empresa_cliente_raw"),
            _col_or_null(cols, "TIPO_CONTRATO").alias("tipo_contrato_raw"),
            _col_or_null(cols, "MOTIVO_SAIDA").alias("motivo_saida_raw"),
        )
        .withColumn("arquivo_origem", F.lit(arquivo_origem))
        .withColumn("ingested_at", F.current_timestamp())
    )


schema = StructType(
    [
        StructField("consultoria_raw", StringType(), True),
        StructField("empresa_cliente_raw", StringType(), True),
        StructField("tipo_contrato_raw", StringType(), True),
        StructField("motivo_saida_raw", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

paths = list_files_by_extension(EMPRESAS_GERAL_DIR, [".csv"])
dfs = []
for path in paths:
    dfs.append(_project_geral(_read_csv_local(path), os.path.basename(path)))

if dfs:
    df_bronze = dfs[0]
    for df_next in dfs[1:]:
        df_bronze = df_bronze.unionByName(df_next)
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_EMPRESAS_PLANILHA, catalog_schema=f"{CATALOG}.{BRONZE}")
