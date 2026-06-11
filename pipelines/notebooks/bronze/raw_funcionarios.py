# Databricks notebook source
# bronze: raw_funcionarios

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

BRONZE_RAW_FUNCIONARIOS = f"{CATALOG}.{BRONZE}.raw_funcionarios"


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
            _col_or_null(cols, "NOME_FUNCIONARIO", "NOME", "FUNCIONARIO").alias("nome_funcionario_raw"),
            _col_or_null(cols, "TELEFONE", "CELULAR", "WHATSAPP", "NUMERO_TELEFONE").alias("telefone_raw"),
            _col_or_null(cols, "SENIORIDADE").alias("senioridade_raw"),
        )
        .withColumn("arquivo_origem", F.lit(arquivo_origem))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(
            F.col("nome_funcionario_raw").isNotNull()
            | F.col("telefone_raw").isNotNull()
            | F.col("senioridade_raw").isNotNull()
        )
    )


schema = StructType(
    [
        StructField("nome_funcionario_raw", StringType(), True),
        StructField("telefone_raw", StringType(), True),
        StructField("senioridade_raw", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

expected_files = {
    os.path.basename(FUNCIONARIOS_BASE_FILE).lower(),
    os.path.basename(FUNCIONARIOS_BASE_FILE).replace(".csv", ".xlsx").lower(),
}
paths = [
    path
    for path in list_files_by_extension(FUNCIONARIOS_REFERENCIA_DIR, [".csv", ".xlsx"])
    if os.path.basename(path).lower() in expected_files
]

dfs = []
for path in paths:
    source_file = os.path.basename(path)
    if path.lower().endswith(".xlsx"):
        dfs.append(_project(read_xlsx_auto(path), source_file))
    else:
        dfs.append(_project(_read_csv_local(path), source_file))

if dfs:
    df_bronze = dfs[0]
    for df_next in dfs[1:]:
        df_bronze = df_bronze.unionByName(df_next)
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_RAW_FUNCIONARIOS, catalog_schema=f"{CATALOG}.{BRONZE}")
