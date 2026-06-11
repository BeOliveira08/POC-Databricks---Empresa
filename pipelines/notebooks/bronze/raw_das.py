# Databricks notebook source
# bronze: das

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/readers

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/pdf

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

import os
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

BRONZE_DAS = f"{CATALOG}.{BRONZE}.raw_das"

CNPJ_PATTERN = re.compile(r"(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})")
COMPETENCIA_PATTERN = re.compile(r"(\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(\d+)")
RAZAO_PATTERN = re.compile(r"(?m)^\s*\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\s+([^\n]+?)\s*$", re.IGNORECASE)
BANCO_ARREC_PATTERN = re.compile(r"([0-9]{3}\s*-\s*[^\n]+?)\s+(\d{2}/\d{2}/\d{4})", re.IGNORECASE)
ARRECADACAO_LABEL_PATTERN = re.compile(r"ARRECADA[ÇC][AÃ]O(?:\s+EM)?\s*:?\s*(\d{2}/\d{2}/\d{4})", re.IGNORECASE)
DATE_PATTERN = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
TRIB_LINE_PATTERN = re.compile(
    r"(?m)^\s*(\d{4})\s+(.+?)\s+([0-9\.\,]+|-)\s+([0-9\.\,]+|-)\s+([0-9\.\,]+|-)\s+([0-9\.\,]+|-)\s*$"
)


def resolve_dt_arrecadacao(texto: str, dt_vencimento_str: str):
    banco_m = BANCO_ARREC_PATTERN.search(texto or "")
    if banco_m:
        return banco_m.group(2)

    label_m = ARRECADACAO_LABEL_PATTERN.search(texto or "")
    if label_m:
        return label_m.group(1)

    datas = DATE_PATTERN.findall(texto or "")
    datas_validas = [d for d in datas if d != dt_vencimento_str]
    if datas_validas:
        return datas_validas[-1]

    return None


def parse_das_pdf(pdf_path: str):
    arquivo = os.path.basename(pdf_path)
    paginas = extract_pdf_pages(pdf_path)
    rows = []

    for texto in paginas:
        cnpj_m = CNPJ_PATTERN.search(texto or "")
        comp_m = COMPETENCIA_PATTERN.search(texto or "")
        razao_m = RAZAO_PATTERN.search(texto or "")

        cnpj_raw = clean_text(cnpj_m.group(1)) if cnpj_m else None
        competencia_raw = clean_text(comp_m.group(1)) if comp_m else None
        dt_vencimento_raw = clean_text(comp_m.group(2)) if comp_m else None
        nr_documento_raw = clean_text(comp_m.group(3)) if comp_m else None
        razao_social_raw = clean_text(razao_m.group(1)) if razao_m else None
        dt_arrecadacao_raw = resolve_dt_arrecadacao(texto or "", dt_vencimento_raw)

        for match in TRIB_LINE_PATTERN.finditer(texto or ""):
            rows.append(
                {
                    "cnpj_prestador": cnpj_raw,
                    "razao_social": razao_social_raw,
                    "competencia": competencia_raw,
                    "dt_vencimento": dt_vencimento_raw,
                    "dt_arrecadacao": dt_arrecadacao_raw,
                    "nr_documento": nr_documento_raw,
                    "cd_tributo": clean_text(match.group(1)),
                    "ds_tributo": clean_text(match.group(2)),
                    "vl_principal": clean_text(match.group(3)),
                    "vl_multa": clean_text(match.group(4)),
                    "vl_juros": clean_text(match.group(5)),
                    "vl_total": clean_text(match.group(6)),
                    "arquivo_origem": arquivo,
                }
            )

    return rows


schema = StructType(
    [
        StructField("cnpj_prestador", StringType(), True),
        StructField("razao_social", StringType(), True),
        StructField("competencia", StringType(), True),
        StructField("dt_vencimento", StringType(), True),
        StructField("dt_arrecadacao", StringType(), True),
        StructField("nr_documento", StringType(), True),
        StructField("cd_tributo", StringType(), True),
        StructField("ds_tributo", StringType(), True),
        StructField("vl_principal", StringType(), True),
        StructField("vl_multa", StringType(), True),
        StructField("vl_juros", StringType(), True),
        StructField("vl_total", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

pdfs = list_files_by_extension(DAS_DIR, [".pdf"])
rows = []
for pdf_path in pdfs:
    rows.extend(parse_das_pdf(pdf_path))

if rows:
    df_bronze = spark.createDataFrame(rows, schema=schema).withColumn("ingested_at", F.current_timestamp())
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_DAS, catalog_schema=f"{CATALOG}.{BRONZE}")
