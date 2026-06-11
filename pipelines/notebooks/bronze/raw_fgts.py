# Databricks notebook source
# bronze: fgts_extratos

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
# MAGIC %run ../../utils/fgts

# COMMAND ----------

import os

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

BRONZE_FGTS = f"{CATALOG}.{BRONZE}.raw_fgts"
TITULAR_CPF = "00000000000"
TITULAR_NOME_COMPLETO = "TITULAR GENERICO"


def parse_fgts_pdf(pdf_path: str):
    arquivo = os.path.basename(pdf_path)
    texto = extract_pdf_text(pdf_path)
    _, cpf_raw, pis_raw, cnpj_raw, conta_raw = parse_header(texto)
    linhas = [l.strip() for l in texto.splitlines() if l.strip()]
    nome_raw = clean_text(linhas[2]) if len(linhas) > 2 else None
    cpf_resolvido = cpf_raw
    if not cpf_resolvido and nome_raw == TITULAR_NOME_COMPLETO:
        cpf_resolvido = TITULAR_CPF
    empresa_raw = get_empresa_from_arquivo(arquivo)

    rows = []
    for dt_lancamento_raw, descricao_raw, valor_raw, saldo_raw in parse_movements(texto):
        rows.append(
            {
                "nome": nome_raw,
                "cpf": cpf_resolvido,
                "pis_pasep": pis_raw,
                "empresa": empresa_raw,
                "cnpj": cnpj_raw,
                "conta_fgts": conta_raw,
                "dt_lancamento": dt_lancamento_raw,
                "descricao": clean_desc(descricao_raw),
                "valor": clean_text(valor_raw),
                "saldo": clean_text(saldo_raw),
                "arquivo_origem": arquivo,
            }
        )
    return rows


schema = StructType(
    [
        StructField("nome", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("pis_pasep", StringType(), True),
        StructField("empresa", StringType(), True),
        StructField("cnpj", StringType(), True),
        StructField("conta_fgts", StringType(), True),
        StructField("dt_lancamento", StringType(), True),
        StructField("descricao", StringType(), True),
        StructField("valor", StringType(), True),
        StructField("saldo", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

pdfs = list_files_by_extension(FGTS_DIR, [".pdf"])
rows = []
for pdf_path in pdfs:
    rows.extend(parse_fgts_pdf(pdf_path))

if rows:
    df_bronze = spark.createDataFrame(rows, schema=schema).withColumn("ingested_at", F.current_timestamp())
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_FGTS, catalog_schema=f"{CATALOG}.{BRONZE}")
