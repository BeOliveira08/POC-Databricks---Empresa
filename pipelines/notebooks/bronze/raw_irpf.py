# Databricks notebook source
# bronze: irpf_pdf

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../config/domain_config

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

BRONZE_IRPF_DECL = f"{CATALOG}.{BRONZE}.raw_irpf"

EXERCICIO_PATTERN = re.compile(r"IRPF-A-(\d{4})-(\d{4})", re.IGNORECASE)
CPF_PATTERN = re.compile(r"CPF[:\s]*([\d\.\-]{11,14})", re.IGNORECASE)
NOME_PATTERN = re.compile(r"Nome(?:\s+do\s+declarante)?[:\s]+([^\n]+)", re.IGNORECASE)
TIPO_DECL_PATTERN = re.compile(r"Tipo de declara\S*[:\s]+([^\n]+)", re.IGNORECASE)
FILE_DOC_PATTERN = re.compile(r"-(DEC|REC)\.pdf$", re.IGNORECASE)

IMPOSTO_RESTITUIR_PATTERNS = [
    re.compile(r"Imposto\s+a\s+restituir[:\s]*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Valor\s+da\s+restitui\S*[:\s]*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
]

IMPOSTO_PAGO_PATTERNS = [
    re.compile(r"Total\s+do\s+imposto\s+pago[:\s]*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Imposto\s+pago[:\s]*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
    re.compile(r"Saldo\s+do\s+imposto\s+a\s+pagar[:\s]*R?\$?\s*([\d\.\,]+)", re.IGNORECASE),
]


def clean_irpf_nome(value: str | None) -> str | None:
    nome = clean_text(value)
    if not nome:
        return None

    nome = re.sub(r"\s+", " ", nome).strip(" :-")
    upper = nome.upper()
    if (
        upper in {"NOME", "DO DECLARANTE"}
        or upper == "DATA DE NASCIMENTO"
        or upper == TITULAR_CPF
        or upper.startswith("IMPOSTO SOBRE A RENDA")
        or "DECLARAC" in upper
        or "PESSOA F" in upper
    ):
        return None
    return nome


def extract_irpf_nome(texto: str) -> str | None:
    texto = texto or ""

    nome_m = NOME_PATTERN.search(texto)
    nome = clean_irpf_nome(nome_m.group(1)) if nome_m else None
    if nome:
        return nome

    linhas = [clean_text(linha) for linha in texto.splitlines()]
    linhas = [linha for linha in linhas if linha]
    for idx, linha in enumerate(linhas):
        linha_upper = linha.upper()
        if linha_upper in {"NOME", "NOME DO DECLARANTE"} and idx + 1 < len(linhas):
            nome = clean_irpf_nome(linhas[idx + 1])
            if nome:
                return nome

        if linha_upper.startswith("NOME:") or linha_upper.startswith("NOME DO DECLARANTE:"):
            nome = clean_irpf_nome(re.sub(r"^NOME(?: DO DECLARANTE)?\s*:\s*", "", linha, flags=re.IGNORECASE))
            if nome:
                return nome

    return None


def normalize_cpf_text(value: str | None) -> str | None:
    digits = re.sub(r"\D", "", value or "")
    return digits if len(digits) == 11 else None


def extract_file_cpf(arquivo: str) -> str | None:
    digits = re.sub(r"\D", "", (arquivo or "").split("-IRPF", 1)[0])
    return digits if len(digits) == 11 else None


def extract_doc_variant(arquivo: str) -> str | None:
    match = FILE_DOC_PATTERN.search(arquivo or "")
    return match.group(1).upper() if match else None


def extract_money_by_patterns(texto: str, patterns: list[re.Pattern]) -> float | None:
    for pattern in patterns:
        match = pattern.search(texto or "")
        if not match:
            continue
        value = br_money_to_float(match.group(1))
        if value is not None:
            return float(value)
    return None


def parse_irpf_pdf(pdf_path: str):
    arquivo = os.path.basename(pdf_path)
    texto = extract_pdf_text(pdf_path)
    exercicio_m = EXERCICIO_PATTERN.search(arquivo)
    cpf_m = CPF_PATTERN.search(texto or "")
    tipo_decl_m = TIPO_DECL_PATTERN.search(texto or "")
    cpf_resolvido = normalize_cpf_text(cpf_m.group(1) if cpf_m else None) or extract_file_cpf(arquivo)
    nome_resolvido = extract_irpf_nome(texto)

    if cpf_resolvido == TITULAR_CPF and not nome_resolvido:
        nome_resolvido = TITULAR_NOME

    return {
        "arquivo_origem": arquivo,
        "cpf": cpf_resolvido,
        "nome": nome_resolvido,
        "ano_exercicio": exercicio_m.group(1) if exercicio_m else None,
        "ano_calendario": exercicio_m.group(2) if exercicio_m else None,
        "tipo_documento": "IRPF",
        "tipo_arquivo": extract_doc_variant(arquivo),
        "tipo_declaracao": clean_text(tipo_decl_m.group(1)) if tipo_decl_m else None,
        "imposto_pago_total": extract_money_by_patterns(texto, IMPOSTO_PAGO_PATTERNS),
        "imposto_restituir": extract_money_by_patterns(texto, IMPOSTO_RESTITUIR_PATTERNS),
    }


schema = StructType(
    [
        StructField("arquivo_origem", StringType(), True),
        StructField("cpf", StringType(), True),
        StructField("nome", StringType(), True),
        StructField("ano_exercicio", StringType(), True),
        StructField("ano_calendario", StringType(), True),
        StructField("tipo_documento", StringType(), True),
        StructField("tipo_arquivo", StringType(), True),
        StructField("tipo_declaracao", StringType(), True),
        StructField("imposto_pago_total", StringType(), True),
        StructField("imposto_restituir", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

pdfs = list_files_by_extension(IRPF_DIR, [".pdf"])
rows = [parse_irpf_pdf(pdf_path) for pdf_path in pdfs]

if rows:
    df_bronze = (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("imposto_pago_total", F.col("imposto_pago_total").cast("double"))
        .withColumn("imposto_restituir", F.col("imposto_restituir").cast("double"))
        .withColumn("ingested_at", F.current_timestamp())
    )
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_IRPF_DECL, catalog_schema=f"{CATALOG}.{BRONZE}")
