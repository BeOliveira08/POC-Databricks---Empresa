# Databricks notebook source
# bronze: raw_rescisoes

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/readers

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

import os
import re
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

BRONZE_RAW_RESCISOES = f"{CATALOG}.{BRONZE}.raw_rescisoes"
CORRECOES_DIR = f"{REFERENCIA_DIR}/rescisoes"

CPF_PATTERN = re.compile(r"\d{3}\.?\d{3}\.?\d{3}-?\d{2}")
CNPJ_PATTERN = re.compile(r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}")
DATE_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4}")


def _ascii(value: str | None) -> str:
    text = value or ""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalized_document_name(arquivo: str) -> str:
    base = re.sub(r"\.[^.]+$", "", arquivo)
    base = _ascii(base).upper()
    base = re.sub(r"^RESCISAO[_\-\s]*", "", base)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def _extract_first(pattern: str, text: str):
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return clean_text(match.group(1)) if match else None


def _find_correcoes_csv(pattern: str) -> str | None:
    matches = list_files_by_extension(CORRECOES_DIR, [".csv"], patterns=[pattern])
    return matches[-1] if matches else None


def _first_existing_col(df, candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def _bool_expr(df, *names):
    col_name = _first_existing_col(df, list(names))
    if not col_name:
        return F.lit(None).cast("boolean")
    value = F.trim(F.lower(F.col(col_name).cast("string")))
    return (
        F.when(value.isin("true", "1", "sim", "yes"), F.lit(True))
        .when(value.isin("false", "0", "nao", "não", "no"), F.lit(False))
        .otherwise(F.col(col_name).cast("boolean"))
    )


def _load_base_rescisoes():
    path = _find_correcoes_csv("BASE_rescisoes")
    if not path:
        return None

    df = read_csv_auto(path)

    def pick(*names):
        col_name = _first_existing_col(df, list(names))
        return F.col(col_name).cast("string") if col_name else F.lit(None).cast("string")

    ingested_at_col = _first_existing_col(df, ["ingested_at"])
    ingested_at_expr = (
        F.to_timestamp(F.col(ingested_at_col).cast("string")) if ingested_at_col else F.current_timestamp()
    )

    return (
        df.select(
            pick("nome_profissional_raw", "nome_profissional").alias("nome_profissional_raw"),
            pick("cpf_raw", "cpf").alias("cpf_raw"),
            pick("empresa_raw", "empresa").alias("empresa_raw"),
            pick("cnpj_raw", "cnpj").alias("cnpj_raw"),
            pick("dt_rescisao_raw", "dt_rescisao", "data_final").alias("dt_rescisao_raw"),
            pick("tipo_rescisao_raw", "tipo_rescisao").alias("tipo_rescisao_raw"),
            pick("tipo_documento_raw", "tipo_documento").alias("tipo_documento_raw"),
            pick("tipo_vinculo_raw", "tipo_vinculo").alias("tipo_vinculo_raw"),
            pick("vl_rescisao_bruta_raw", "vl_rescisao_bruta").alias("vl_rescisao_bruta_raw"),
            pick("vl_rescisao_liquida_raw", "vl_rescisao_liquida").alias("vl_rescisao_liquida_raw"),
            pick("vl_multa_rescisoria_raw", "vl_multa_rescisoria").alias("vl_multa_rescisoria_raw"),
            pick("arquivo_origem_txt", "arquivo_origem", "arquivo").alias("arquivo_origem_txt"),
            pick("arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
            _bool_expr(df, "fl_arquivo_pdf_existe").alias("fl_arquivo_pdf_existe"),
            pick("texto_bruto_documento").alias("texto_bruto_documento"),
            F.coalesce(pick("criterio_origem"), F.lit("REFERENCIA")).alias("criterio_origem"),
            ingested_at_expr.alias("ingested_at"),
        )
        .filter(F.col("arquivo_origem_txt").isNotNull())
        .dropDuplicates(["arquivo_origem_txt"])
    )


def _load_correcoes_rescisoes():
    path = _find_correcoes_csv("depara_rescisoes")
    if not path:
        return None

    df = read_csv_auto(path)
    arquivo_col = _first_existing_col(df, ["arquivo_origem_txt", "arquivo_origem", "arquivo"])
    if not arquivo_col:
        return None

    def pick(*names):
        col_name = _first_existing_col(df, list(names))
        return F.col(col_name).cast("string") if col_name else F.lit(None).cast("string")

    return (
        df.select(
            F.col(arquivo_col).cast("string").alias("arquivo_origem_txt"),
            pick("nome_profissional_corrigido", "nome_profissional").alias("nome_profissional_corrigido"),
            pick("cpf_corrigido", "cpf").alias("cpf_corrigido"),
            pick("empresa_corrigida", "empresa").alias("empresa_corrigida"),
            pick("cnpj_corrigido", "cnpj").alias("cnpj_corrigido"),
            pick("dt_rescisao_corrigida", "dt_rescisao").alias("dt_rescisao_corrigida"),
            pick("tipo_rescisao_corrigida", "tipo_rescisao").alias("tipo_rescisao_corrigida"),
            pick("criterio_origem_corrigido", "criterio_origem").alias("criterio_origem_corrigido"),
        )
        .filter(F.col("arquivo_origem_txt").isNotNull())
        .dropDuplicates(["arquivo_origem_txt"])
    )


def _collect_sources():
    txt_paths = list_files_by_extension_recursive(RESCISOES_DIR, [".txt"])
    pdf_paths = list_files_by_extension_recursive(RESCISOES_DIR, [".pdf"])
    grouped = {}
    for path in sorted(set(txt_paths)):
        key = _normalized_document_name(os.path.basename(path))
        grouped.setdefault(key, {})["txt_path"] = path
    for path in sorted(set(pdf_paths)):
        key = _normalized_document_name(os.path.basename(path))
        grouped.setdefault(key, {})["pdf_path"] = path
    return list(grouped.values())


def _parse_text(texto: str, arquivo: str) -> dict:
    texto_ascii = _ascii(texto or "")
    linhas = [clean_text(line) for line in texto_ascii.splitlines()]
    linhas = [line for line in linhas if line]

    cpf_m = CPF_PATTERN.search(texto_ascii)
    cnpj_m = CNPJ_PATTERN.search(texto_ascii)
    datas = DATE_PATTERN.findall(texto_ascii)

    tipo = "OUTRO"
    low = texto_ascii.lower()
    if "distrato" in low:
        tipo = "DISTRATO"
    elif "aviso previo" in low or "aviso prévio" in low:
        tipo = "AVISO_PREVIO"
    elif "termino de contrato" in low or "término de contrato" in low:
        tipo = "TERMINO_CONTRATO"

    tipo_documento = "OUTRO"
    if "homolog" in low:
        tipo_documento = "HOMOLOGACAO"
    elif "comprovante" in low and "pagamento" in low:
        tipo_documento = "COMPROVANTE_PAGAMENTO"
    elif "multa rescisoria" in low:
        tipo_documento = "MULTA_RESCISORIA"
    elif "aviso" in low:
        tipo_documento = "AVISO"
    elif "termo de rescisao" in low or "trct" in low:
        tipo_documento = "TRCT"

    tipo_vinculo = "CLT"
    if "prestacao de servico" in low or "contratada:" in low:
        tipo_vinculo = "PJ"

    return {
        "nome_profissional_raw": _extract_first(r"(?i)(?:nome do trabalhador|nome)\s*:?\s*([^\n]{8,})", texto_ascii) or (linhas[0] if linhas else None),
        "cpf_raw": clean_text(cpf_m.group(0)) if cpf_m else None,
        "empresa_raw": _extract_first(r"(?i)(?:empresa|empregadora)\s*:?\s*([^\n]{6,})", texto_ascii) or (linhas[1] if len(linhas) > 1 else None),
        "cnpj_raw": clean_text(cnpj_m.group(0)) if cnpj_m else None,
        "dt_rescisao_raw": datas[-1] if datas else None,
        "tipo_rescisao_raw": tipo,
        "tipo_documento_raw": tipo_documento,
        "tipo_vinculo_raw": tipo_vinculo,
        "vl_rescisao_bruta_raw": None,
        "vl_rescisao_liquida_raw": None,
        "vl_multa_rescisoria_raw": None,
        "criterio_origem": "TXT",
    }


schema = StructType(
    [
        StructField("nome_profissional_raw", StringType(), True),
        StructField("cpf_raw", StringType(), True),
        StructField("empresa_raw", StringType(), True),
        StructField("cnpj_raw", StringType(), True),
        StructField("dt_rescisao_raw", StringType(), True),
        StructField("tipo_rescisao_raw", StringType(), True),
        StructField("tipo_documento_raw", StringType(), True),
        StructField("tipo_vinculo_raw", StringType(), True),
        StructField("vl_rescisao_bruta_raw", StringType(), True),
        StructField("vl_rescisao_liquida_raw", StringType(), True),
        StructField("vl_multa_rescisoria_raw", StringType(), True),
        StructField("arquivo_origem_txt", StringType(), True),
        StructField("arquivo_pdf_validacao", StringType(), True),
        StructField("fl_arquivo_pdf_existe", BooleanType(), True),
        StructField("texto_bruto_documento", StringType(), True),
        StructField("criterio_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

rows = []
for item in _collect_sources():
    txt_path = item.get("txt_path")
    if not txt_path:
        continue
    arquivo = os.path.basename(txt_path)
    pdf_path = item.get("pdf_path")
    texto = open(txt_path, "r", encoding="utf-8", errors="ignore").read()
    parsed = _parse_text(texto, arquivo)
    parsed.update(
        {
            "arquivo_origem_txt": arquivo,
            "arquivo_pdf_validacao": os.path.basename(pdf_path) if pdf_path else None,
            "fl_arquivo_pdf_existe": pdf_path is not None,
            "texto_bruto_documento": texto,
        }
    )
    rows.append(parsed)

if rows:
    df_bronze = spark.createDataFrame(rows, schema=schema).withColumn("ingested_at", F.current_timestamp())
else:
    df_bronze = spark.createDataFrame([], schema=schema)

df_base = _load_base_rescisoes()
if df_base is not None:
    df_bronze = df_base.unionByName(
        df_bronze.join(df_base.select("arquivo_origem_txt"), "arquivo_origem_txt", "left_anti")
    )

df_correcoes = _load_correcoes_rescisoes()
if df_correcoes is not None:
    df_bronze = (
        df_bronze.alias("b")
        .join(F.broadcast(df_correcoes).alias("c"), "arquivo_origem_txt", "left")
        .withColumn("nome_profissional_raw", F.coalesce(F.col("c.nome_profissional_corrigido"), F.col("b.nome_profissional_raw")))
        .withColumn("cpf_raw", F.coalesce(F.col("c.cpf_corrigido"), F.col("b.cpf_raw")))
        .withColumn("empresa_raw", F.coalesce(F.col("c.empresa_corrigida"), F.col("b.empresa_raw")))
        .withColumn("cnpj_raw", F.coalesce(F.col("c.cnpj_corrigido"), F.col("b.cnpj_raw")))
        .withColumn("dt_rescisao_raw", F.coalesce(F.col("c.dt_rescisao_corrigida"), F.col("b.dt_rescisao_raw")))
        .withColumn("tipo_rescisao_raw", F.coalesce(F.col("c.tipo_rescisao_corrigida"), F.col("b.tipo_rescisao_raw")))
        .withColumn("criterio_origem", F.coalesce(F.col("c.criterio_origem_corrigido"), F.col("b.criterio_origem")))
        .drop(
            "nome_profissional_corrigido",
            "cpf_corrigido",
            "empresa_corrigida",
            "cnpj_corrigido",
            "dt_rescisao_corrigida",
            "tipo_rescisao_corrigida",
            "criterio_origem_corrigido",
        )
    )

write_overwrite(df_bronze, BRONZE_RAW_RESCISOES, catalog_schema=f"{CATALOG}.{BRONZE}")
