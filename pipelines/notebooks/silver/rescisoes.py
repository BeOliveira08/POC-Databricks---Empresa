# Databricks notebook source
# silver: rescisoes

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_RESCISOES = f"{CATALOG}.{BRONZE}.raw_rescisoes"
SILVER_TB_RESCISOES = f"{CATALOG}.{SILVER}.tb_rescisoes"
SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"

date_udf = F.udf(parse_flexible_date_to_iso, "string")

if not table_exists(BRONZE_RAW_RESCISOES):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_RAW_RESCISOES}")


def parse_date_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")


def extract_cpf_from_text(text: str | None) -> str | None:
    if not text:
        return None
    patterns = [
        r"18\s*CPF\s+(\d{11})",
        r"\bCPF\s+(\d{11})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            digits = re.sub(r"\D", "", match.group(1))
            if len(digits) == 11:
                return digits
    return None


def extract_cnpj_from_text(text: str | None) -> str | None:
    if not text:
        return None
    patterns = [
        r"01\s*CNPJ/CEI\s+(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})",
        r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            digits = re.sub(r"\D", "", match.group(1))
            if len(digits) == 14:
                return digits
    return None


def extract_afastamento_date(text: str | None) -> str | None:
    if not text:
        return None
    patterns = [
        r"26\s*Data de Afastamento\s+(\d{2}/\d{2}/\d{4})",
        r"24\s*Data de Admiss[aã]o\s+(\d{2}/\d{2}/\d{4})\s+26\s*Data de Afastamento\s+(\d{2}/\d{2}/\d{4})",
        r"\bData de Afastamento\b[^\d]*(\d{2}/\d{2}/\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            return parse_flexible_date_to_iso(match.group(match.lastindex))
    return None


def extract_document_type(text: str | None, file_name: str | None) -> str | None:
    low = f"{file_name or ''} {text or ''}".lower()
    if "comprovante" in low and "pagamento" in low:
        return "COMPROVANTE_PAGAMENTO"
    if "multa rescisoria" in low:
        return "MULTA_RESCISORIA"
    if "homolog" in low:
        return "HOMOLOGACAO"
    if "aviso" in low:
        return "AVISO"
    if "trct" in low or "termo de rescisao" in low:
        return "TRCT"
    if "carta rescis" in low or "termino de contrato" in low:
        return "CARTA_RESCISAO"
    if "distrato" in low:
        return "DISTRATO_PJ"
    return "OUTRO"


def extract_tipo_vinculo(text: str | None, raw_value: str | None) -> str | None:
    raw = (raw_value or "").strip().upper()
    if raw in {"CLT", "PJ"}:
        return raw
    low = (text or "").lower()
    if "prestacao de servico" in low or "contratada:" in low:
        return "PJ"
    if (
        "termo de rescisao do contrato de trabalho" in low
        or "ctps" in low
        or "empregador" in low
        or "empregado" in low
        or "trct" in low
    ):
        return "CLT"
    return None


def _extract_money(patterns: list[str], text: str | None) -> float | None:
    if not text:
        return None
    upper = text.upper()
    for pattern in patterns:
        match = re.search(pattern, upper, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            value = re.sub(r"[^\d,.-]", "", match.group(1))
            try:
                return float(value.replace(".", "").replace(",", "."))
            except Exception:
                continue
    return None


def extract_total_bruto(text: str | None) -> float | None:
    return _extract_money([r"TOTAL BRUTO\s*([\d\.,]+)", r"TOTALBRUTO\s*([\d\.,]+)"], text)


def extract_total_liquido(text: str | None) -> float | None:
    return _extract_money(
        [
            r"VALOR LIQUIDO DE R\$\s*([\d\.,]+)",
            r"VALOR LIQUIDO\s*([\d\.,]+)",
            r"TOTAL LIQUIDO\s*([\d\.,]+)",
            r"LIQUIDO A RECEBER\s*([\d\.,]+)",
            r"LIQUIDO\s*([\d\.,]+)",
        ],
        text,
    )


def extract_multa_rescisoria(text: str | None) -> float | None:
    return _extract_money(
        [
            r"MULTA RESCISORIA\s*([\d\.,]+)",
            r"MULTA DE 40%\s*([\d\.,]+)",
            r"MULTA FGTS\s*([\d\.,]+)",
        ],
        text,
    )


def parse_money_text(text: str | None) -> float | None:
    if not text:
        return None
    value = re.sub(r"[^\d,.-]", "", str(text))
    if not value:
        return None
    try:
        return float(value.replace(".", "").replace(",", "."))
    except Exception:
        return None


cpf_text_udf = F.udf(extract_cpf_from_text, "string")
cnpj_text_udf = F.udf(extract_cnpj_from_text, "string")
afastamento_text_udf = F.udf(extract_afastamento_date, "string")
tipo_documento_udf = F.udf(extract_document_type, "string")
tipo_vinculo_udf = F.udf(extract_tipo_vinculo, "string")
money_text_udf = F.udf(parse_money_text, "double")
vl_bruto_udf = F.udf(extract_total_bruto, "double")
vl_liquido_udf = F.udf(extract_total_liquido, "double")
vl_multa_udf = F.udf(extract_multa_rescisoria, "double")


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


def cleaned_name_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt == "") |
        txt.rlike(
            r"ENTIDADE SINDICAL|CODIGO SINDICAL|RAZAO SOCIAL|NOME DA ENTIDADE|CNPJ|CPF|DATA DE NASCIMENTO|TERMO DE RESCISAO|TRCT"
        ) |
        txt.rlike(r".*\d{2,}.*"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


df_empresa_ref = empty_df_for_schema("nu_cnpj string, nome_empresa_ref string")
if table_exists(SILVER_TB_EMPRESA):
    df_empresa_ref = (
        spark.table(SILVER_TB_EMPRESA)
        .select(
            F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
            F.col("nm_empresa").cast("string").alias("nome_empresa_ref"),
        )
        .filter(F.col("nu_cnpj").isNotNull())
        .dropDuplicates(["nu_cnpj"])
    )

df_base = (
    spark.table(BRONZE_RAW_RESCISOES)
    .select(
        F.col("cpf_raw").cast("string").alias("cpf_raw"),
        F.col("cnpj_raw").cast("string").alias("cnpj_raw"),
        F.col("nome_profissional_raw").cast("string").alias("nome_profissional_raw"),
        F.col("empresa_raw").cast("string").alias("empresa_raw"),
        F.col("dt_rescisao_raw").cast("string").alias("dt_rescisao_raw"),
        F.col("tipo_rescisao_raw").cast("string").alias("tipo_rescisao_raw"),
        F.col("tipo_documento_raw").cast("string").alias("tipo_documento_raw"),
        F.col("tipo_vinculo_raw").cast("string").alias("tipo_vinculo_raw"),
        F.col("vl_rescisao_bruta_raw").cast("string").alias("vl_rescisao_bruta_raw"),
        F.col("vl_rescisao_liquida_raw").cast("string").alias("vl_rescisao_liquida_raw"),
        F.col("vl_multa_rescisoria_raw").cast("string").alias("vl_multa_rescisoria_raw"),
        F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_pdf_validacao")).cast("string").alias("file_name"),
        F.col("texto_bruto_documento").cast("string").alias("texto_bruto_documento"),
        F.col("criterio_origem").cast("string").alias("criterio_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .withColumn("cpf_raw_digits", only_digits_expr("cpf_raw"))
    .withColumn("cnpj_raw_digits", only_digits_expr("cnpj_raw"))
    .withColumn("cpf_texto", cpf_text_udf("texto_bruto_documento"))
    .withColumn("cnpj_texto", cnpj_text_udf("texto_bruto_documento"))
    .withColumn("dt_rescisao_texto", afastamento_text_udf("texto_bruto_documento"))
    .withColumn(
        "cpf",
        F.coalesce(
            F.when(F.length(F.col("cpf_raw_digits")) == 11, F.col("cpf_raw_digits")),
            F.when(F.length(F.col("cpf_texto")) == 11, F.col("cpf_texto")),
        ),
    )
    .withColumn(
        "cnpj",
        F.coalesce(
            F.when(F.length(F.col("cnpj_raw_digits")) == 14, F.col("cnpj_raw_digits")),
            F.when(F.length(F.col("cnpj_texto")) == 14, F.col("cnpj_texto")),
        ),
    )
    .withColumn("nome_profissional_base", cleaned_name_expr("nome_profissional_raw"))
    .withColumn("nome_empresa_base", cleaned_name_expr("empresa_raw"))
    .withColumn(
        "dt_rescisao",
        F.coalesce(
            parse_date_expr("dt_rescisao_raw"),
            F.to_date(F.col("dt_rescisao_texto"), "yyyy-MM-dd"),
        ),
    )
    .withColumn("tipo_documento", F.coalesce(nullable_text_expr("tipo_documento_raw"), tipo_documento_udf("texto_bruto_documento", "file_name")))
    .withColumn("tipo_vinculo", F.coalesce(nullable_text_expr("tipo_vinculo_raw"), tipo_vinculo_udf("texto_bruto_documento", "tipo_vinculo_raw")))
    .withColumn(
        "tipo_rescisao",
        F.coalesce(
            F.when(cleaned_name_expr("tipo_rescisao_raw").isNotNull(), normalized_text_expr("tipo_rescisao_raw")),
            F.lit("OUTRO"),
        ),
    )
    .withColumn(
        "vl_rescisao_bruta",
        F.coalesce(
            money_text_udf("vl_rescisao_bruta_raw"),
            vl_bruto_udf("texto_bruto_documento"),
        ),
    )
    .withColumn(
        "vl_rescisao_liquida",
        F.coalesce(
            money_text_udf("vl_rescisao_liquida_raw"),
            vl_liquido_udf("texto_bruto_documento"),
        ),
    )
    .withColumn(
        "vl_multa_rescisoria",
        F.coalesce(
            money_text_udf("vl_multa_rescisoria_raw"),
            vl_multa_udf("texto_bruto_documento"),
        ),
    )
)

df_enriched = (
    df_base.alias("b")
    .join(
        F.broadcast(df_empresa_ref).alias("e"),
        F.col("b.cnpj") == F.col("e.nu_cnpj"),
        "left",
    )
    .withColumn("nome_profissional", F.col("b.nome_profissional_base"))
    .withColumn("nome_empresa", F.coalesce(F.col("b.nome_empresa_base"), F.col("e.nome_empresa_ref")))
    .filter(F.col("file_name").isNotNull())
    .filter(F.col("dt_rescisao").isNotNull())
    .filter(F.upper(F.coalesce(F.col("tipo_vinculo"), F.lit("CLT"))).isin("CLT", "PJ"))
    .filter(
        F.col("cpf").isNotNull() |
        F.col("cnpj").isNotNull() |
        F.col("nome_profissional").isNotNull() |
        F.col("nome_empresa").isNotNull()
    )
)

w = Window.partitionBy(
    F.coalesce(F.col("cpf"), F.lit("")),
    F.coalesce(F.col("cnpj"), F.lit("")),
    F.coalesce(F.col("dt_rescisao").cast("string"), F.lit("")),
    F.coalesce(F.col("file_name"), F.lit("")),
).orderBy(
    F.when(F.col("nome_profissional").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.when(F.col("nome_empresa").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("ingested_at_origem").desc_nulls_last(),
)

w_id_rescisao = Window.orderBy(
    F.coalesce(F.col("cpf"), F.lit("")),
    F.coalesce(F.col("cnpj"), F.lit("")),
    F.coalesce(F.col("dt_rescisao").cast("string"), F.lit("")),
    F.coalesce(F.col("file_name"), F.lit("")),
)

df_final = (
    df_enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("id_rescisao", F.row_number().over(w_id_rescisao))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_rescisao").cast("int").alias("id_rescisao"),
        F.col("file_name").cast("string").alias("file_name"),
        F.col("cpf").cast("string").alias("cpf"),
        F.col("cnpj").cast("string").alias("cnpj"),
        F.col("nome_profissional").cast("string").alias("nome_profissional"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("tipo_vinculo").cast("string").alias("tipo_vinculo"),
        F.col("tipo_documento").cast("string").alias("tipo_documento"),
        F.col("tipo_rescisao").cast("string").alias("tipo_rescisao"),
        F.col("dt_rescisao").cast("date").alias("dt_rescisao"),
        F.col("vl_rescisao_bruta").cast("double").alias("vl_rescisao_bruta"),
        F.col("vl_rescisao_liquida").cast("double").alias("vl_rescisao_liquida"),
        F.col("vl_multa_rescisoria").cast("double").alias("vl_multa_rescisoria"),
        F.col("criterio_origem").cast("string").alias("criterio_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_RESCISOES, catalog_schema=f"{CATALOG}.{SILVER}")
