# Databricks notebook source
# bronze: raw_distratos

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

BRONZE_RAW_DISTRATOS = f"{CATALOG}.{BRONZE}.raw_distratos"
CORRECOES_DIR = f"{REFERENCIA_DIR}/distratos"

CNPJ_PATTERN = re.compile(r"\d{2}[\.,]?\s*\d{3}[\.,]?\s*\d{3}\s*/?\s*\d{4}\s*-?\s*\d{2}")
DATE_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4}")
INTERNAL_CNPJS = {"11111111111111", "22222222222222"}
MONTH_NAME_MAP = {
    "JAN": "01",
    "JANEIRO": "01",
    "FEV": "02",
    "FEVEREIRO": "02",
    "MAR": "03",
    "MARCO": "03",
    "MARÇO": "03",
    "ABR": "04",
    "ABRIL": "04",
    "MAI": "05",
    "MAIO": "05",
    "JUN": "06",
    "JUNHO": "06",
    "JUL": "07",
    "JULHO": "07",
    "AGO": "08",
    "AGOSTO": "08",
    "SET": "09",
    "SETEMBRO": "09",
    "OUT": "10",
    "OUTUBRO": "10",
    "NOV": "11",
    "NOVEMBRO": "11",
    "DEZ": "12",
    "DEZEMBRO": "12",
}
FLEX_DATE_PATTERN = re.compile(
    r"\b(\d{1,2}(?:/\d{1,2}/\d{4}|/[A-ZÇ]+/\d{4}|(?:\s+DE\s+[A-ZÇ]+\s+DE\s+\d{4})))\b",
    re.IGNORECASE,
)


def _ascii(value: str | None) -> str:
    text = value or ""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalized_document_name(arquivo: str) -> str:
    base = re.sub(r"\.[^.]+$", "", arquivo)
    base = _ascii(base).upper()
    base = re.sub(r"^DISTRAT[OA]?[_\-\s]*", "", base)
    base = re.sub(r"^DISTRITO[_\-\s]*", "", base)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def _strip_signature_tail(texto: str) -> str:
    if not texto:
        return texto
    upper = _ascii(texto).upper()
    cut_markers = [
        "CLICKSIGN",
        "DOCUSIGN ENVELOPE ID",
        "D4SIGN ",
        "ASSINATURAS",
        "EVENTOS DO DOCUMENTO",
        "LOG",
        "HASH DO DOCUMENTO ORIGINAL",
        "DOCUMENTO NUMERO #",
        "DATAS E HORARIOS EM GMT",
        "PROCESSO DE ASSINATURA",
    ]
    cut_pos = len(texto)
    for marker in cut_markers:
        idx = upper.find(marker)
        if idx >= 0:
            cut_pos = min(cut_pos, idx)
    return texto[:cut_pos].strip()


def _extract_company_from_file_name(arquivo: str) -> str | None:
    value = clean_text(_normalized_document_name(arquivo))
    if not value:
        return None
    value = re.sub(r"\b(PDF|TXT)\b", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip(" -_.")
    value = clean_text(value)
    if not value or value.upper() in {"PDF", "TXT"}:
        return None
    return value


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


def _load_base_distratos():
    path = _find_correcoes_csv("BASE_distratos")
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
            pick("empresa").alias("empresa"),
            pick("cnpj").alias("cnpj"),
            pick("dt_inicio").alias("dt_inicio"),
            pick("dt_fim").alias("dt_fim"),
            pick("motivo").alias("motivo"),
            pick("arquivo_origem_txt", "arquivo_origem", "arquivo").alias("arquivo_origem_txt"),
            pick("arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
            _bool_expr(df, "fl_arquivo_pdf_existe").alias("fl_arquivo_pdf_existe"),
            pick("texto_bruto_documento").alias("texto_bruto_documento"),
            pick("criterio_origem").alias("criterio_origem"),
            ingested_at_expr.alias("ingested_at"),
        )
        .filter(F.col("arquivo_origem_txt").isNotNull())
        .dropDuplicates(["arquivo_origem_txt"])
    )


def _load_correcoes_distratos():
    path = _find_correcoes_csv("depara_distratos")
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
            pick("empresa_corrigida", "empresa").alias("empresa_corrigida"),
            pick("cnpj_corrigido", "cnpj").alias("cnpj_corrigido"),
            pick("dt_inicio_corrigida", "dt_inicio").alias("dt_inicio_corrigida"),
            pick("dt_fim_corrigida", "dt_fim").alias("dt_fim_corrigida"),
            pick("motivo_corrigido", "motivo").alias("motivo_corrigido"),
            pick("criterio_origem_corrigido", "criterio_origem").alias("criterio_origem_corrigido"),
        )
        .filter(F.col("arquivo_origem_txt").isNotNull())
        .dropDuplicates(["arquivo_origem_txt"])
    )


def _normalize_date(value: str | None) -> str | None:
    value = clean_text(value)
    if not value:
        return None
    value_ascii = _ascii(value).upper().replace(".", "").strip()
    month_match = re.fullmatch(r"(\d{1,2})/([A-ZÇ]+)/(\d{4})", value_ascii)
    if month_match:
        dd, month_name, yyyy = month_match.groups()
        mm = MONTH_NAME_MAP.get(month_name)
        if not mm:
            return None
        value = f"{int(dd):02d}/{mm}/{yyyy}"
    else:
        month_long_match = re.fullmatch(r"(\d{1,2})\s+DE\s+([A-ZÇ]+)\s+DE\s+(\d{4})", value_ascii)
        if month_long_match:
            dd, month_name, yyyy = month_long_match.groups()
            mm = MONTH_NAME_MAP.get(month_name)
            if not mm:
                return None
            value = f"{int(dd):02d}/{mm}/{yyyy}"
        elif not re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
            return None
    dd, mm, yyyy = value.split("/")
    d = int(dd)
    m = int(mm)
    if d <= 12 and m > 12:
        dd, mm = mm, dd
    d = int(dd)
    m = int(mm)
    if not (1 <= d <= 31 and 1 <= m <= 12):
        return None
    return f"{dd}/{mm}/{yyyy}"


def _date_key(value: str) -> tuple[int, int, int]:
    dd, mm, yyyy = value.split("/")
    return int(yyyy), int(mm), int(dd)


def _pick_min_date(values: list[str]) -> str | None:
    return min(values, key=_date_key) if values else None


def _pick_max_date(values: list[str]) -> str | None:
    return max(values, key=_date_key) if values else None


def _extract_dates_in_text(texto_norm: str) -> list[str]:
    values = []
    for raw_value in FLEX_DATE_PATTERN.findall(texto_norm or ""):
        normalized = _normalize_date(raw_value)
        if normalized:
            values.append(normalized)
    return values


def _sanitize_reason(value: str | None) -> str | None:
    value = clean_text(_ascii(value))
    if not value:
        return None
    upper = value.upper()
    if upper in {"IDENTIFICACAO DO EMPREGADOR", "IDENTIFICACAO DO TRABALHADOR", "PREZADO,"}:
        return None
    if re.fullmatch(r"\d[\d\./,-]*", value):
        return None
    if "CNPJ/CEI" in upper or "IDENTIFICACAO" in upper or "TIPO DE CONTRATO" in upper or len(value) > 160:
        return None
    if upper.startswith("1 - CONTRATO DE TRABALHO"):
        return None
    return value


def _clean_cnpj_candidates(matches: list[str]) -> list[str]:
    preferred = [clean_cnpj(value) for value in matches if "/" in value]
    preferred = [value for value in preferred if value and value not in INTERNAL_CNPJS]
    if preferred:
        return preferred
    fallback = [clean_cnpj(value) for value in matches]
    return [value for value in fallback if value and value not in INTERNAL_CNPJS]


def _is_trct(texto_norm: str) -> bool:
    return "TERMO DE RESCISAO DO CONTRATO DE TRABALHO" in texto_norm or "IDENTIFICACAO DO EMPREGADOR" in texto_norm


def _is_proposal_like_document(texto_norm: str, arquivo: str) -> bool:
    arquivo_norm = _ascii(arquivo or "").upper()
    proposal_terms = [
        "CARTA PROPOSTA",
        "CARTA OFERTA",
        "PROPOSTA DE TRABALHO",
        "PROPOSTA COMERCIAL",
        "SEJA BEM VINDO",
        "CONTRATACAO",
    ]
    if any(term in arquivo_norm for term in proposal_terms):
        return True
    if any(term in texto_norm for term in proposal_terms):
        has_distrato_signal = any(
            signal in texto_norm
            for signal in [
                "DISTRATO",
                "TERMO DE RESCISAO",
                "RESCINDIDO",
                "RESCISAO DO CONTRATO",
                "EXTINCAO DO CONTRATO DE EXPERIENCIA",
            ]
        )
        return not has_distrato_signal
    return False


def _is_distrato_document(texto_norm: str, arquivo: str) -> bool:
    arquivo_norm = _ascii(arquivo or "").upper()
    if _is_proposal_like_document(texto_norm, arquivo_norm):
        return False
    required_signals = [
        "DISTRATO",
        "TERMO DE RESCISAO",
        "RESCISAO DO CONTRATO",
        "RESCINDIDO REFERIDO CONTRATO",
        "EXTINCAO DO CONTRATO DE EXPERIENCIA",
        "CAUSA DO AFASTAMENTO",
    ]
    return any(signal in texto_norm for signal in required_signals) or "DISTRATO" in arquivo_norm


def _extract_employer_block(texto_norm: str) -> str:
    match = re.search(
        r"IDENTIFICACAO DO EMPREGADOR(.*?)(?:VERBAS RESCISORIAS|DISCRIMINACAO DAS VERBAS|DADOS DO CONTRATO)",
        texto_norm,
        re.IGNORECASE | re.DOTALL,
    )
    return match.group(1) if match else texto_norm


def _extract_trct_reason(texto_norm: str) -> str | None:
    patterns = [
        r"22\s+CAUSA DO AFASTAMENTO\s+(.+?)\s+(?:23\s+REMUNERACAO|27\s+COD\.?\s+AFASTAMENTO|29\s+PENSAO|DADOS DO CONTRATO)",
        r"(RESCISAO ANTECIPADA,\s*PELO EMPREGADOR,[^\n]+)",
        r"(DESPEDIDA SEM JUSTA CAUSA,\s*PELO EMPREGADOR)",
        r"(I1\s*-\s*FIM ANTECIPADO[^\n]+)",
        r"(FINALIZACAO AUTOMATICA[^\n]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE | re.DOTALL)
        if match:
            reason = re.sub(r"\s+", " ", match.group(1)).strip()
            cleaned = _sanitize_reason(reason)
            if cleaned:
                return cleaned
    return None


def _extract_dates_near_label(texto_norm: str, label_pattern: str, window: int = 120) -> list[str]:
    match = re.search(label_pattern, texto_norm, re.IGNORECASE)
    if not match:
        return []
    tail = texto_norm[match.end():match.end() + window]
    values = [_normalize_date(value) for value in FLEX_DATE_PATTERN.findall(tail)]
    return [value for value in values if value]


def _first_distinct_date(values: list[str], excluded: list[str] | None = None) -> str | None:
    excluded = set(excluded or [])
    for value in values:
        if value and value not in excluded:
            return value
    return None


def _extract_semantic_start_end_dates(texto_norm: str) -> tuple[str | None, str | None]:
    semantic_start_patterns = [
        r"FIRMAD[OA]\s+EM\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"PACTUADO\s+NA\s+DATA\s+DE\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"CELEBRARAM.*?FIRMADO\s+EM\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"CONTRATO.*?EM\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"DATA DE INICIO DO REFERIDO CONTRATO E\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
    ]
    semantic_end_patterns = [
        r"RESCINDIDO\s+REFERIDO\s+CONTRATO\s+EM\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"CESSOU-SE\s+A\s+PRESTACAO\s+DE\s+SERVICOS.*?EM\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"NA\s+PRESENTE\s+DATA\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"TERMINA\s+EM\s+(" + FLEX_DATE_PATTERN.pattern[2:-2] + r")",
        r"CESSAR\s+SUA\s+ATIVIDADE.*?NA\s+REFERIDA\s+DATA",
    ]

    dt_inicio = None
    for pattern in semantic_start_patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE | re.DOTALL)
        if match:
            dt_inicio = _normalize_date(match.group(1))
            if dt_inicio:
                break

    dt_fim = None
    for pattern in semantic_end_patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE | re.DOTALL)
        if match and match.lastindex:
            dt_fim = _normalize_date(match.group(1))
            if dt_fim:
                break

    if not dt_fim:
        line_match = re.search(r"CESSOU-SE\s+A\s+PRESTACAO\s+DE\s+SERVICOS\s+POR\s+PARTE\s+DA\s+CONTRATADA\s+EM\s+(\d{2}/\d{2}/\d{4})", texto_norm, re.IGNORECASE)
        if line_match:
            dt_fim = _normalize_date(line_match.group(1))

    return dt_inicio, dt_fim


def _parse_text(texto: str, arquivo: str) -> dict:
    texto_sem_log = _strip_signature_tail(texto or "")
    texto_norm = _ascii(texto_sem_log).upper()
    if not _is_distrato_document(texto_norm, arquivo):
        return None

    if _is_trct(texto_norm):
        employer_block = _extract_employer_block(texto_norm)
        cnpjs = _clean_cnpj_candidates(CNPJ_PATTERN.findall(employer_block)) or _clean_cnpj_candidates(CNPJ_PATTERN.findall(texto))
        birth_dates = _extract_dates_near_label(texto_norm, r"19\s+DATA\s+DE\s+NASCIMENTO")
        admission_dates = _extract_dates_near_label(texto_norm, r"24\s+DATA\s+DE\s+ADMISSAO", 150)
        afastamento_dates = _extract_dates_near_label(texto_norm, r"26\s+DATA\s+DE\s+AFASTAMENTO", 150)
        birth_date = _first_distinct_date(birth_dates)
        dt_inicio = _first_distinct_date(admission_dates, [birth_date] if birth_date else [])
        dt_fim = _first_distinct_date(afastamento_dates, [birth_date, dt_inicio] if birth_date or dt_inicio else [])
        all_dates = _extract_dates_in_text(texto_norm)
        all_dates = [d for d in all_dates if d and d != "00/00/0000" and int(d[-4:]) >= 2000]
        if not dt_inicio:
            dt_inicio = _pick_min_date(all_dates)
        if not dt_fim:
            dt_fim = _pick_max_date(all_dates)
        dt_inicio, dt_fim = fix_dates(dt_inicio, dt_fim)
        return {
            "empresa": _extract_company_from_file_name(arquivo),
            "cnpj": cnpjs[0] if cnpjs else None,
            "dt_inicio": dt_inicio,
            "dt_fim": dt_fim,
            "motivo": _extract_trct_reason(texto_norm),
            "criterio_origem": "TRCT_TXT",
        }

    cnpjs = [clean_cnpj(cnpj) for cnpj in CNPJ_PATTERN.findall(texto_sem_log)]
    cnpjs = [cnpj for cnpj in cnpjs if cnpj and cnpj not in INTERNAL_CNPJS]
    dt_inicio, dt_fim = _extract_semantic_start_end_dates(texto_norm)
    dates = _extract_dates_in_text(texto_norm)
    dates = [d for d in dates if d and d != "00/00/0000" and int(d[-4:]) >= 2000]
    dates = sorted(set(dates), key=_date_key)
    if not dt_inicio:
        dt_inicio = _pick_min_date(dates)
    if not dt_fim:
        dt_fim = _pick_max_date(dates)
    dt_inicio, dt_fim = fix_dates(dt_inicio, dt_fim)
    reason = None
    for pattern in [
        r"(RESCISAO ANTECIPADA PELO EMPREGADOR)",
        r"(ANTECIPADA POR PARTE DO EMPREGADOR)",
        r"(FINALIZACAO AUTOMATICA[^\.]*)",
        r"(DE COMUM ACORDO[^\.]*)",
        r"(MUTUO CONSENTIMENTO[^\.]*)",
    ]:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            reason = _sanitize_reason(match.group(1))
            if reason:
                break
    return {
        "empresa": _extract_company_from_file_name(arquivo),
        "cnpj": cnpjs[0] if cnpjs else None,
        "dt_inicio": dt_inicio,
        "dt_fim": dt_fim,
        "motivo": reason,
        "criterio_origem": "DISTRATO_TXT",
    }


def _collect_sources():
    txt_paths = list_files_by_extension_recursive(DISTRATOS_DIR, [".txt"])
    pdf_paths = list_files_by_extension_recursive(DISTRATOS_DIR, [".pdf"])
    grouped = {}
    for path in sorted(set(txt_paths)):
        key = _normalized_document_name(os.path.basename(path))
        grouped.setdefault(key, {})["txt_path"] = path
    for path in sorted(set(pdf_paths)):
        key = _normalized_document_name(os.path.basename(path))
        grouped.setdefault(key, {})["pdf_path"] = path
    return list(grouped.values())


schema = StructType(
    [
        StructField("empresa", StringType(), True),
        StructField("cnpj", StringType(), True),
        StructField("dt_inicio", StringType(), True),
        StructField("dt_fim", StringType(), True),
        StructField("motivo", StringType(), True),
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
    if not parsed:
        continue
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

df_base = _load_base_distratos()
if df_base is not None:
    df_bronze = df_base.unionByName(
        df_bronze.join(df_base.select("arquivo_origem_txt"), "arquivo_origem_txt", "left_anti")
    )

df_correcoes = _load_correcoes_distratos()
if df_correcoes is not None:
    df_bronze = (
        df_bronze.alias("b")
        .join(F.broadcast(df_correcoes).alias("c"), "arquivo_origem_txt", "left")
        .withColumn("empresa", F.coalesce(F.col("c.empresa_corrigida"), F.col("b.empresa")))
        .withColumn("cnpj", F.coalesce(F.col("c.cnpj_corrigido"), F.col("b.cnpj")))
        .withColumn("dt_inicio", F.coalesce(F.col("c.dt_inicio_corrigida"), F.col("b.dt_inicio")))
        .withColumn("dt_fim", F.coalesce(F.col("c.dt_fim_corrigida"), F.col("b.dt_fim")))
        .withColumn("motivo", F.coalesce(F.col("c.motivo_corrigido"), F.col("b.motivo")))
        .withColumn("criterio_origem", F.coalesce(F.col("c.criterio_origem_corrigido"), F.col("b.criterio_origem")))
        .drop(
            "empresa_corrigida",
            "cnpj_corrigido",
            "dt_inicio_corrigida",
            "dt_fim_corrigida",
            "motivo_corrigido",
            "criterio_origem_corrigido",
        )
    )

write_overwrite(df_bronze, BRONZE_RAW_DISTRATOS, catalog_schema=f"{CATALOG}.{BRONZE}")
