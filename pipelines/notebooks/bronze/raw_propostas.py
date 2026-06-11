# Databricks notebook source
# bronze: raw_propostas

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
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, BooleanType

BRONZE_RAW_PROPOSTAS = f"{CATALOG}.{BRONZE}.raw_propostas"
CORRECOES_DIR = f"{REFERENCIA_DIR}/propostas"

CNPJ_PATTERN = re.compile(r"\d{2}\.?\s*\d{3}\.?\s*\d{3}\s*/?\s*\d{4}\s*-?\s*\d{2}")
DATE_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4}")
SENIORIDADE_PATTERN = re.compile(r"\b(JR|JUNIOR|PLENO|SENIOR|SR|ESPECIALISTA)\b", re.IGNORECASE)
CARGO_LABEL_PATTERNS = [
    re.compile(r"Cargo\s*:\s*([^\n]+)", re.IGNORECASE),
    re.compile(r"Posi[cÇ][aÃ]o\s*:\s*([^\n]+)", re.IGNORECASE),
]
VALOR_LABEL_PATTERNS = [
    re.compile(r"Sal[aá]rio(?:\s+mensal)?\s*[:\-]?\s*R\$\s*([\d]{1,3}(?:\.[\d]{3})*(?:,\d{2})?)", re.IGNORECASE),
    re.compile(r"Remunera[cÇ][aÃ]o\s+Mensal\s*[:\-]?\s*R\$\s*([\d]{1,3}(?:\.[\d]{3})*(?:,\d{2})?)", re.IGNORECASE),
    re.compile(r"Pacote de remunera[cÇ][aÃ]o[^\dR$]{0,40}Sal[aá]rio mensal:\s*R\$\s*([\d]{1,3}(?:\.[\d]{3})*(?:,\d{2})?)", re.IGNORECASE),
    re.compile(r"PACOTE DE REMUNERA[ÇC][ÃA]O\s*R\$\s*([\d]{1,3}(?:\.[\d]{3})*(?:,\d{2})?)", re.IGNORECASE),
    re.compile(r"Sal[aá]rio\s*R\$\s*([\d]{1,3}(?:\.[\d]{3})*(?:,\d{2})?)\s*/m[eê]s", re.IGNORECASE),
]
EMPRESA_LABEL_PATTERNS = [
    re.compile(r"Empresa\s*:\s*([^\n]+)", re.IGNORECASE),
    re.compile(r"time da\s+([^\n\.]+)", re.IGNORECASE),
    re.compile(r"time\s+([A-Z][A-Za-z&\.\- ]+),\s+uma consultoria", re.IGNORECASE),
]
INICIO_LABEL_PATTERNS = [
    re.compile(r"(?:Data de in[ií]cio|Previs[aã]o de in[ií]cio|In[ií]cio|Admiss[aã]o|Data de Admiss[aã]o)\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})", re.IGNORECASE),
    re.compile(r"Data de in[ií]cio:\s*([0-9]{1,2}\s+de\s+[A-Za-zçãéôóí]+\s+de\s+\d{4})", re.IGNORECASE),
]


def _list_txt_files(base_dir: str):
    paths = []
    for ext in [".txt"]:
        paths.extend(list_files_by_extension_recursive(base_dir, [ext]))
    return sorted(set(paths))


def read_text_file(path: str) -> str:
    try:
        return dbutils.fs.head(path, 10_000_000)
    except Exception:
        pass

    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        pass

    try:
        with open(path, "r", encoding="latin-1") as f:
            return f.read()
    except Exception:
        return ""


def _matching_pdf_name(txt_path: str) -> str | None:
    txt_name = os.path.basename(txt_path)
    txt_base = os.path.splitext(txt_name)[0].lower()
    pdfs = list_files_by_extension_recursive(PROPOSTAS_DIR, [".pdf"])
    for pdf_path in pdfs:
        if os.path.splitext(os.path.basename(pdf_path))[0].lower() == txt_base:
            return os.path.basename(pdf_path)
    return None


def _extract_company_from_file_name(arquivo: str) -> str | None:
    base = os.path.splitext(arquivo)[0].upper()
    base = re.sub(r"^(CARTA[_\-\s]*PROPOSTA|CARTA|PROPOSTA|CONTRATA[ÇC][ÃA]O|CATA)[_\-\s]*", "", base)
    base = re.sub(r"[_\-]+", " ", base)
    return clean_text(base)


def _extract_first(pattern: re.Pattern, texto: str) -> str | None:
    match = pattern.search(texto or "")
    if not match:
        return None
    if match.lastindex:
        return clean_text(match.group(1))
    return clean_text(match.group(0))


def _extract_by_patterns(texto: str, patterns: list[re.Pattern]) -> str | None:
    for pattern in patterns:
        value = _extract_first(pattern, texto)
        if value:
            return value
    return None


def _sanitize_cargo(value: str | None) -> str | None:
    value = clean_text(value)
    if not value:
        return None
    value = re.split(r"(?:Papel:|Regime de contrata[cç][aã]o|Jornada de trabalho|N[°º] Vaga|Local de trabalho)", value, maxsplit=1)[0]
    value = clean_text(value)
    value = re.sub(r"\s+\d+$", "", value or "")
    return value[:120] if value else None


def _sanitize_empresa(value: str | None, arquivo: str) -> str | None:
    value = clean_text(value)
    if not value:
        return _extract_company_from_file_name(arquivo)
    lower = value.lower()
    if "plano de sa" in lower or "coparticipa" in lower or len(value) > 80:
        return _extract_company_from_file_name(arquivo)
    return value


def _extract_company_from_file_name(arquivo: str) -> str | None:
    base = os.path.splitext(arquivo)[0].upper()
    base = re.sub(r"^(CARTA[_\-\s]*PROPOSTA|CARTA[_\-\s]*OFERTA|CARTA|PROPOSTA|CONTRATA[ÇC][ĂA]O|CATA)[_\-\s]*", "", base)
    base = re.sub(r"\b[0-9A-F]{8,}\b", " ", base)
    base = re.sub(r"\b(TITULAR|JUNIOR|COSTA|GENERICO|ALT|PDF|TXT)\b", " ", base)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    value = clean_text(base)
    if not value:
        return None
    if re.fullmatch(r"[0-9A-F _-]{8,}", value.upper()):
        return None
    return value


def _sanitize_empresa(value: str | None, arquivo: str) -> str | None:
    def _clean_candidate(raw: str | None) -> str | None:
        candidate = clean_text(raw)
        if not candidate:
            return None
        candidate = re.sub(r"\{[^}]*\}", " ", candidate)
        candidate = re.sub(r"\b[0-9A-F]{8,}\b", " ", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\b(CARTA|PROPOSTA|OFERTA|TRABALHO|TITULAR|JUNIOR|COSTA|GENERICO|ALT|PDF|TXT)\b", " ", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s+", " ", candidate).strip(" -_.")
        candidate = clean_text(candidate)
        if not candidate:
            return None
        upper = candidate.upper()
        if upper in {"CARTA", "PROPOSTA", "OFERTA", "TRABALHO", "DE TRABALHO", "NAO INFORMADO"}:
            return None
        if re.fullmatch(r"[0-9A-F _-]{8,}", upper):
            return None
        if len(candidate) > 80:
            return None
        if "plano de sa" in candidate.lower() or "coparticipa" in candidate.lower():
            return None
        return candidate

    cleaned = _clean_candidate(value)
    if cleaned:
        return cleaned
    return _clean_candidate(_extract_company_from_file_name(arquivo))


def _ascii_fold(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\ufeff", " ").strip()
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text.upper().strip()


def _canonicalize_empresa(value: str | None) -> str | None:
    cleaned = clean_text(value)
    if not cleaned:
        return None

    normalized = _ascii_fold(cleaned)
    aliases = [
        (r"\bCONSULTORIA\s*ALFA\b", "CONSULTORIA ALFA LTDA"),
        (r"\bCONSULTORIA\s*BETA\b", "CONSULTORIA BETA LTDA"),
        (r"\bEMPRESA\s*ALFA\b", "EMPRESA ALFA S.A."),
        (r"\bEMPRESA\s*BETA\b", "EMPRESA BETA LTDA"),
        (r"\bCLIENTE\s*ALFA\b", "CLIENTE ALFA S.A."),
        (r"\bCLIENTE\s*BETA\b", "CLIENTE BETA LTDA"),
    ]
    for pattern, canonical in aliases:
        if re.search(pattern, normalized, re.IGNORECASE):
            return canonical
    return cleaned


def _extract_company_from_file_name(arquivo: str) -> str | None:
    base = _ascii_fold(os.path.splitext(arquivo)[0])
    if not base:
        return None

    canonical = _canonicalize_empresa(base)
    if canonical and canonical != clean_text(base):
        return canonical

    base = re.sub(r"\b[0-9A-F]{8,}\b", " ", base)
    base = re.sub(
        r"\b(CARTA|PROPOSTA|OFERTA|CONTRATACAO|TRABALHO|SEJA|BEM|VINDO|PARA|SP|BENECOMP|CLICKSIGN|DOCX|PDF|TXT)\b",
        " ",
        base,
    )
    base = re.sub(r"\b(TITULAR|JUNIOR|COSTA|GENERICO|ALT|DA|DE|DO)\b", " ", base)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip(" -_.")
    cleaned = clean_text(base)
    if not cleaned:
        return None
    if re.fullmatch(r"[0-9A-F _-]{8,}", _ascii_fold(cleaned)):
        return None
    return _canonicalize_empresa(cleaned)


def _extract_company_from_text_alias(texto: str | None) -> str | None:
    normalized = _ascii_fold(texto)
    if not normalized:
        return None
    aliases = [
        (r"\bCONSULTORIA\s*ALFA\b", "CONSULTORIA ALFA LTDA"),
        (r"\bCONSULTORIA\s*BETA\b", "CONSULTORIA BETA LTDA"),
        (r"\bEMPRESA\s*ALFA\b", "EMPRESA ALFA S.A."),
        (r"\bEMPRESA\s*BETA\b", "EMPRESA BETA LTDA"),
        (r"\bCLIENTE\s*ALFA\b", "CLIENTE ALFA S.A."),
        (r"\bCLIENTE\s*BETA\b", "CLIENTE BETA LTDA"),
    ]
    for pattern, canonical in aliases:
        if re.search(pattern, normalized, re.IGNORECASE):
            return canonical
    return None


def _sanitize_empresa(value: str | None, arquivo: str, texto: str | None = None) -> str | None:
    def _clean_candidate(raw: str | None) -> str | None:
        candidate = clean_text(raw)
        if not candidate:
            return None
        candidate = re.sub(r"\{[^}]*\}", " ", candidate)
        candidate = re.sub(r"\b[0-9A-F]{8,}\b", " ", candidate, flags=re.IGNORECASE)
        candidate = re.sub(
            r"\b(CARTA|PROPOSTA|OFERTA|TRABALHO|CONTRATACAO|SEJA|BEM|VINDO|PARA|TITULAR|JUNIOR|COSTA|GENERICO|ALT|PDF|TXT|DOCX|DA|DE|DO)\b",
            " ",
            candidate,
            flags=re.IGNORECASE,
        )
        candidate = re.sub(r"\s+", " ", candidate).strip(" -_.")
        candidate = clean_text(candidate)
        if not candidate:
            return None
        upper = _ascii_fold(candidate)
        if upper in {"CARTA", "PROPOSTA", "OFERTA", "TRABALHO", "DE TRABALHO", "NAO INFORMADO"}:
            return None
        if re.fullmatch(r"[0-9A-F _-]{8,}", upper):
            return None
        if len(candidate) > 80:
            return None
        if "plano de sa" in candidate.lower() or "coparticipa" in candidate.lower():
            return None
        return _canonicalize_empresa(candidate)

    cleaned = _clean_candidate(value)
    if cleaned:
        return cleaned

    text_alias = _extract_company_from_text_alias(texto)
    if text_alias:
        return text_alias

    return _clean_candidate(_extract_company_from_file_name(arquivo))


def _normalize_date(value: str | None) -> str | None:
    value = clean_text(value)
    if not value:
        return None
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
        return value
    month_map = {
        "janeiro": "01", "fevereiro": "02", "março": "03", "marco": "03", "abril": "04",
        "maio": "05", "junho": "06", "julho": "07", "agosto": "08", "setembro": "09",
        "outubro": "10", "novembro": "11", "dezembro": "12",
    }
    match = re.search(r"(\d{1,2})\s+de\s+([A-Za-zçãéôóí]+)\s+de\s+(\d{4})", value, re.IGNORECASE)
    if match:
        dd = int(match.group(1))
        mm = month_map.get(match.group(2).lower())
        yyyy = match.group(3)
        if mm:
            return f"{dd:02d}/{mm}/{yyyy}"
    return None


def parse_proposta_txt(txt_path: str):
    arquivo = os.path.basename(txt_path)
    texto = read_text_file(txt_path)
    cnpjs = CNPJ_PATTERN.findall(texto or "")
    cargo = _sanitize_cargo(_extract_by_patterns(texto, CARGO_LABEL_PATTERNS))
    senioridade = _extract_first(SENIORIDADE_PATTERN, cargo or texto)
    valor = _extract_by_patterns(texto, VALOR_LABEL_PATTERNS)
    dt_inicio = _normalize_date(_extract_by_patterns(texto, INICIO_LABEL_PATTERNS))
    if not dt_inicio:
        dt_inicio = _normalize_date(_extract_first(DATE_PATTERN, texto))
    empresa = _sanitize_empresa(_extract_by_patterns(texto, EMPRESA_LABEL_PATTERNS), arquivo, texto)
    if empresa and empresa.upper() == "BENECOMP":
        empresa = "SOLUTIS"

    pdf_name = _matching_pdf_name(txt_path)
    return {
        "empresa": empresa,
        "cnpj": clean_cnpj(cnpjs[0]) if cnpjs else None,
        "cargo": cargo,
        "senioridade": senioridade,
        "dt_inicio_prevista": dt_inicio,
        "valor_proposto": valor,
        "arquivo_origem_txt": arquivo,
        "arquivo_pdf_validacao": pdf_name,
        "fl_arquivo_pdf_existe": pdf_name is not None,
        "texto_bruto_documento": texto,
    }


def _find_correcoes_csv(pattern: str) -> str | None:
    matches = list_files_by_extension(CORRECOES_DIR, [".csv"], patterns=[pattern])
    return matches[-1] if matches else None


def _first_existing_col(df, candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    return None


def _load_correcoes_propostas():
    path = _find_correcoes_csv("depara_propostas")
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
            pick("cargo_corrigido", "cargo").alias("cargo_corrigido"),
            pick("senioridade_corrigida", "senioridade").alias("senioridade_corrigida"),
            pick("dt_inicio_prevista_corrigida", "dt_inicio_prevista").alias("dt_inicio_prevista_corrigida"),
            pick("valor_proposto_corrigido", "valor_proposto").alias("valor_proposto_corrigido"),
        )
        .filter(F.col("arquivo_origem_txt").isNotNull())
        .dropDuplicates(["arquivo_origem_txt"])
    )


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


def _load_base_propostas():
    path = _find_correcoes_csv("BASE_propostas")
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
            pick("cargo").alias("cargo"),
            pick("senioridade").alias("senioridade"),
            pick("dt_inicio_prevista").alias("dt_inicio_prevista"),
            pick("valor_proposto").alias("valor_proposto"),
            pick("arquivo_origem_txt", "arquivo_origem", "arquivo").alias("arquivo_origem_txt"),
            pick("arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
            _bool_expr(df, "fl_arquivo_pdf_existe").alias("fl_arquivo_pdf_existe"),
            pick("texto_bruto_documento").alias("texto_bruto_documento"),
            ingested_at_expr.alias("ingested_at"),
        )
        .filter(F.col("arquivo_origem_txt").isNotNull())
        .dropDuplicates(["arquivo_origem_txt"])
    )


schema = StructType(
    [
        StructField("empresa", StringType(), True),
        StructField("cnpj", StringType(), True),
        StructField("cargo", StringType(), True),
        StructField("senioridade", StringType(), True),
        StructField("dt_inicio_prevista", StringType(), True),
        StructField("valor_proposto", StringType(), True),
        StructField("arquivo_origem_txt", StringType(), True),
        StructField("arquivo_pdf_validacao", StringType(), True),
        StructField("fl_arquivo_pdf_existe", BooleanType(), True),
        StructField("texto_bruto_documento", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

txts = _list_txt_files(PROPOSTAS_DIR)
rows = [parse_proposta_txt(txt_path) for txt_path in txts]

if rows:
    df_extraido = spark.createDataFrame(rows, schema=schema).withColumn("ingested_at", F.current_timestamp())
else:
    df_extraido = spark.createDataFrame([], schema=schema)

df_bronze = df_extraido

df_base = _load_base_propostas()
if df_base is not None:
    sanitize_empresa_udf = F.udf(lambda empresa, arquivo, texto: _sanitize_empresa(empresa, arquivo, texto), StringType())
    clean_cnpj_udf = F.udf(clean_cnpj, StringType())
    normalize_date_udf = F.udf(_normalize_date, StringType())

    def non_empty_value(col_name: str):
        return F.when(F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit(""))) != "", F.col(col_name).cast("string"))

    def usable_date(col_name: str):
        raw = F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
        return F.when(
            raw.isin("", "1900-01-01", "01/01/1900", "1900/01/01"),
            F.lit(None).cast("string"),
        ).otherwise(normalize_date_udf(raw))

    def usable_money(col_name: str):
        raw = F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
        return F.when(
            raw.isin("", "0", "0,00", "0.00", "R$ 0,00", "R$0,00"),
            F.lit(None).cast("string"),
        ).otherwise(raw)

    df_bronze = (
        df_base.alias("b")
        .join(df_extraido.alias("p"), "arquivo_origem_txt", "full_outer")
        .select(
            F.coalesce(
                sanitize_empresa_udf(F.col("b.empresa"), F.col("arquivo_origem_txt"), F.col("b.texto_bruto_documento")),
                F.col("p.empresa"),
            ).alias("empresa"),
            F.coalesce(clean_cnpj_udf(F.col("b.cnpj")), F.col("p.cnpj")).alias("cnpj"),
            F.coalesce(non_empty_value("b.cargo"), F.col("p.cargo")).alias("cargo"),
            F.coalesce(non_empty_value("b.senioridade"), F.col("p.senioridade")).alias("senioridade"),
            F.coalesce(usable_date("b.dt_inicio_prevista"), F.col("p.dt_inicio_prevista")).alias("dt_inicio_prevista"),
            F.coalesce(usable_money("b.valor_proposto"), F.col("p.valor_proposto")).alias("valor_proposto"),
            F.col("arquivo_origem_txt").alias("arquivo_origem_txt"),
            F.coalesce(F.col("b.arquivo_pdf_validacao"), F.col("p.arquivo_pdf_validacao")).alias("arquivo_pdf_validacao"),
            F.when(
                F.coalesce(F.col("b.fl_arquivo_pdf_existe"), F.lit(False)) | F.coalesce(F.col("p.fl_arquivo_pdf_existe"), F.lit(False)),
                F.lit(True),
            ).otherwise(F.lit(False)).alias("fl_arquivo_pdf_existe"),
            F.coalesce(F.col("p.texto_bruto_documento"), F.col("b.texto_bruto_documento")).alias("texto_bruto_documento"),
            F.coalesce(F.col("b.ingested_at"), F.col("p.ingested_at"), F.current_timestamp()).alias("ingested_at"),
        )
    )

df_correcoes = _load_correcoes_propostas()
if df_correcoes is not None:
    df_bronze = (
        df_bronze.alias("b")
        .join(F.broadcast(df_correcoes).alias("c"), "arquivo_origem_txt", "left")
        .withColumn("empresa", F.coalesce(F.col("c.empresa_corrigida"), F.col("b.empresa")))
        .withColumn("cnpj", F.coalesce(F.col("c.cnpj_corrigido"), F.col("b.cnpj")))
        .withColumn("cargo", F.coalesce(F.col("c.cargo_corrigido"), F.col("b.cargo")))
        .withColumn("senioridade", F.coalesce(F.col("c.senioridade_corrigida"), F.col("b.senioridade")))
        .withColumn("dt_inicio_prevista", F.coalesce(F.col("c.dt_inicio_prevista_corrigida"), F.col("b.dt_inicio_prevista")))
        .withColumn("valor_proposto", F.coalesce(F.col("c.valor_proposto_corrigido"), F.col("b.valor_proposto")))
        .drop(
            "empresa_corrigida",
            "cnpj_corrigido",
            "cargo_corrigido",
            "senioridade_corrigida",
            "dt_inicio_prevista_corrigida",
            "valor_proposto_corrigido",
        )
    )

write_overwrite(df_bronze, BRONZE_RAW_PROPOSTAS, catalog_schema=f"{CATALOG}.{BRONZE}")
