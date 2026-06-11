# Databricks notebook source
# bronze: raw_contratos

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/readers

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------
# MAGIC %run ../../utils/company

# COMMAND ----------

import os
import re
import unicodedata
import hashlib

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

BRONZE_RAW_CONTRATOS = f"{CATALOG}.{BRONZE}.raw_contratos"

money_udf = F.udf(br_money_to_float, "double")
clause_tipo_udf = F.udf(extract_clause_tipo, "string")
clause_flag_exclusividade_udf = F.udf(extract_clause_flag_exclusividade, "int")
clause_flag_nao_concorrencia_udf = F.udf(extract_clause_flag_nao_concorrencia, "int")
clause_snippet_udf = F.udf(extract_clause_snippet, "string")
clause_strategy_udf = F.udf(extract_clause_match_strategy, "string")
clause_score_udf = F.udf(extract_clause_match_score, "int")

INTERNAL_CNPJS = {"11111111111111", "22222222222222"}
TITULAR_CPF_PATTERN = re.compile(r"098\D*414\D*957\D*08")
TITULAR_NOME_PATTERN = re.compile(r"TITULAR\s+JUNIOR\s+DA\s+COSTA\s+GENERICO", re.IGNORECASE)
INTERNAL_COMPANY_PATTERN = re.compile(r"\b(PORTFOLIO|SJ\s*GENERICO|PROVIDER_A)\b", re.IGNORECASE)
CLT_TERMS_PATTERN = re.compile(r"\b(CONTRATO\s+DE\s+TRABALHO|EMPREGADORA?|CTPS|CARTEIRA\s+DE\s+TRABALHO|ADMISSIONAL)\b", re.IGNORECASE)
PJ_TERMS_PATTERN = re.compile(r"\b(PRESTACAO\s+DE\s+SERVICOS|CONTRATANTE|CONTRATADA|PESSOA\s+JURIDICA|ORDEM\s+DE\s+SERVICO)\b", re.IGNORECASE)
INVALID_PARTY_PREFIXES = ("RUA", "RAZAO SOCIAL", "CLICKSIGN", "A)", "(I)", "I)", "ENDERECO", "CNPJ")
CNPJ_PATTERN = re.compile(r"\d{2}[\.,]?\s*\d{3}[\.,]?\s*\d{3}\s*/?\s*\d{4}\s*-?\s*\d{2}")
DATE_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4}")
CARGO_KEYWORDS = (
    "ANALISTA",
    "ENGENHEIRO",
    "CONSULTOR",
    "DESENVOLVEDOR",
    "ARQUITETO",
    "CIENTISTA",
    "ESPECIALISTA",
    "GERENTE",
    "COORDENADOR",
    "TECH LEAD",
)


# COMMAND ----------

def _ascii(value: str | None) -> str:
    text = value or ""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


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


def canonical_company_name(nome_raw, cnpj_raw):
    if nome_raw is None:
        return None
    nome_txt = str(nome_raw).strip()
    if not nome_txt or nome_txt.upper() in {"NAO_INFORMADO", "NULL", "-"}:
        return None
    nome_canonico, _, matched = resolve_empresa_alias(nome_txt, cnpj_raw)
    if matched:
        return nome_canonico
    nome_norm = normalize_company_text(nome_txt)
    return nome_norm or None


canonical_company_udf = F.udf(canonical_company_name, "string")


def _sanitize_party(value: str | None) -> str | None:
    value = clean_text(value)
    if not value:
        return None
    value = re.sub(
        r"\b(?:Endereco|Representante|CNPJ/CPF|CNPJ/MF|CPF/MF)\b.*$",
        "",
        _ascii(value),
        flags=re.IGNORECASE,
    )
    value = re.sub(
        r",?\s*(?:pessoa juridica.*|empresa privada.*|empresa estabelecida.*|com sede.*|inscrita.*)$",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = value.replace("CONTRATANTE:", "").replace("CONTRATADA:", "").strip(" -:;,")
    upper = value.upper()
    if any(upper.startswith(prefix) for prefix in INVALID_PARTY_PREFIXES):
        return None
    if len(value) < 4:
        return None
    return clean_text(value)


def _sanitize_company(value: str | None, arquivo: str | None = None) -> str | None:
    cleaned = _sanitize_party(value)
    if cleaned:
        return cleaned
    return _extract_company_from_file_name(arquivo) if arquivo else None


def _sanitize_cargo(value: str | None) -> str | None:
    value = clean_text(_ascii(value))
    if not value:
        return None
    value = re.split(r"(?:REGIME DE CONTRATACAO|JORNADA|LOCAL DE TRABALHO|OBJETO|CLAUSULA)", value, maxsplit=1)[0]
    value = clean_text(value)
    if len(value) > 120:
        return None
    if not any(keyword in value.upper() for keyword in CARGO_KEYWORDS):
        return None
    return value


def normalized_name_key_expr(col_expr):
    cleaned = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    cleaned = F.regexp_replace(cleaned, r"\([^)]*\)", " ")
    cleaned = F.regexp_replace(cleaned, r"\b\d{6,}\b", " ")
    cleaned = F.regexp_replace(cleaned, r"[^A-Z0-9 ]", " ")
    cleaned = F.regexp_replace(
        cleaned,
        (
            r"\b("
            r"SA|S|A|LTDA|LTD|EIRELI|ME|MEI|EPP|SLU|SS|"
            r"CONSULTORIA|CONSULTING|ASSESSORIA|SERVICOS|SERVIÇOS|SERVICO|"
            r"TECNOLOGIA|TECNOLOGIAS|INFORMATICA|INFORMÁTICA|INFORMACAO|"
            r"COMUNICACAO|DESENVOLVIMENTO|SISTEMAS|SOFTWARE|SOLUCOES|"
            r"SOLUÇÕES|SOLUCAO|NEGOCIOS|OUTSOURCING|BUSINESS|CENTER|"
            r"ENGENHARIA|TREINAMENTOS|COMERCIO|RECURSOS|HUMANOS|PESSOAS|"
            r"INTELIGENCIA|DADOS|PROFISSIONAIS|TRABALHO|DE|DO|DA|DAS|DOS|E|EM"
            r")\b"
        ),
        " ",
    )
    cleaned = F.regexp_replace(cleaned, r"\b[A-Z0-9]\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", "")
    return F.when(cleaned != "", cleaned)


def _normalize_date(value: str | None) -> str | None:
    value = clean_text(value)
    if not value or not re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
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


def sanitize_cliente(value: str | None) -> str | None:
    value = clean_text(_ascii(value))
    if not value:
        return None
    upper = value.upper()
    if upper in {"NAO_INFORMADO", "NULL", "-", "EMPREGADORA"}:
        return None
    for pattern in [
        r"^UM TERCEIRO$",
        r"^TERCEIROS?$",
        r"^CLIENTES? DESTA$",
        r"^TERCEIROS ELENCADOS.*",
        r"^CONTRATANTE PELA CONTRATATA$",
        r"^SER REALIZADA POR .*",
    ]:
        if re.match(pattern, upper):
            return None
    return value


def _normalized_document_name(arquivo: str) -> str:
    base = re.sub(r"\.[^.]+$", "", arquivo)
    base = _ascii(base).upper()
    base = re.sub(r"^[0-9A-F]{8,}__", "", base)
    base = re.sub(r"\b(CLICKSIGN|D4SIGN|DOCUMENTO COMPACTADO|ASSINADO|ALT|FILE)\b", " ", base)
    base = re.sub(r"\(\d+\)", " ", base)
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def _normalized_document_lookup_key(arquivo: str | None) -> str | None:
    if not arquivo:
        return None
    return _normalized_document_name(os.path.basename(arquivo))


def _extract_company_from_file_name(arquivo: str | None) -> str | None:
    if not arquivo:
        return None
    base = _normalized_document_name(arquivo)
    base = re.sub(r"^CONTRATO\s+", "", base)
    return clean_text(base)


def document_file_company_key(value: str | None) -> str | None:
    company_name = _extract_company_from_file_name(value)
    if not company_name:
        return None
    normalized = _ascii(company_name).upper()
    normalized = re.sub(r"\([^)]*\)", " ", normalized)
    normalized = re.sub(r"\b\d{6,}\b", " ", normalized)
    normalized = re.sub(r"[^A-Z0-9 ]", " ", normalized)
    normalized = re.sub(
        (
            r"\b("
            r"SA|S|A|LTDA|LTD|EIRELI|ME|MEI|EPP|SLU|SS|"
            r"CONSULTORIA|CONSULTING|ASSESSORIA|SERVICOS|SERVIÇOS|SERVICO|"
            r"TECNOLOGIA|TECNOLOGIAS|INFORMATICA|INFORMÁTICA|INFORMACAO|"
            r"COMUNICACAO|DESENVOLVIMENTO|SISTEMAS|SOFTWARE|SOLUCOES|"
            r"SOLUÇÕES|SOLUCAO|NEGOCIOS|OUTSOURCING|BUSINESS|CENTER|"
            r"ENGENHARIA|TREINAMENTOS|COMERCIO|RECURSOS|HUMANOS|PESSOAS|"
            r"INTELIGENCIA|DADOS|PROFISSIONAIS|TRABALHO|DE|DO|DA|DAS|DOS|E|EM"
            r")\b"
        ),
        " ",
        normalized,
    )
    normalized = re.sub(r"\b[A-Z0-9]\b", " ", normalized)
    normalized = re.sub(r"\s+", "", normalized).strip()
    return normalized or None


document_file_company_key_udf = F.udf(document_file_company_key, "string")


def _extract_contract_party(texto_norm: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE | re.DOTALL)
        if match:
            return _sanitize_party(match.group(1))
    return None


def _extract_company_generic(texto_norm: str, arquivo: str) -> tuple[str | None, int | None]:
    patterns = [
        r"DE UM LADO,\s*A EMPRESA\s+([^\n,]+)",
        r"DE UM LADO,\s*([^\n,]+?)\s*,\s*DORAVANTE DENOMINAD[AO]\s+EMPREGAD",
        r"ENTRE SI CELEBRAM,\s*DE UM LADO,\s*A\s+([^\n,]+)",
        r"ENTRE SI FAZEM,\s*DE UM LADO,\s*([^\n,]+)",
        r"\bEMPREGADOR(?:A)?\s+([^\n,]+)",
        r"\b([A-Z0-9&\.\- /]+?)\s*,\s*INSCRIT[AO]\s+NO\s+CNPJ",
    ]
    for pattern in patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            candidate = _sanitize_company(match.group(1), arquivo)
            if candidate:
                return candidate, None
    return _extract_company_from_file_name(arquivo), None


def _pick_empresa(contratante: str | None, contratada: str | None, arquivo: str) -> tuple[str | None, int | None]:
    target = None
    cnpj_index = None
    if contratada and re.search(r"\b(PORTFOLIO|SJ GENERICO|PROVIDER_A|TITULAR)\b", contratada, re.IGNORECASE):
        target = contratante
        cnpj_index = 0
    elif contratante and re.search(r"\b(PORTFOLIO|SJ GENERICO|PROVIDER_A|TITULAR)\b", contratante, re.IGNORECASE):
        target = contratada
        cnpj_index = 1
    else:
        target = contratante or contratada or _extract_company_from_file_name(arquivo)
    empresa = _sanitize_company(target, arquivo)
    if empresa:
        return empresa, cnpj_index
    return _extract_company_generic(_ascii(target or ""), arquivo)


def _extract_cnpj(cnpjs: list[str], preferred_index: int | None) -> str | None:
    cleaned = [clean_cnpj(cnpj) for cnpj in cnpjs]
    cleaned = [cnpj for cnpj in cleaned if cnpj]
    if preferred_index is not None and 0 <= preferred_index < len(cnpjs):
        preferred = clean_cnpj(cnpjs[preferred_index])
        if preferred and preferred not in INTERNAL_CNPJS:
            return preferred
    for cnpj in cleaned:
        if cnpj not in INTERNAL_CNPJS:
            return cnpj
    return None


def _extract_cnpj_at(cnpjs: list[str], preferred_index: int | None, allow_internal: bool = False) -> str | None:
    if preferred_index is None or preferred_index < 0 or preferred_index >= len(cnpjs):
        return None
    cnpj = clean_cnpj(cnpjs[preferred_index])
    if not cnpj:
        return None
    if not allow_internal and cnpj in INTERNAL_CNPJS:
        return None
    return cnpj


def _extract_dates(texto_norm: str) -> tuple[str | None, str | None]:
    start_patterns = [
        r"(?:INICIO|DATA DE INICIO|VIGENCIA INICIAL)\s*:\s*(\d{2}/\d{2}/\d{4})",
        r"A DATA DE INICIO DO REFERIDO CONTRATO E\s*(\d{2}/\d{2}/\d{4})",
        r"ADMISSAO\s*:?\s*(\d{2}/\d{2}/\d{4})",
    ]
    end_patterns = [
        r"(?:TERMINO|DATA FINAL|DATA DE TERMINO|VIGENCIA FINAL)\s*:\s*(\d{2}/\d{2}/\d{4})",
    ]

    dt_inicio = None
    for pattern in start_patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            dt_inicio = _normalize_date(match.group(1))
            break

    dt_fim = None
    for pattern in end_patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            dt_fim = _normalize_date(match.group(1))
            break

    if dt_inicio or dt_fim:
        return fix_dates(dt_inicio, dt_fim)

    datas = DATE_PATTERN.findall(texto_norm or "")
    valid = [_normalize_date(d) for d in datas]
    valid = [d for d in valid if d]
    if len(valid) >= 2:
        return fix_dates(valid[0], valid[1])
    if len(valid) == 1:
        return valid[0], None
    return None, None


def _extract_cargo(texto_norm: str) -> str | None:
    label_patterns = [
        r"Cargo\s*:\s*([^\n]+)",
        r"Posicao\s*:\s*([^\n]+)",
        r"FUNCOES DE:\s*([^\n\.]+)",
    ]
    for pattern in label_patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            cargo = _sanitize_cargo(match.group(1))
            if cargo:
                return cargo

    role_patterns = [
        r"\b(ANALISTA(?:\s+DE\s+[A-Z ]+)?)\b",
        r"\b(ENGENHEIRO(?:\s+DE\s+[A-Z ]+)?)\b",
        r"\b(CONSULTOR(?:\s+DE\s+[A-Z ]+)?)\b",
        r"\b(DESENVOLVEDOR(?:\s+[A-Z ]+)?)\b",
        r"\b(ARQUITETO(?:\s+DE\s+[A-Z ]+)?)\b",
        r"\b(CIENTISTA(?:\s+DE\s+[A-Z ]+)?)\b",
        r"\b(ESPECIALISTA(?:\s+DE\s+[A-Z ]+)?)\b",
        r"\b(GERENTE(?:\s+DE\s+[A-Z ]+)?)\b",
    ]
    for pattern in role_patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            cargo = _sanitize_cargo(match.group(1))
            if cargo:
                return cargo
    return None


def _extract_valor(texto_norm: str) -> str | None:
    patterns = [
        r"(?:VALOR MENSAL|REMUNERACAO MENSAL|SALARIO MENSAL)\s*:\s*R\$\s*([\d]{1,3}(?:\.[\d]{3})*,\d{2})",
        r"VALOR\s+DE\s+R\$\s*([\d]{1,3}(?:\.[\d]{3})*,\d{2})(?:\s*\([^)]+\))?\s*(?:MENSAIS|MENSAL|POR MES|/MES)",
        r"SALARIO\s*R\$\s*([\d]{1,3}(?:\.[\d]{3})*,\d{2})(?:\s*\([^)]+\))?\s*(?:/MES|MENSAL)",
        r"AJUDA DE CUSTOS MENSAIS NO VALOR DE R\$\s*([\d]{1,3}(?:\.[\d]{3})*,\d{2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            return clean_text(match.group(1))
    return None


def _extract_cliente_texto(texto_norm: str, arquivo: str) -> str | None:
    patterns = [
        r"(?:CLIENTE|EMPRESA CLIENTE|TOMADOR)\s*:\s*([^\n]+)",
        r"PROJETO(?:\s+PARA)?\s*:\s*([^\n]+)",
        r"LOCAL\s+DE\s+ALOCACAO\s*:\s*([^\n]+)",
        r"PRESTACAO\s+DE\s+SERVICOS\s+(?:A|PARA)\s+([^\n,]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, texto_norm, re.IGNORECASE)
        if match:
            candidate = _sanitize_company(match.group(1), arquivo)
            if candidate:
                return candidate
    return None


def _same_company(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    norm_a = re.sub(r"[^A-Z0-9]", "", _ascii(a).upper())
    norm_b = re.sub(r"[^A-Z0-9]", "", _ascii(b).upper())
    return norm_a != "" and norm_a == norm_b


def _resolve_roles(tipo_vinculo: str, contratante: str | None, contratada: str | None, empresa: str | None, arquivo: str, texto_norm: str, cnpjs: list[str]):
    contratante_clean = _sanitize_company(contratante, arquivo)
    contratada_clean = _sanitize_company(contratada, arquivo)
    empresa_clean = _sanitize_company(empresa, arquivo)
    cliente_texto = _extract_cliente_texto(texto_norm, arquivo)

    contratante_internal = bool(contratante_clean and re.search(r"\b(PORTFOLIO|SJ GENERICO|PROVIDER_A|TITULAR)\b", contratante_clean, re.IGNORECASE))
    contratada_internal = bool(contratada_clean and re.search(r"\b(PORTFOLIO|SJ GENERICO|PROVIDER_A|TITULAR)\b", contratada_clean, re.IGNORECASE))

    empresa_prestador = None
    cnpj_prestador = None
    consultoria = None
    cnpj_consultoria = None
    empresa_cliente = sanitize_cliente(cliente_texto)

    if tipo_vinculo == "PJ":
        if contratante_internal and not contratada_internal:
            empresa_prestador = contratante_clean
            cnpj_prestador = _extract_cnpj_at(cnpjs, 0, allow_internal=True)
            consultoria = contratada_clean
            cnpj_consultoria = _extract_cnpj_at(cnpjs, 1)
        elif contratada_internal and not contratante_internal:
            empresa_prestador = contratada_clean
            cnpj_prestador = _extract_cnpj_at(cnpjs, 1, allow_internal=True)
            consultoria = contratante_clean
            cnpj_consultoria = _extract_cnpj_at(cnpjs, 0)
        else:
            consultoria = empresa_clean or contratada_clean or contratante_clean
            cnpj_consultoria = _extract_cnpj(cnpjs, None)

        if not empresa_cliente and empresa_clean and not _same_company(empresa_clean, consultoria):
            empresa_cliente = sanitize_cliente(empresa_clean)
        if _same_company(empresa_cliente, consultoria):
            empresa_cliente = None
    else:
        consultoria = empresa_clean or contratante_clean or contratada_clean
        cnpj_consultoria = _extract_cnpj(cnpjs, None)
        if not empresa_cliente:
            empresa_cliente = sanitize_cliente(empresa_clean)

    if _same_company(empresa_prestador, consultoria):
        consultoria = None
        cnpj_consultoria = None

    return {
        "empresa_prestador": empresa_prestador,
        "cnpj_prestador": cnpj_prestador,
        "consultoria": consultoria,
        "cnpj_consultoria": cnpj_consultoria,
        "empresa_cliente": empresa_cliente,
    }


def _detect_tipo_vinculo(source_path: str, texto: str | None, texto_norm: str) -> tuple[str, str]:
    path_norm = _ascii(source_path).upper().replace("\\", "/")
    digits_text = re.sub(r"\D", "", texto or "")

    has_internal = bool(INTERNAL_COMPANY_PATTERN.search(texto_norm)) or any(cnpj in digits_text for cnpj in INTERNAL_CNPJS)
    has_titular_cpf = bool(TITULAR_CPF_PATTERN.search(texto or "")) or ("00000000000" in digits_text)
    has_titular_nome = bool(TITULAR_NOME_PATTERN.search(texto_norm))
    has_clt_terms = bool(CLT_TERMS_PATTERN.search(texto_norm))
    has_pj_terms = bool(PJ_TERMS_PATTERN.search(texto_norm))

    if has_internal and has_pj_terms:
        return "PJ", "empresa_propria_e_termos_pj"
    if has_pj_terms and not has_clt_terms:
        return "PJ", "somente_termos_pj"
    if has_clt_terms and (has_titular_cpf or has_titular_nome) and not has_internal:
        return "CLT", "titular_e_termos_clt"
    if has_clt_terms and not has_pj_terms:
        return "CLT", "somente_termos_clt"
    if "/CTPS/" in path_norm or "/CLT/" in path_norm:
        return "CLT", "pasta_ctps_ou_clt"
    if "/CONTRATOS_PJ/" in path_norm or "/PJ/" in path_norm:
        return "PJ", "pasta_pj"
    if has_internal:
        return "PJ", "empresa_propria"
    if has_titular_cpf or has_titular_nome:
        return "CLT", "titular_detectado"
    return "INDEFINIDO", "sem_sinal_forte"


def _document_key(arquivo: str, texto_norm: str) -> str:
    text_basis = re.sub(r"\s+", " ", texto_norm or "").strip()
    if text_basis:
        return hashlib.sha1(text_basis[:5000].encode("utf-8", errors="ignore")).hexdigest()
    return _normalized_document_name(arquivo)


# COMMAND ----------

def read_planilha_pj():
    paths = list_files_by_extension(EMPRESAS_PJ_DIR, [".csv"])
    dfs = []
    for path in paths:
        df_raw = _read_csv_local(path)
        cols = df_raw.columns
        available_cols = set(cols)
        dfs.append(
            df_raw.select(
                F.lit("PLANILHA_PJ").alias("fonte_referencia"),
                norm_text("TP_VINCULO").alias("tp_vinculo") if _pick_col(cols, "TP_VINCULO") else F.lit("PJ").alias("tp_vinculo"),
                canonical_company_udf(_col_or_null(cols, "EMPRESA_PRESTADOR"), normalize_cnpj_expr(only_digits_expr(_pick_col(cols, "CNPJ_PRESTADOR")) if _pick_col(cols, "CNPJ_PRESTADOR") else F.lit(""))).alias("empresa_prestador"),
                normalize_cnpj_expr(only_digits_expr(_pick_col(cols, "CNPJ_PRESTADOR")) if _pick_col(cols, "CNPJ_PRESTADOR") else F.lit("")).alias("cnpj_prestador"),
                canonical_company_udf(_col_or_null(cols, "CONSULTORIA"), normalize_cnpj_expr(only_digits_expr(_pick_col(cols, "CNPJ_CONSULTORIA")) if _pick_col(cols, "CNPJ_CONSULTORIA") else F.lit(""))).alias("consultoria"),
                normalize_cnpj_expr(only_digits_expr(_pick_col(cols, "CNPJ_CONSULTORIA")) if _pick_col(cols, "CNPJ_CONSULTORIA") else F.lit("")).alias("cnpj_consultoria"),
                first_non_empty_string_expr(available_cols, "EMPRESA_CLIENTE", "CLIENTE_FINAL", "CLIENTE").alias("empresa_cliente"),
                first_non_empty_string_expr(available_cols, "CARGO", "FUNCAO", "FUNÇÃO").alias("cargo"),
                first_non_empty_string_expr(available_cols, "DT_INICIO", "DATA_INICIO", "DATA INICIO", "INICIO", "INÍCIO").alias("dt_inicio"),
                first_non_empty_string_expr(available_cols, "DT_FIM", "DATA_FIM", "DATA FIM", "FIM").alias("dt_fim"),
                first_non_empty_string_expr(
                    available_cols,
                    "SALARIO",
                    "SALÁRIO",
                    "VALOR_MENSAL",
                    "VALOR MENSAL",
                    "VALOR",
                    "REMUNERACAO",
                    "REMUNERAÇÃO",
                    "REMUNERACAO_MENSAL",
                    "REMUNERAÇÃO_MENSAL",
                    "SALARIO_MENSAL",
                    "SALÁRIO_MENSAL",
                ).alias("valor_mensal"),
                first_non_empty_string_expr(available_cols, "STATUS", "SITUACAO", "SITUAÇÃO").alias("status"),
                F.lit(os.path.basename(path)).alias("arquivo_origem_referencia"),
            )
        )
    if not dfs:
        return spark.createDataFrame([], schema="fonte_referencia string, tp_vinculo string, empresa_prestador string, cnpj_prestador string, consultoria string, cnpj_consultoria string, empresa_cliente string, cargo string, dt_inicio string, dt_fim string, valor_mensal string, status string, arquivo_origem_referencia string")
    df = dfs[0]
    for nxt in dfs[1:]:
        df = df.unionByName(nxt)
    return df.withColumn("empresa_cliente", F.udf(sanitize_cliente, StringType())("empresa_cliente"))


def read_planilha_clt():
    paths = list_files_by_extension(EMPRESAS_CLT_DIR, [".csv"])
    dfs = []
    for path in paths:
        df_raw = _read_csv_local(path)
        cols = df_raw.columns
        available_cols = set(cols)
        cliente_expr = (
            first_non_empty_string_expr(available_cols, "EMPRESA_CLIENTE", "CLIENTE_FINAL", "CLIENTE")
            if {"EMPRESA_CLIENTE", "CLIENTE_FINAL", "CLIENTE"} & available_cols
            else canonical_company_udf(
                _col_or_null(cols, "Empresa"),
                normalize_cnpj_expr(only_digits_expr(_pick_col(cols, "CNPJ")) if _pick_col(cols, "CNPJ") else F.lit("")),
            )
        )
        dfs.append(
            df_raw.select(
                F.lit("PLANILHA_CLT").alias("fonte_referencia"),
                F.lit("CLT").alias("tp_vinculo"),
                F.lit(None).cast("string").alias("empresa_prestador"),
                F.lit(None).cast("string").alias("cnpj_prestador"),
                canonical_company_udf(_col_or_null(cols, "Empresa"), normalize_cnpj_expr(only_digits_expr(_pick_col(cols, "CNPJ")) if _pick_col(cols, "CNPJ") else F.lit(""))).alias("consultoria"),
                normalize_cnpj_expr(only_digits_expr(_pick_col(cols, "CNPJ")) if _pick_col(cols, "CNPJ") else F.lit("")).alias("cnpj_consultoria"),
                cliente_expr.alias("empresa_cliente"),
                _col_or_null(cols, "Cargo").alias("cargo"),
                _col_or_null(cols, "Data Inicio", "Data Início").alias("dt_inicio"),
                _col_or_null(cols, "Data Fim").alias("dt_fim"),
                _col_or_null(cols, "Salario", "Salário").alias("valor_mensal"),
                _col_or_null(cols, "Status").alias("status"),
                F.lit(os.path.basename(path)).alias("arquivo_origem_referencia"),
            )
        )
    if not dfs:
        return spark.createDataFrame([], schema="fonte_referencia string, tp_vinculo string, empresa_prestador string, cnpj_prestador string, consultoria string, cnpj_consultoria string, empresa_cliente string, cargo string, dt_inicio string, dt_fim string, valor_mensal string, status string, arquivo_origem_referencia string")
    df = dfs[0]
    for nxt in dfs[1:]:
        df = df.unionByName(nxt)
    return df.withColumn("empresa_cliente", F.udf(sanitize_cliente, StringType())("empresa_cliente"))


# COMMAND ----------

document_lookup_key_udf = F.udf(_normalized_document_lookup_key, "string")


def read_exclusividade_referencia():
    paths = list_files_by_extension(EXCLUSIVIDADE_DIR, [".csv"])
    schema = (
        "arquivo_pdf_validacao string, arquivo_origem_txt string, tp_clausula_exclusividade string, "
        "fl_clausula_exclusividade int, fl_clausula_nao_concorrencia int, trecho_clausula_exclusividade string, "
        "criterio_extracao_clausula string, score_extracao_clausula int, arquivo_origem_referencia_clausula string"
    )
    if not paths:
        return spark.createDataFrame([], schema=schema)

    dfs = []
    for path in paths:
        df_raw = _read_csv_local(path)
        cols = df_raw.columns
        dfs.append(
            df_raw.select(
                first_non_empty_string_expr(cols, "ARQUIVO_PDF_VALIDACAO", "ARQUIVO_PDF", "ARQUIVO").alias("arquivo_pdf_validacao"),
                first_non_empty_string_expr(cols, "ARQUIVO_ORIGEM_TXT", "ARQUIVO_TXT").alias("arquivo_origem_txt"),
                first_non_empty_string_expr(cols, "TP_CLAUSULA_EXCLUSIVIDADE").alias("tp_clausula_exclusividade"),
                F.col(_pick_col(cols, "FL_CLAUSULA_EXCLUSIVIDADE")).cast("int").alias("fl_clausula_exclusividade") if _pick_col(cols, "FL_CLAUSULA_EXCLUSIVIDADE") else F.lit(None).cast("int").alias("fl_clausula_exclusividade"),
                F.col(_pick_col(cols, "FL_CLAUSULA_NAO_CONCORRENCIA")).cast("int").alias("fl_clausula_nao_concorrencia") if _pick_col(cols, "FL_CLAUSULA_NAO_CONCORRENCIA") else F.lit(None).cast("int").alias("fl_clausula_nao_concorrencia"),
                first_non_empty_string_expr(cols, "TRECHO_CLAUSULA_EXCLUSIVIDADE", "TRECHO").alias("trecho_clausula_exclusividade"),
                first_non_empty_string_expr(cols, "CRITERIO_EXTRACAO_CLAUSULA", "CRITERIO").alias("criterio_extracao_clausula"),
                F.col(_pick_col(cols, "SCORE_EXTRACAO_CLAUSULA", "SCORE")).cast("int").alias("score_extracao_clausula") if _pick_col(cols, "SCORE_EXTRACAO_CLAUSULA", "SCORE") else F.lit(None).cast("int").alias("score_extracao_clausula"),
                F.lit(os.path.basename(path)).alias("arquivo_origem_referencia_clausula"),
            )
        )

    df = dfs[0]
    for nxt in dfs[1:]:
        df = df.unionByName(nxt)

    return (
        df
        .withColumn("arquivo_pdf_validacao", clean_text_udf(F.col("arquivo_pdf_validacao")))
        .withColumn("arquivo_origem_txt", clean_text_udf(F.col("arquivo_origem_txt")))
        .withColumn("tp_clausula_exclusividade", F.when(clean_text_udf(F.col("tp_clausula_exclusividade")) == "", F.lit(None)).otherwise(clean_text_udf(F.col("tp_clausula_exclusividade"))))
        .withColumn("trecho_clausula_exclusividade", clean_text_udf(F.col("trecho_clausula_exclusividade")))
        .withColumn("criterio_extracao_clausula", clean_text_udf(F.col("criterio_extracao_clausula")))
        .withColumn("lookup_pdf_key", document_lookup_key_udf(F.col("arquivo_pdf_validacao")))
        .withColumn("lookup_txt_key", document_lookup_key_udf(F.col("arquivo_origem_txt")))
        .withColumn(
            "lookup_key",
            F.coalesce(F.col("lookup_pdf_key"), F.col("lookup_txt_key")),
        )
        .drop("lookup_pdf_key", "lookup_txt_key")
        .dropDuplicates(["lookup_key"])
    )


clean_text_udf = F.udf(clean_text, "string")


# COMMAND ----------

def _collect_document_sources():
    txt_paths = list_files_by_extension_recursive(CONTRATOS_PJ_DIR, [".txt"]) + list_files_by_extension_recursive(CONTRATOS_CLT_DIR, [".txt"])
    pdf_paths = list_files_by_extension_recursive(CONTRATOS_PJ_DIR, [".pdf"]) + list_files_by_extension_recursive(CONTRATOS_CLT_DIR, [".pdf"])
    grouped = {}
    for path in sorted(set(txt_paths)):
        key = _normalized_document_name(os.path.basename(path))
        grouped.setdefault(key, {})["txt_path"] = path
    for path in sorted(set(pdf_paths)):
        key = _normalized_document_name(os.path.basename(path))
        grouped.setdefault(key, {})["pdf_path"] = path
    return list(grouped.values())


def parse_document_group(item: dict) -> dict | None:
    txt_path = item.get("txt_path")
    if not txt_path:
        return None
    pdf_path = item.get("pdf_path")
    arquivo = os.path.basename(txt_path)
    texto = open(txt_path, "r", encoding="utf-8", errors="ignore").read()
    texto_norm = _ascii(texto).upper()
    tipo_vinculo, criterio_tipo = _detect_tipo_vinculo(txt_path, texto, texto_norm)
    if tipo_vinculo == "INDEFINIDO":
        return None

    cnpjs = CNPJ_PATTERN.findall(texto or "")
    contratante = _extract_contract_party(
        texto_norm,
        [
            r"CONTRATANTE:\s*([^\n]+)",
            r"ESTA CONTRATACAO E REALIZADA ENTRE:\s*([^\n]+)",
            r"DE UM LADO,\s*([^\n]+?)\s*,\s*PESSOA JURIDICA",
            r"DE UM LADO\s+([^\n]+?)\s*,\s*EMPRESA",
        ],
    )
    contratada = _extract_contract_party(
        texto_norm,
        [
            r"CONTRATADA:\s*([^\n]+)",
            r"E DE OUTRO LADO\s+([^\n]+?)\s*,\s*PESSOA JURIDICA",
            r"E, DE OUTRO LADO,\s*([^\n]+?)\s*,\s*PESSOA JURIDICA",
        ],
    )
    empresa, cnpj_index = _pick_empresa(contratante, contratada, arquivo)
    if not empresa:
        empresa, cnpj_index = _extract_company_generic(texto_norm, arquivo)
    cargo = _extract_cargo(texto_norm)
    dt_inicio, dt_fim = _extract_dates(texto_norm)
    valor = _extract_valor(texto_norm)
    role_fields = _resolve_roles(tipo_vinculo, contratante, contratada, empresa, arquivo, texto_norm, cnpjs)

    return {
        "fonte_referencia": "DOCUMENTO",
        "tp_vinculo": tipo_vinculo,
        "empresa_prestador": role_fields["empresa_prestador"],
        "cnpj_prestador": role_fields["cnpj_prestador"],
        "consultoria": role_fields["consultoria"] or empresa,
        "cnpj_consultoria": role_fields["cnpj_consultoria"] or _extract_cnpj(cnpjs, cnpj_index),
        "empresa_cliente": role_fields["empresa_cliente"],
        "cargo": cargo,
        "dt_inicio": dt_inicio,
        "dt_fim": dt_fim,
        "valor_mensal": valor,
        "status": None,
        "arquivo_origem_referencia": None,
        "arquivo_origem_txt": arquivo,
        "arquivo_pdf_validacao": os.path.basename(pdf_path) if pdf_path else None,
        "fl_arquivo_pdf_existe": pdf_path is not None,
        "texto_bruto_contrato": texto,
        "tp_clausula_exclusividade": None,
        "fl_clausula_exclusividade": None,
        "fl_clausula_nao_concorrencia": None,
        "trecho_clausula_exclusividade": None,
        "criterio_extracao_clausula": None,
        "score_extracao_clausula": None,
        "criterio_match_documento": criterio_tipo,
        "_documento_key": _document_key(arquivo, texto_norm),
    }


def _merge_document_rows(rows: list[dict]) -> list[dict]:
    merged = {}
    for row in rows:
        key = row.get("_documento_key") or row.get("arquivo_origem_txt")
        current = merged.get(key)
        if current is None:
            merged[key] = dict(row)
            continue
        winner, loser = current, row
        if sum(bool(row.get(k)) for k in row.keys()) > sum(bool(current.get(k)) for k in current.keys()):
            winner, loser = row, current
        combined = dict(winner)
        for field, value in loser.items():
            if field == "_documento_key":
                continue
            if not combined.get(field) and value:
                combined[field] = value
        merged[key] = combined
    out = []
    for row in merged.values():
        row.pop("_documento_key", None)
        out.append(row)
    return out


def read_documents_df():
    rows = []
    for item in _collect_document_sources():
        parsed = parse_document_group(item)
        if parsed:
            rows.append(parsed)
    rows = _merge_document_rows(rows)
    schema = StructType(
        [
            StructField("fonte_referencia", StringType(), True),
            StructField("tp_vinculo", StringType(), True),
            StructField("empresa_prestador", StringType(), True),
            StructField("cnpj_prestador", StringType(), True),
            StructField("consultoria", StringType(), True),
            StructField("cnpj_consultoria", StringType(), True),
            StructField("empresa_cliente", StringType(), True),
            StructField("cargo", StringType(), True),
            StructField("dt_inicio", StringType(), True),
            StructField("dt_fim", StringType(), True),
            StructField("valor_mensal", StringType(), True),
            StructField("status", StringType(), True),
            StructField("arquivo_origem_referencia", StringType(), True),
            StructField("arquivo_origem_txt", StringType(), True),
            StructField("arquivo_pdf_validacao", StringType(), True),
            StructField("fl_arquivo_pdf_existe", BooleanType(), True),
            StructField("texto_bruto_contrato", StringType(), True),
            StructField("tp_clausula_exclusividade", StringType(), True),
            StructField("fl_clausula_exclusividade", IntegerType(), True),
            StructField("fl_clausula_nao_concorrencia", IntegerType(), True),
            StructField("trecho_clausula_exclusividade", StringType(), True),
            StructField("criterio_extracao_clausula", StringType(), True),
            StructField("score_extracao_clausula", IntegerType(), True),
            StructField("criterio_match_documento", StringType(), True),
        ]
    )
    if not rows:
        df = spark.createDataFrame([], schema=schema)
    else:
        df = spark.createDataFrame(rows, schema=schema)

    df = (
        df
        .withColumn("empresa_prestador", canonical_company_udf(F.col("empresa_prestador"), F.col("cnpj_prestador")))
        .withColumn("consultoria", canonical_company_udf(F.col("consultoria"), F.col("cnpj_consultoria")))
        .withColumn("empresa_cliente", F.udf(sanitize_cliente, StringType())(F.col("empresa_cliente")))
        .withColumn("tp_clausula_exclusividade", clause_tipo_udf("texto_bruto_contrato"))
        .withColumn("fl_clausula_exclusividade", clause_flag_exclusividade_udf("texto_bruto_contrato"))
        .withColumn("fl_clausula_nao_concorrencia", clause_flag_nao_concorrencia_udf("texto_bruto_contrato"))
        .withColumn("trecho_clausula_exclusividade", clause_snippet_udf("texto_bruto_contrato"))
        .withColumn("criterio_extracao_clausula", clause_strategy_udf("texto_bruto_contrato"))
        .withColumn("score_extracao_clausula", clause_score_udf("texto_bruto_contrato"))
    )

    df_ref_clausula = read_exclusividade_referencia()
    if df_ref_clausula.limit(1).count() == 0:
        return df

    df_ref_clausula = df_ref_clausula.select(
        F.col("lookup_key").alias("cl_lookup_key"),
        F.col("tp_clausula_exclusividade").alias("cl_tp_clausula_exclusividade"),
        F.col("fl_clausula_exclusividade").alias("cl_fl_clausula_exclusividade"),
        F.col("fl_clausula_nao_concorrencia").alias("cl_fl_clausula_nao_concorrencia"),
        F.col("trecho_clausula_exclusividade").alias("cl_trecho_clausula_exclusividade"),
        F.col("criterio_extracao_clausula").alias("cl_criterio_extracao_clausula"),
        F.col("score_extracao_clausula").alias("cl_score_extracao_clausula"),
    )

    return (
        df
        .withColumn("doc_pdf_key", document_lookup_key_udf(F.col("arquivo_pdf_validacao")))
        .withColumn("doc_txt_key", document_lookup_key_udf(F.col("arquivo_origem_txt")))
        .join(
            df_ref_clausula.alias("cl"),
            (F.col("doc_pdf_key") == F.col("cl.cl_lookup_key")) | (F.col("doc_txt_key") == F.col("cl.cl_lookup_key")),
            "left",
        )
        .withColumn("fl_referencia_clausula_match", F.col("cl.cl_lookup_key").isNotNull())
        .withColumn(
            "tp_clausula_exclusividade",
            F.when(F.col("fl_referencia_clausula_match"), F.col("cl.cl_tp_clausula_exclusividade")).otherwise(F.col("tp_clausula_exclusividade")),
        )
        .withColumn(
            "fl_clausula_exclusividade",
            F.when(F.col("fl_referencia_clausula_match"), F.col("cl.cl_fl_clausula_exclusividade")).otherwise(F.col("fl_clausula_exclusividade")),
        )
        .withColumn(
            "fl_clausula_nao_concorrencia",
            F.when(F.col("fl_referencia_clausula_match"), F.coalesce(F.col("cl.cl_fl_clausula_nao_concorrencia"), F.lit(0))).otherwise(F.col("fl_clausula_nao_concorrencia")),
        )
        .withColumn(
            "trecho_clausula_exclusividade",
            F.when(F.col("fl_referencia_clausula_match"), F.col("cl.cl_trecho_clausula_exclusividade")).otherwise(F.col("trecho_clausula_exclusividade")),
        )
        .withColumn(
            "criterio_extracao_clausula",
            F.when(F.col("fl_referencia_clausula_match"), F.coalesce(F.col("cl.cl_criterio_extracao_clausula"), F.lit("REFERENCIA_MANUAL"))).otherwise(F.col("criterio_extracao_clausula")),
        )
        .withColumn(
            "score_extracao_clausula",
            F.when(F.col("fl_referencia_clausula_match"), F.coalesce(F.col("cl.cl_score_extracao_clausula"), F.lit(0))).otherwise(F.col("score_extracao_clausula")),
        )
        .drop(
            "doc_pdf_key",
            "doc_txt_key",
            "fl_referencia_clausula_match",
            "cl_lookup_key",
            "cl_tp_clausula_exclusividade",
            "cl_fl_clausula_exclusividade",
            "cl_fl_clausula_nao_concorrencia",
            "cl_trecho_clausula_exclusividade",
            "cl_criterio_extracao_clausula",
            "cl_score_extracao_clausula",
        )
    )


# COMMAND ----------

df_ref = read_planilha_pj().unionByName(read_planilha_clt())
df_docs = read_documents_df()

df_ref_match = (
    df_ref
    .withColumn("ref_row_id", F.monotonically_increasing_id())
    .withColumn("ref_consultoria_key", normalized_name_key_expr(F.col("consultoria")))
    .withColumn(
        "ref_dt_inicio_date",
        F.coalesce(
            F.expr("try_to_date(dt_inicio, 'dd/MM/yyyy')"),
            F.expr("try_to_date(dt_inicio, 'yyyy-MM-dd')"),
            F.expr("try_to_date(dt_inicio)"),
        ),
    )
)

df_docs_match = (
    df_docs
    .withColumn("doc_row_id", F.monotonically_increasing_id())
    .withColumn("doc_consultoria_key", normalized_name_key_expr(F.col("consultoria")))
    .withColumn("doc_file_company_key", document_file_company_key_udf(F.col("arquivo_origem_txt")))
    .withColumn(
        "doc_dt_inicio_date",
        F.coalesce(
            F.expr("try_to_date(dt_inicio, 'dd/MM/yyyy')"),
            F.expr("try_to_date(dt_inicio, 'yyyy-MM-dd')"),
            F.expr("try_to_date(dt_inicio)"),
        ),
    )
)

cnpj_match = (
    F.col("ref.cnpj_consultoria").isNotNull()
    & F.col("doc.cnpj_consultoria").isNotNull()
    & (F.col("ref.cnpj_consultoria") == F.col("doc.cnpj_consultoria"))
)

name_match = (
    F.col("ref.ref_consultoria_key").isNotNull()
    & F.col("doc.doc_consultoria_key").isNotNull()
    & (F.col("ref.ref_consultoria_key") == F.col("doc.doc_consultoria_key"))
)

file_name_match = (
    F.col("ref.ref_consultoria_key").isNotNull()
    & F.col("doc.doc_file_company_key").isNotNull()
    & (F.col("ref.ref_consultoria_key") == F.col("doc.doc_file_company_key"))
)

name_compatible = (
    name_match
    | file_name_match
    | F.col("ref.ref_consultoria_key").isNull()
    | F.col("doc.doc_consultoria_key").isNull()
)

join_cond = (
    (F.col("ref.tp_vinculo") == F.col("doc.tp_vinculo"))
    & (
        (cnpj_match & name_compatible)
        | name_match
        | file_name_match
    )
)

matched_candidates = (
    df_ref_match.alias("ref")
    .join(df_docs_match.alias("doc"), join_cond, "left")
    .withColumn(
        "match_priority",
        F.when(cnpj_match & name_compatible, F.lit(0))
         .when(name_match, F.lit(1))
         .when(file_name_match, F.lit(2))
         .otherwise(F.lit(99)),
    )
    .withColumn(
        "date_distance",
        F.when(
            F.col("ref.ref_dt_inicio_date").isNotNull() & F.col("doc.doc_dt_inicio_date").isNotNull(),
            F.abs(F.datediff(F.col("ref.ref_dt_inicio_date"), F.col("doc.doc_dt_inicio_date"))),
        ).otherwise(F.lit(999999)),
    )
)

w_doc = Window.partitionBy("doc.doc_row_id").orderBy(
    F.col("match_priority").asc(),
    F.col("date_distance").asc(),
    F.when(F.col("ref.ref_dt_inicio_date").isNotNull(), F.lit(0)).otherwise(F.lit(1)).asc(),
    F.col("ref.ref_dt_inicio_date").desc_nulls_last(),
    F.col("ref.ref_row_id").asc(),
)

df_best_doc_match = (
    matched_candidates
    .filter(F.col("doc.doc_row_id").isNotNull())
    .withColumn("rn_doc", F.row_number().over(w_doc))
    .filter(F.col("rn_doc") == 1)
    .select(
        F.col("ref.ref_row_id").alias("ref_row_id"),
        F.col("doc.doc_row_id").alias("doc_row_id"),
        F.col("doc.empresa_cliente").alias("doc_empresa_cliente"),
        F.col("doc.cargo").alias("doc_cargo"),
        F.col("doc.dt_inicio").alias("doc_dt_inicio"),
        F.col("doc.dt_fim").alias("doc_dt_fim"),
        F.col("doc.valor_mensal").alias("doc_valor_mensal"),
        F.col("doc.arquivo_origem_txt").alias("arquivo_origem_txt"),
        F.col("doc.arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
        F.coalesce(F.col("doc.fl_arquivo_pdf_existe"), F.lit(False)).alias("fl_arquivo_pdf_existe"),
        F.col("doc.texto_bruto_contrato").alias("texto_bruto_contrato"),
        F.col("doc.tp_clausula_exclusividade").alias("tp_clausula_exclusividade"),
        F.col("doc.fl_clausula_exclusividade").alias("fl_clausula_exclusividade"),
        F.col("doc.fl_clausula_nao_concorrencia").alias("fl_clausula_nao_concorrencia"),
        F.col("doc.trecho_clausula_exclusividade").alias("trecho_clausula_exclusividade"),
        F.col("doc.criterio_extracao_clausula").alias("criterio_extracao_clausula"),
        F.col("doc.score_extracao_clausula").alias("score_extracao_clausula"),
        F.col("doc.criterio_match_documento").alias("criterio_match_documento"),
    )
)

df_joined = (
    df_ref_match.alias("ref")
    .join(df_best_doc_match.alias("best"), F.col("ref.ref_row_id") == F.col("best.ref_row_id"), "left")
    .select(
        F.col("ref.fonte_referencia").alias("fonte_referencia"),
        F.col("ref.tp_vinculo").alias("tp_vinculo"),
        F.col("ref.empresa_prestador").alias("empresa_prestador"),
        F.col("ref.cnpj_prestador").alias("cnpj_prestador"),
        F.col("ref.consultoria").alias("consultoria"),
        F.col("ref.cnpj_consultoria").alias("cnpj_consultoria"),
        F.coalesce(F.col("ref.empresa_cliente"), F.col("best.doc_empresa_cliente")).alias("empresa_cliente"),
        F.coalesce(F.col("ref.cargo"), F.col("best.doc_cargo")).alias("cargo"),
        F.coalesce(F.col("ref.dt_inicio"), F.col("best.doc_dt_inicio")).alias("dt_inicio"),
        F.coalesce(F.col("ref.dt_fim"), F.col("best.doc_dt_fim")).alias("dt_fim"),
        F.coalesce(F.col("ref.valor_mensal"), F.col("best.doc_valor_mensal")).alias("valor_mensal"),
        F.col("ref.status").alias("status"),
        F.col("ref.arquivo_origem_referencia").alias("arquivo_origem_referencia"),
        F.col("best.arquivo_origem_txt").alias("arquivo_origem_txt"),
        F.col("best.arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
        F.coalesce(F.col("best.fl_arquivo_pdf_existe"), F.lit(False)).alias("fl_arquivo_pdf_existe"),
        F.col("best.texto_bruto_contrato").alias("texto_bruto_contrato"),
        F.col("best.tp_clausula_exclusividade").alias("tp_clausula_exclusividade"),
        F.col("best.fl_clausula_exclusividade").alias("fl_clausula_exclusividade"),
        F.col("best.fl_clausula_nao_concorrencia").alias("fl_clausula_nao_concorrencia"),
        F.col("best.trecho_clausula_exclusividade").alias("trecho_clausula_exclusividade"),
        F.col("best.criterio_extracao_clausula").alias("criterio_extracao_clausula"),
        F.col("best.score_extracao_clausula").alias("score_extracao_clausula"),
        F.when(F.col("best.arquivo_origem_txt").isNotNull(), F.col("best.criterio_match_documento")).otherwise(F.lit("SEM_DOCUMENTO")).alias("criterio_match_documento"),
    )
)

matched_doc_keys = (
    df_best_doc_match
    .select("doc_row_id", "arquivo_origem_txt")
    .dropDuplicates(["doc_row_id"])
)

df_doc_only = (
    df_docs_match.alias("doc")
    .join(matched_doc_keys.alias("md"), F.col("doc.doc_row_id") == F.col("md.doc_row_id"), "left_anti")
    .select(
        F.lit("DOC_ONLY").alias("fonte_referencia"),
        F.col("doc.tp_vinculo").alias("tp_vinculo"),
        F.col("doc.empresa_prestador").alias("empresa_prestador"),
        F.col("doc.cnpj_prestador").alias("cnpj_prestador"),
        F.col("doc.consultoria").alias("consultoria"),
        F.col("doc.cnpj_consultoria").alias("cnpj_consultoria"),
        F.col("doc.empresa_cliente").alias("empresa_cliente"),
        F.col("doc.cargo").alias("cargo"),
        F.col("doc.dt_inicio").alias("dt_inicio"),
        F.col("doc.dt_fim").alias("dt_fim"),
        F.col("doc.valor_mensal").alias("valor_mensal"),
        F.lit(None).cast("string").alias("status"),
        F.lit(None).cast("string").alias("arquivo_origem_referencia"),
        F.col("doc.arquivo_origem_txt").alias("arquivo_origem_txt"),
        F.col("doc.arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
        F.coalesce(F.col("doc.fl_arquivo_pdf_existe"), F.lit(False)).alias("fl_arquivo_pdf_existe"),
        F.col("doc.texto_bruto_contrato").alias("texto_bruto_contrato"),
        F.col("doc.tp_clausula_exclusividade").alias("tp_clausula_exclusividade"),
        F.col("doc.fl_clausula_exclusividade").alias("fl_clausula_exclusividade"),
        F.col("doc.fl_clausula_nao_concorrencia").alias("fl_clausula_nao_concorrencia"),
        F.col("doc.trecho_clausula_exclusividade").alias("trecho_clausula_exclusividade"),
        F.col("doc.criterio_extracao_clausula").alias("criterio_extracao_clausula"),
        F.col("doc.score_extracao_clausula").alias("score_extracao_clausula"),
        F.col("doc.criterio_match_documento").alias("criterio_match_documento"),
    )
)

df_final = (
    df_joined
    .unionByName(df_doc_only)
    .withColumn("ingested_at", F.current_timestamp())
)

try:
    registered_aliases = register_company_alias_candidates_df(
        df_final,
        [
            ("empresa_prestador", "cnpj_prestador", "PROPRIA"),
            ("consultoria", "cnpj_consultoria", "CONSULTORIA"),
            ("empresa_cliente", None, "CLIENTE"),
        ],
        source_system="raw_contratos",
    )
    print(f"[OK] raw_contratos alias candidates registrados: {registered_aliases}")
except Exception as e:
    print(f"[WARN] raw_contratos alias candidate capture falhou e nao bloqueou a carga: {e}")

write_overwrite(df_final, BRONZE_RAW_CONTRATOS, catalog_schema=f"{CATALOG}.{BRONZE}")
