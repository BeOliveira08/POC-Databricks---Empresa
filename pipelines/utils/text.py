# Databricks notebook source
# text
#
# Pure Python text cleaning and parsing helpers.
# Usage: %run ../../utils/text
#
# These functions operate on plain Python values (str, None).
# Use for UDF inputs, PDF parsing, and row-level processing outside Spark.

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from datetime import datetime


def clean_text(s: str | None) -> str | None:
    """Strip and collapse whitespace. Returns None for empty/null."""
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None


def ascii_fold_text(value: str | None) -> str:
    """Return text without accents/combining chars for fuzzy matching."""
    text = value or ""
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def clean_cnpj(valor: str | None) -> str | None:
    """Return CNPJ as exactly 14 digits, or None if invalid.

    Handles float-formatted CNPJs (e.g. "1.39538E+13") and zero-padded inputs.
    """
    if valor is None:
        return None
    raw = str(valor).strip()
    if not raw:
        return None

    if re.search(r"[eE][+-]?\d+", raw):
        try:
            raw = f"{Decimal(raw):.0f}"
        except (InvalidOperation, ValueError):
            pass
    elif re.fullmatch(r"\d+\.0+", raw):
        raw = raw.split(".")[0]

    d = re.sub(r"\D", "", raw)
    if len(d) == 13:
        d = "0" + d
    return d if len(d) == 14 else None


def clean_cpf(valor: str | None) -> str | None:
    """Return CPF as exactly 11 digits, or None if invalid."""
    if valor is None:
        return None
    raw = str(valor).strip()
    if not raw:
        return None

    if re.search(r"[eE][+-]?\d+", raw):
        try:
            raw = f"{Decimal(raw):.0f}"
        except (InvalidOperation, ValueError):
            pass
    elif re.fullmatch(r"\d+\.0+", raw):
        raw = raw.split(".")[0]

    d = re.sub(r"\D", "", raw)
    return d if len(d) == 11 else None


def only_digits_str(valor: str | None) -> str | None:
    """Strip all non-digit characters from a Python string. Returns None for empty."""
    if not valor:
        return None
    d = re.sub(r"\D", "", str(valor))
    return d or None


def br_money_to_decimal(s: str | None) -> Decimal | None:
    """Parse a Brazilian-formatted currency string to Decimal.

    Handles formats: "1.234,56", "1234,56", "-1.234,56", "1234.56",
    and mixed-separator variants such as "13,500.00" / "13,500,00".
    Returns None for empty, null, or unparseable values.
    """
    if s is None:
        return None
    s = str(s).strip()
    if not s or s in ("-", ""):
        return None
    neg = s.startswith("-")
    if neg:
        s = s[1:].strip()
    s = re.sub(r"[^0-9\.,]", "", s)
    if not s:
        return None

    comma_count = s.count(",")
    dot_count = s.count(".")

    if comma_count and dot_count:
        # Use the rightmost separator as decimal and strip the others.
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            int_part = s[:last_comma].replace(".", "").replace(",", "")
            frac_part = s[last_comma + 1 :].replace(".", "").replace(",", "")
        else:
            int_part = s[:last_dot].replace(".", "").replace(",", "")
            frac_part = s[last_dot + 1 :].replace(".", "").replace(",", "")
        s = f"{int_part}.{frac_part}" if frac_part else int_part
    elif comma_count:
        if comma_count > 1:
            last_comma = s.rfind(",")
            frac_part = s[last_comma + 1 :]
            if len(frac_part) in {1, 2}:
                int_part = s[:last_comma].replace(",", "")
                s = f"{int_part}.{frac_part}"
            else:
                s = s.replace(",", "")
        else:
            int_part, frac_part = s.split(",", 1)
            if len(frac_part) in {1, 2}:
                s = f"{int_part}.{frac_part}"
            elif len(frac_part) == 3:
                s = f"{int_part}{frac_part}"
            else:
                s = f"{int_part}.{frac_part}"
    elif dot_count:
        if dot_count > 1:
            last_dot = s.rfind(".")
            frac_part = s[last_dot + 1 :]
            if len(frac_part) in {1, 2}:
                int_part = s[:last_dot].replace(".", "")
                s = f"{int_part}.{frac_part}"
            else:
                s = s.replace(".", "")
        else:
            int_part, frac_part = s.split(".", 1)
            if len(frac_part) in {1, 2}:
                s = f"{int_part}.{frac_part}"
            elif len(frac_part) == 3:
                s = f"{int_part}{frac_part}"
            else:
                s = f"{int_part}.{frac_part}"

    if neg:
        s = f"-{s}"
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def br_money_to_float(s: str | None) -> float | None:
    """Parse a Brazilian-formatted currency string to float.

    Convenience wrapper around br_money_to_decimal for contexts that need float.
    """
    result = br_money_to_decimal(s)
    return float(result) if result is not None else None


def parse_flexible_date_to_iso(valor: str | None) -> str | None:
    """Parse mixed BR date formats and return ISO yyyy-mm-dd.

    Supported examples:
    - 25/05/2023
    - 2023-05-25
    - 05/2023 -> 2023-05-01
    - 2023-05 -> 2023-05-01
    - abr. 01, 2025 -> 2025-04-01
    - abr/2025 -> 2025-04-01
    """
    if valor is None:
        return None

    raw = clean_text(str(valor))
    if not raw:
        return None

    upper = raw.upper()
    if upper in {"NULL", "-", "N/A", "NA", "NAO INFORMADO", "NÃO INFORMADO"}:
        return None

    # Direct numeric formats
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Partial numeric month/year formats -> first day of month
    for fmt in ("%m/%Y", "%Y-%m", "%m-%Y", "%Y/%m"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.replace(day=1).strftime("%Y-%m-%d")
        except ValueError:
            pass

    month_map = {
        "JAN": 1,
        "FEV": 2,
        "FEB": 2,
        "MAR": 3,
        "ABR": 4,
        "APR": 4,
        "MAI": 5,
        "MAY": 5,
        "JUN": 6,
        "JUL": 7,
        "AGO": 8,
        "AUG": 8,
        "SET": 9,
        "SEP": 9,
        "OUT": 10,
        "OCT": 10,
        "NOV": 11,
        "DEZ": 12,
        "DEC": 12,
    }

    normalized = raw.upper()
    normalized = normalized.replace(",", " ")
    normalized = re.sub(r"\.", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    # MON DD YYYY
    match = re.fullmatch(r"([A-Z]{3})\s+(\d{1,2})\s+(\d{4})", normalized)
    if match:
        mon, day, year = match.groups()
        month = month_map.get(mon)
        if month:
            try:
                return datetime(int(year), month, int(day)).strftime("%Y-%m-%d")
            except ValueError:
                return None

    # MON YYYY -> first day
    match = re.fullmatch(r"([A-Z]{3})[/\s-]+(\d{4})", normalized)
    if match:
        mon, year = match.groups()
        month = month_map.get(mon)
        if month:
            return f"{int(year):04d}-{month:02d}-01"

    return None


def normalize_contract_text(value: str | None) -> str:
    """Normalize contract text for clause-level matching."""
    text = ascii_fold_text(value)
    text = text.replace("\ufeff", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def split_contract_clauses(text: str | None) -> list[str]:
    """Split a contract into likely clause-sized chunks."""
    normalized = normalize_contract_text(text)
    if not normalized:
        return []

    marked = re.sub(
        r"(?i)(?=(?:clausula|clausula\s+\w+|paragrafo|paragrafo\s+\w+|item)\s*(?:\d+|[ivxlcdm]+|[a-z])?\s*[\.\-:]?)",
        "\n@@CLAUSE@@ ",
        normalized,
    )
    parts = [clean_text(part) for part in marked.split("@@CLAUSE@@")]
    parts = [part for part in parts if part]
    if len(parts) > 1:
        return parts

    sentence_parts = re.split(r"(?<=[.;:])\s+(?=[A-Z0-9])", normalized)
    return [part for part in (clean_text(part) for part in sentence_parts) if part]


def extract_contract_clause_evidence(text: str | None) -> dict:
    """Extract the best exclusivity/non-compete evidence from raw contract text."""
    normalized = normalize_contract_text(text)
    if not normalized:
        return {
            "tipo": None,
            "fl_exclusividade": None,
            "fl_nao_concorrencia": 0,
            "trecho": None,
            "criterio": "SEM_TEXTO",
            "score": 0,
        }

    clauses = split_contract_clauses(normalized)
    if not clauses:
        clauses = [normalized]

    keyword_pattern = re.compile(
        r"(?i)\b("
        r"exclusiv\w+|nao\s+exclusiv\w+|sem\s+exclusiv\w+|"
        r"nao\s+concorr\w+|nao\s+compet\w+|clientes?\s+da\s+contratante|"
        r"preferencia|dedicacao\s+exclusiva|dedicacao\s+integral"
        r")\b"
    )

    rules = [
        (
            "NAO_EXCLUSIVO",
            120,
            "CLAUSULA_LITERAL_NAO_EXCLUSIVA",
            [
                r"\bsem\s+exclusividade\b",
                r"\bnao\s+(?:ha|havera|existira)\s+exclusividade\b",
                r"\bsem\s+qualquer\s+exclusividade\b",
                r"\bnao\s+confere.*exclusividade\b",
                r"\bpodendo\s+a\s+contratada\s+prestar\s+servicos\s+a\s+outras\b",
                r"\bfacultad[ao]\s+a\s+contratada\s+firmar\s+quaisquer\s+outros\s+contratos\b",
                r"\bcom\s+ou\s+sem\s+exclusividade\b",
            ],
        ),
        (
            "EXCLUSIVO",
            110,
            "CLAUSULA_LITERAL_EXCLUSIVA",
            [
                r"\btotal\s+exclusividade\b",
                r"\bcarater\s+exclusivo\b",
                r"\bdedicacao\s+exclusiva\b",
                r"\bdedicacao\s+integral\b",
                r"\bnao\s+prestara\s+quaisquer\s+servicos\s+a\s+qualquer\s+terceiro\b",
                r"\bmanter\s+(?:a\s+)?exclusividade\b",
                r"\bassegurar\s+a\s+total\s+exclusividade\b",
            ],
        ),
        (
            "NAO_CONCORRENCIA",
            100,
            "CLAUSULA_LITERAL_NAO_CONCORRENCIA",
            [
                r"\bnao\s+concorr\w+\b",
                r"\bnao\s+compet\w+\b",
                r"\bnao\s+prospectar\b",
                r"\bnao\s+oferecer\s+os\s+seus\s+servicos\b",
                r"\bnao\s+firmar\s+contrato.*clientes?\s+da\s+contratante\b",
                r"\bclientes?\s+da\s+contratante\b",
            ],
        ),
    ]

    best = {
        "tipo": None,
        "fl_exclusividade": None,
        "fl_nao_concorrencia": 0,
        "trecho": None,
        "criterio": "SEM_MATCH",
        "score": 0,
    }

    for clause in clauses:
        clause_norm = clean_text(clause) or ""
        if not clause_norm:
            continue

        matched = False
        for tipo, score, criterio, patterns in rules:
            if any(re.search(pattern, clause_norm, re.IGNORECASE) for pattern in patterns):
                candidate = {
                    "tipo": tipo,
                    "fl_exclusividade": 1 if tipo == "EXCLUSIVO" else 0 if tipo == "NAO_EXCLUSIVO" else None,
                    "fl_nao_concorrencia": 1 if tipo == "NAO_CONCORRENCIA" else 0,
                    "trecho": clause_norm[:1200],
                    "criterio": criterio,
                    "score": score,
                }
                if candidate["score"] > best["score"]:
                    best = candidate
                matched = True

        if matched:
            continue

        if keyword_pattern.search(clause_norm):
            tipo = None
            fl_exclusividade = None
            fl_nao_concorrencia = 0
            clause_lower = clause_norm.lower()

            if re.search(r"\bnao\s+concorr|\bnao\s+compet|\bclientes?\s+da\s+contratante", clause_lower):
                tipo = "NAO_CONCORRENCIA"
                fl_nao_concorrencia = 1
            elif re.search(r"\bsem\s+exclusividade|\bnao\s+exclusiv", clause_lower):
                tipo = "NAO_EXCLUSIVO"
                fl_exclusividade = 0
            elif re.search(r"\bexclusiv", clause_lower):
                tipo = "EXCLUSIVO"
                fl_exclusividade = 1

            candidate = {
                "tipo": tipo,
                "fl_exclusividade": fl_exclusividade,
                "fl_nao_concorrencia": fl_nao_concorrencia,
                "trecho": clause_norm[:1200],
                "criterio": "MATCH_CONTEXTUAL_CLAUSULA",
                "score": 60 if tipo else 40,
            }
            if candidate["score"] > best["score"]:
                best = candidate

    if best["trecho"] is None:
        flat = re.sub(r"\s+", " ", normalized).strip()
        keyword_match = keyword_pattern.search(flat)
        if keyword_match:
            start = max(0, keyword_match.start() - 220)
            end = min(len(flat), keyword_match.end() + 320)
            best["trecho"] = flat[start:end]
            best["criterio"] = "MATCH_JANELA_TEXTO"
            best["score"] = max(best["score"], 25)

    return best


def extract_clause_tipo(text: str | None) -> str | None:
    return extract_contract_clause_evidence(text).get("tipo")


def extract_clause_flag_exclusividade(text: str | None) -> int | None:
    return extract_contract_clause_evidence(text).get("fl_exclusividade")


def extract_clause_flag_nao_concorrencia(text: str | None) -> int:
    return int(extract_contract_clause_evidence(text).get("fl_nao_concorrencia") or 0)


def extract_clause_snippet(text: str | None) -> str | None:
    return extract_contract_clause_evidence(text).get("trecho")


def extract_clause_match_strategy(text: str | None) -> str | None:
    return extract_contract_clause_evidence(text).get("criterio")


def extract_clause_match_score(text: str | None) -> int:
    return int(extract_contract_clause_evidence(text).get("score") or 0)


def fix_dates(dt_inicio_str: str | None, dt_fim_str: str | None) -> tuple:
    """Swap start/end dates if they are inverted. Returns the pair unchanged if either is None or unparseable."""
    if not dt_inicio_str or not dt_fim_str:
        return dt_inicio_str, dt_fim_str
    from datetime import datetime
    try:
        di = datetime.strptime(dt_inicio_str, "%d/%m/%Y")
        df = datetime.strptime(dt_fim_str, "%d/%m/%Y")
        if di <= df:
            return dt_inicio_str, dt_fim_str
        return dt_fim_str, dt_inicio_str
    except Exception:
        return dt_inicio_str, dt_fim_str
