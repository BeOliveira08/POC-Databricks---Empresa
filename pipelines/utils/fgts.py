# Databricks notebook source
# fgts
#
# Shared helpers for FGTS parsing (PDF and JSONL sources).
# Usage: %run ../../utils/fgts

# COMMAND ----------
# MAGIC %run ./text

# COMMAND ----------

import re

EXPECTED_FGTS_COLS = [
    ("id_empresa",       "BIGINT"),
    ("entity_key",       "STRING"),
    ("nu_cpf",           "STRING"),
    ("nu_cnpj",          "STRING"),
    ("nome_normalizado", "STRING"),
    ("arquivo",          "STRING"),
    ("empresa_raw",      "STRING"),
    ("empresa_padrao",   "STRING"),
    ("empresa_grupo",    "STRING"),
    ("empresa_match",    "BOOLEAN"),
    ("emitido_em",       "STRING"),
    ("pis_pasep",        "STRING"),
    ("cnpj_empregador",  "STRING"),
    ("conta_fgts",       "STRING"),
    ("dt_lancamento",    "DATE"),
    ("competencia",      "DATE"),
    ("descricao",        "STRING"),
    ("event_type",       "STRING"),
    ("valor",            "DECIMAL(18,2)"),
    ("saldo",            "DECIMAL(18,2)"),
    ("ingestion_ts",     "TIMESTAMP"),
    ("pipeline_run_id",  "STRING"),
    ("ingested_at",      "TIMESTAMP"),
]


def ensure_bronze_fgts_table(bronze_fgts_table: str) -> None:
    ddl = ",\n  ".join([f"{c} {t}" for c, t in EXPECTED_FGTS_COLS])
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze_fgts_table} (
      {ddl}
    ) USING DELTA
    """)
    current_cols = {f.name for f in spark.table(bronze_fgts_table).schema.fields}
    for col_name, col_type in EXPECTED_FGTS_COLS:
        if col_name not in current_cols:
            spark.sql(f"ALTER TABLE {bronze_fgts_table} ADD COLUMN {col_name} {col_type}")
            print(f"[OK] Coluna {col_name} adicionada em {bronze_fgts_table}")


DATE_START_RE = re.compile(r"^\d{2}/\d{2}/\d{4}\b")
MONEY_RE = re.compile(r"(-?\s*(?:R?\$?\s*)?\d{1,3}(?:\.\d{3})*,\d{2})")
EMITIDO_RE = re.compile(
    r"Hist[oó]rico emitido em:\s*([0-9]{2}/[0-9]{2}/[0-9]{4}\s*-\s*[0-9]{2}:[0-9]{2})",
    re.IGNORECASE
)
PIS_RE = re.compile(r"\b(\d{3}\.\d{5}\.\d{2}-\d)\b")
CPF_RE = re.compile(r"\b(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11})\b")
CPF_LABEL_RE = re.compile(r"CPF\s*[:\-]?\s*(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11})", re.IGNORECASE)
CPF_NEAR_HEADER_RE = re.compile(
    r"(?:NOME|TRABALHADOR|PIS/PASEP|PIS|PASEP|INSCRICAO)[\s\S]{0,160}?(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11})",
    re.IGNORECASE,
)
INSCRICAO_EMPREGADOR_RE = re.compile(r"INSCRIÇÃO\s+DO\s+EMPREGADOR\s*[<\s]*(\d{13,14})", re.IGNORECASE)
CNPJ_13_14_RE = re.compile(r"\b(\d{13,14})\b")
CONTA_RE = re.compile(r"\b(\d{13,}/\d{3,})\b")


def classify_event(desc: str) -> str:
    d = (desc or "").upper()
    if "MULTA RESCIS" in d:
        return "multa_rescisoria"
    if "CREDITO DE JAM" in d or "CRÉDITO DE JAM" in d:
        return "rendimento_jam"
    if "PLR" in d or "LUCRO" in d or "PARTICIP" in d:
        return "plr"
    if "SAQUE" in d:
        return "saque"
    if "DEP RESCIS" in d or "DEPÓSITO RESCIS" in d:
        return "deposito_rescisorio"
    if "DEPOSITO" in d or "DEPÓSITO" in d:
        return "deposito"
    if "SALDO ANTERIOR" in d:
        return "saldo_anterior"
    return "outros"


def clean_desc(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def get_empresa_from_arquivo(arquivo: str) -> str:
    if not arquivo:
        return None
    s = re.sub(r"(?i)^extrato_", "", arquivo)
    s = re.sub(r"\.(pdf|jsonl)$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*\(\d+\)$", "", s)
    return clean_desc(s.replace("_", " ").upper()) or None


def parse_header(text: str):
    emitido_em = cpf = pis = cnpj = conta = None

    m = EMITIDO_RE.search(text or "")
    if m:
        emitido_em = clean_desc(m.group(1))

    m = PIS_RE.search(text or "")
    if m:
        pis = clean_desc(m.group(1))

    m = CPF_LABEL_RE.search(text or "")
    if m:
        cpf = clean_cpf(m.group(1))
    else:
        m = CPF_NEAR_HEADER_RE.search(text or "")
        if m:
            cpf = clean_cpf(m.group(1))

    if not cpf:
        m = CPF_RE.search(text or "")
        if m:
            cpf = clean_cpf(m.group(1))

    m = INSCRICAO_EMPREGADOR_RE.search(text or "")
    if m:
        cnpj = clean_cnpj(m.group(1))
    else:
        m = CNPJ_13_14_RE.search(text or "")
        if m:
            cnpj = clean_cnpj(m.group(1))

    m = CONTA_RE.search(text or "")
    if m:
        conta = clean_desc(m.group(1))

    return emitido_em, cpf, pis, cnpj, conta


def parse_movements(text: str):
    lines = [ln.rstrip() for ln in (text or "").splitlines() if ln.strip()]
    blocks, curr = [], []

    for ln in lines:
        if DATE_START_RE.match(ln.strip()):
            if curr:
                blocks.append(" ".join(curr))
                curr = []
        curr.append(ln.strip())
    if curr:
        blocks.append(" ".join(curr))

    out = []
    for blk in blocks:
        b = clean_desc(blk)

        if b.upper().startswith("SALDO ANTERIOR"):
            nums = MONEY_RE.findall(b)
            out.append((None, "SALDO ANTERIOR", nums[-2] if len(nums) >= 2 else None, nums[-1] if nums else None))
            continue

        if not DATE_START_RE.match(b):
            continue

        dt = b[:10]
        monies = MONEY_RE.findall(b)
        if len(monies) < 2:
            continue

        money_iter = list(MONEY_RE.finditer(b))
        cut_pos = money_iter[-2].start()
        desc = clean_desc(b[10:cut_pos].strip())
        out.append((dt, desc, monies[-2], monies[-1]))

    return out
