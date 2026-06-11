# Databricks notebook source
# bronze: raw_ctps

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

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

BRONZE_RAW_CTPS = f"{CATALOG}.{BRONZE}.raw_ctps"
TITULAR_CPF = "00000000000"
TITULAR_NOME_COMPLETO = "TITULAR GENERICO"
# Mantido aqui como ancora documental para futuras correcoes, embora ainda nao
# exista coluna dedicada na bronze raw_ctps.
TITULAR_DATA_NASCIMENTO = "1990-10-04"

CPF_PATTERN = re.compile(r"CPF(?:\s*[:\-])?(?:\s|\n)+([0-9\.\-]{11,14})", re.IGNORECASE)
NOME_PATTERN = re.compile(r"Nome civil(?:\s|\n)+CPF(?:\s|\n)+([^\n]+)", re.IGNORECASE)
CONTRATO_SPLIT_PATTERN = re.compile(
    r"\n(?=\d{2}/\d{2}/\d{4}\s*-\s*(?:\d{2}/\d{2}/\d{4}|\(atual\)|Aberto)\s*\n)",
    re.IGNORECASE,
)
DATA_CABECALHO_PATTERN = re.compile(
    r"(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4}|\(atual\)|Aberto)",
    re.IGNORECASE,
)
EMPREGADOR_PATTERN = re.compile(r"Empregador\s+([^\n]+?)(?:\s+CNPJ|$)", re.IGNORECASE)
CNPJ_PATTERN = re.compile(r"CNPJ(?:\s*[:\-])?\s*([0-9\./\-]{14,18})", re.IGNORECASE)
CARGO_PATTERN = re.compile(r"Cargo\s+([^\n]+?)(?:\s+CBO|$)", re.IGNORECASE)
CBO_PATTERN = re.compile(r"CBO\s+Cargo\s+([0-9-]+)", re.IGNORECASE)
SALARIO_PATTERN = re.compile(r"Sal[aÃ¡]rio contratual\s+([^\n]+)", re.IGNORECASE)


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
        for enc in ["utf-8", "utf-8-sig", "cp1252", "latin-1"]:
            try:
                df = (
                    spark.read
                    .option("header", "true")
                    .option("sep", sep)
                    .option("encoding", enc)
                    .option("quote", '"')
                    .option("escape", '"')
                    .csv(path)
                )
                cols = [c.replace("\ufeff", "").strip() for c in df.columns]
                if len(cols) > best_width:
                    best_df = df.toDF(*cols)
                    best_width = len(cols)
                if len(cols) > 1:
                    return df.toDF(*cols)
            except Exception:
                pass
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


def extract_header_identity(texto: str) -> tuple[str | None, str | None]:
    lines = [clean_text(line) for line in texto.splitlines()]
    lines = [line for line in lines if line]

    for idx, line in enumerate(lines):
        if line.upper() != "NOME CIVIL":
            continue
        if idx + 3 < len(lines) and lines[idx + 1].upper() == "CPF":
            nome = clean_text(lines[idx + 2])
            cpf = clean_text(lines[idx + 3])
            if nome or cpf:
                return nome, cpf
        if idx + 1 < len(lines):
            nome = clean_text(lines[idx + 1])
            cpf = clean_text(lines[idx + 2]) if idx + 2 < len(lines) else None
            if nome or cpf:
                return nome, cpf

    cpf_m = CPF_PATTERN.search(texto or "")
    nome_m = NOME_PATTERN.search(texto or "")
    return (
        clean_text(nome_m.group(1)) if nome_m else None,
        clean_text(cpf_m.group(1)) if cpf_m else None,
    )


def _normalized_document_name(arquivo: str) -> str:
    base = re.sub(r"\.[^.]+$", "", arquivo)
    base = _ascii(base).upper()
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base


def parse_ctps_text(texto: str, arquivo: str) -> list[dict]:
    nome_raw, cpf_raw = extract_header_identity(texto or "")
    blocos = CONTRATO_SPLIT_PATTERN.split(texto or "")
    if blocos and "Empregador" not in blocos[0]:
        blocos = blocos[1:]

    rows = []
    for bloco in blocos:
        bloco = bloco.strip()
        if not bloco:
            continue

        data_m = DATA_CABECALHO_PATTERN.search(bloco[:200])
        emp_m = EMPREGADOR_PATTERN.search(bloco)
        if not data_m or not emp_m:
            continue

        cnpj_m = CNPJ_PATTERN.search(bloco)
        cargo_m = CARGO_PATTERN.search(bloco)
        cbo_m = CBO_PATTERN.search(bloco)
        salario_m = SALARIO_PATTERN.search(bloco)

        dt_inicio_raw = clean_text(data_m.group(1))
        dt_fim_raw = clean_text(data_m.group(2))
        if dt_fim_raw and dt_fim_raw.lower() in {"aberto", "(atual)"}:
            dt_fim_raw = None

        cnpj_limpo = clean_cnpj(clean_text(cnpj_m.group(1)) if cnpj_m else None)
        rows.append(
            {
                "fonte_referencia": "DOCUMENTO",
                "tp_vinculo": "CLT",
                "nu_cpf": clean_cpf(cpf_raw),
                "nome": clean_text(nome_raw),
                "nu_cnpj_empresa": cnpj_limpo,
                "nome_empresa": canonical_company_name(clean_text(emp_m.group(1)), cnpj_limpo),
                "cargo": clean_text(cargo_m.group(1)) if cargo_m else None,
                "cbo": clean_text(cbo_m.group(1)) if cbo_m else None,
                "dt_inicio": dt_inicio_raw,
                "dt_fim": dt_fim_raw,
                "vl_salario": clean_text(salario_m.group(1)) if salario_m else None,
                "status": None,
                "arquivo_origem_referencia": None,
                "arquivo_origem_txt": arquivo,
                "texto_bruto_documento": bloco,
            }
        )
    return rows


# COMMAND ----------

def read_planilha_ctps():
    paths = list_files_by_extension(EMPRESAS_CLT_DIR, [".csv"])
    dfs = []
    for path in paths:
        df_raw = _read_csv_local(path)
        cols = df_raw.columns
        cnpj_col = _pick_col(cols, "CNPJ")
        cnpj_expr = normalize_cnpj_expr(only_digits_expr(cnpj_col)) if cnpj_col else F.lit(None).cast("string")
        dfs.append(
            df_raw.select(
                F.lit("PLANILHA").alias("fonte_referencia"),
                F.lit("CLT").alias("tp_vinculo"),
                F.lit(TITULAR_CPF).cast("string").alias("nu_cpf"),
                F.lit(TITULAR_NOME_COMPLETO).cast("string").alias("nome"),
                cnpj_expr.alias("nu_cnpj_empresa"),
                canonical_company_udf(_col_or_null(cols, "Empresa"), cnpj_expr).alias("nome_empresa"),
                _col_or_null(cols, "Cargo").alias("cargo"),
                F.lit(None).cast("string").alias("cbo"),
                _col_or_null(cols, "Data Inicio", "Data Início").alias("dt_inicio"),
                _col_or_null(cols, "Data Fim").alias("dt_fim"),
                _col_or_null(cols, "Salario", "Salário").alias("vl_salario"),
                _col_or_null(cols, "Status").alias("status"),
                F.lit(os.path.basename(path)).alias("arquivo_origem_referencia"),
            )
        )
    if not dfs:
        return spark.createDataFrame([], schema="""
            fonte_referencia string, tp_vinculo string, nu_cpf string, nome string,
            nu_cnpj_empresa string, nome_empresa string, cargo string, cbo string,
            dt_inicio string, dt_fim string, vl_salario string, status string,
            arquivo_origem_referencia string
        """)
    df = dfs[0]
    for nxt in dfs[1:]:
        df = df.unionByName(nxt)
    return df


def read_document_ctps():
    txt_paths = list_files_by_extension_recursive(CONTRATOS_CLT_DIR, [".txt"])
    pdf_paths = list_files_by_extension_recursive(CONTRATOS_CLT_DIR, [".pdf"])
    if not txt_paths:
        txt_paths = list_files_by_extension_recursive(CTPS_DIR, [".txt"])
    if not pdf_paths:
        pdf_paths = list_files_by_extension_recursive(CTPS_DIR, [".pdf"])

    pdf_by_key = {_normalized_document_name(os.path.basename(p)): os.path.basename(p) for p in pdf_paths}
    rows = []
    for txt_path in sorted(set(txt_paths)):
        arquivo = os.path.basename(txt_path)
        key = _normalized_document_name(arquivo)
        with open(txt_path, "r", encoding="utf-8", errors="ignore") as handle:
            texto = handle.read()
        parsed_rows = parse_ctps_text(texto, arquivo)
        for row in parsed_rows:
            row["arquivo_pdf_validacao"] = pdf_by_key.get(key)
            row["fl_arquivo_pdf_existe"] = pdf_by_key.get(key) is not None
            row["criterio_match_documento"] = "TXT_CTPS"
            rows.append(row)

    schema = StructType(
        [
            StructField("fonte_referencia", StringType(), True),
            StructField("tp_vinculo", StringType(), True),
            StructField("nu_cpf", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("nu_cnpj_empresa", StringType(), True),
            StructField("nome_empresa", StringType(), True),
            StructField("cargo", StringType(), True),
            StructField("cbo", StringType(), True),
            StructField("dt_inicio", StringType(), True),
            StructField("dt_fim", StringType(), True),
            StructField("vl_salario", StringType(), True),
            StructField("status", StringType(), True),
            StructField("arquivo_origem_referencia", StringType(), True),
            StructField("arquivo_origem_txt", StringType(), True),
            StructField("arquivo_pdf_validacao", StringType(), True),
            StructField("fl_arquivo_pdf_existe", BooleanType(), True),
            StructField("texto_bruto_documento", StringType(), True),
            StructField("criterio_match_documento", StringType(), True),
        ]
    )
    if not rows:
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(rows, schema=schema)


# COMMAND ----------

df_ref = read_planilha_ctps().withColumn(
    "ref_key",
    F.regexp_replace(F.upper(F.coalesce(F.col("nu_cnpj_empresa"), F.col("nome_empresa"), F.lit(""))), r"[^A-Z0-9]", ""),
)
df_doc = read_document_ctps().withColumn(
    "doc_key",
    F.regexp_replace(F.upper(F.coalesce(F.col("nu_cnpj_empresa"), F.col("nome_empresa"), F.lit(""))), r"[^A-Z0-9]", ""),
)

join_cond = (
    (
        F.col("ref.nu_cnpj_empresa").isNotNull()
        & F.col("doc.nu_cnpj_empresa").isNotNull()
        & (F.col("ref.nu_cnpj_empresa") == F.col("doc.nu_cnpj_empresa"))
    )
    | (
        F.col("ref.nu_cnpj_empresa").isNull()
        & F.col("doc.nu_cnpj_empresa").isNull()
        & (F.col("ref.ref_key") != "")
        & (F.col("ref.ref_key") == F.col("doc.doc_key"))
    )
)

w = Window.partitionBy(
    "ref.nome_empresa", "ref.nu_cnpj_empresa", "ref.cargo", "ref.dt_inicio", "ref.dt_fim"
).orderBy(
    F.when(F.col("doc.nu_cpf").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("doc.arquivo_origem_txt").desc_nulls_last(),
)

df_joined = (
    df_ref.alias("ref")
    .join(df_doc.alias("doc"), join_cond, "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        F.col("ref.fonte_referencia").alias("fonte_referencia"),
        F.lit("CLT").alias("tp_vinculo"),
        F.col("doc.nu_cpf").alias("nu_cpf"),
        F.col("doc.nome").alias("nome"),
        F.col("ref.nu_cnpj_empresa").alias("nu_cnpj_empresa"),
        F.col("ref.nome_empresa").alias("nome_empresa"),
        F.coalesce(F.col("ref.cargo"), F.col("doc.cargo")).alias("cargo"),
        F.col("doc.cbo").alias("cbo"),
        F.coalesce(F.col("ref.dt_inicio"), F.col("doc.dt_inicio")).alias("dt_inicio"),
        F.coalesce(F.col("ref.dt_fim"), F.col("doc.dt_fim")).alias("dt_fim"),
        F.coalesce(F.col("ref.vl_salario"), F.col("doc.vl_salario")).alias("vl_salario"),
        F.col("ref.status").alias("status"),
        F.col("ref.arquivo_origem_referencia").alias("arquivo_origem_referencia"),
        F.col("doc.arquivo_origem_txt").alias("arquivo_origem_txt"),
        F.col("doc.arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
        F.coalesce(F.col("doc.fl_arquivo_pdf_existe"), F.lit(False)).alias("fl_arquivo_pdf_existe"),
        F.col("doc.texto_bruto_documento").alias("texto_bruto_documento"),
        F.when(F.col("doc.arquivo_origem_txt").isNotNull(), F.col("doc.criterio_match_documento")).otherwise(F.lit("SEM_DOCUMENTO")).alias("criterio_match_documento"),
    )
)

matched_docs = (
    df_joined
    .filter(F.col("arquivo_origem_txt").isNotNull())
    .select("arquivo_origem_txt")
    .dropDuplicates(["arquivo_origem_txt"])
)

df_doc_only = (
    df_doc.alias("doc")
    .join(matched_docs.alias("m"), F.col("doc.arquivo_origem_txt") == F.col("m.arquivo_origem_txt"), "left_anti")
    .select(
        F.lit("DOC_ONLY").alias("fonte_referencia"),
        F.lit("CLT").alias("tp_vinculo"),
        F.col("doc.nu_cpf").alias("nu_cpf"),
        F.col("doc.nome").alias("nome"),
        F.col("doc.nu_cnpj_empresa").alias("nu_cnpj_empresa"),
        F.col("doc.nome_empresa").alias("nome_empresa"),
        F.col("doc.cargo").alias("cargo"),
        F.col("doc.cbo").alias("cbo"),
        F.col("doc.dt_inicio").alias("dt_inicio"),
        F.col("doc.dt_fim").alias("dt_fim"),
        F.col("doc.vl_salario").alias("vl_salario"),
        F.lit(None).cast("string").alias("status"),
        F.lit(None).cast("string").alias("arquivo_origem_referencia"),
        F.col("doc.arquivo_origem_txt").alias("arquivo_origem_txt"),
        F.col("doc.arquivo_pdf_validacao").alias("arquivo_pdf_validacao"),
        F.coalesce(F.col("doc.fl_arquivo_pdf_existe"), F.lit(False)).alias("fl_arquivo_pdf_existe"),
        F.col("doc.texto_bruto_documento").alias("texto_bruto_documento"),
        F.col("doc.criterio_match_documento").alias("criterio_match_documento"),
    )
)

df_final = (
    df_joined
    .unionByName(df_doc_only)
    .withColumn(
        "nu_cpf",
        F.when(
            F.upper(F.coalesce(F.col("tp_vinculo"), F.lit(""))) == F.lit("CLT"),
            F.coalesce(F.col("nu_cpf"), F.lit(TITULAR_CPF)),
        ).otherwise(F.col("nu_cpf"))
    )
    .withColumn(
        "nome",
        F.when(
            F.upper(F.coalesce(F.col("tp_vinculo"), F.lit(""))) == F.lit("CLT"),
            F.coalesce(F.col("nome"), F.lit(TITULAR_NOME_COMPLETO)),
        ).otherwise(F.col("nome"))
    )
    .withColumn("ingested_at", F.current_timestamp())
)

try:
    registered_aliases = register_company_alias_candidates_df(
        df_final,
        [
            ("nome_empresa", "nu_cnpj_empresa", "EMPREGADORA"),
        ],
        source_system="raw_ctps",
    )
    print(f"[OK] raw_ctps alias candidates registrados: {registered_aliases}")
except Exception as e:
    print(f"[WARN] raw_ctps alias candidate capture falhou e nao bloqueou a carga: {e}")

write_overwrite(df_final, BRONZE_RAW_CTPS, catalog_schema=f"{CATALOG}.{BRONZE}")
