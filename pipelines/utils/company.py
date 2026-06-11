# Databricks notebook source
# company
#
# Company normalization, alias resolution, key generation, and bronze.empresas merge helpers.
# Usage: %run ../../utils/company

# COMMAND ----------

import csv
import os
import re
import unicodedata
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %run ./text

# COMMAND ----------
# MAGIC %run ../config/domain_config

# COMMAND ----------


# ──────────────────────────────────────────────────────────────────────────────
# Normalização de texto
# ──────────────────────────────────────────────────────────────────────────────

def normalize_company_text(valor: str) -> str | None:
    """Remove acentos, padroniza sufixos legais e retorna string maiúscula."""
    if valor is None:
        return None
    s = str(valor).strip()
    if not s:
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = s.upper()
    s = re.sub(r"[&_]", " ", s)
    s = re.sub(r"[^A-Z0-9/.\s-]", " ", s)
    s = re.sub(r"\bS\.?A\.?\b",   " SA ",    s)
    s = re.sub(r"\bLTDA\.?\b",    " LTDA ",  s)
    s = re.sub(r"\bEIRELI\b",     " EIRELI ", s)
    s = re.sub(r"\bS/A\b",        " SA ",    s)
    s = re.sub(r"\bME\b",         " ",       s)
    s = re.sub(r"\bEPP\b",        " ",       s)
    s = re.sub(r"\s+",            " ",       s)
    return s.strip(" .-") or None


COMPANY_ALIAS_REGISTRY_COLUMNS = [
    "alias_name",
    "canonical_name",
    "grupo",
    "canonical_cnpj",
    "match_mode",
    "is_active",
    "source_system",
    "first_seen_at",
    "last_seen_at",
]

_COMPANY_ALIAS_REGISTRY_CACHE = None
_COMPANY_ALIAS_REGISTRY_MTIME = None


def company_alias_registry_path() -> str:
    configured = globals().get("EMPRESA_ALIASES_CONFIG_PATH")
    if configured:
        return configured

    empresas_dir = globals().get("EMPRESAS_DIR")
    if empresas_dir:
        return f"{empresas_dir}/empresa_aliases.csv"

    referencia_dir = globals().get("REFERENCIA_DIR", "/Volumes/portfolio/default/arquivos/referencia")
    return f"{referencia_dir}/empresas/empresa_aliases.csv"


def _normalize_match_mode(value: str | None) -> str:
    mode = (value or "EXACT").strip().upper()
    return mode if mode in {"EXACT", "CONTAINS", "REGEX"} else "EXACT"


def _truthy_csv_flag(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "sim", "s", "yes", "y"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _company_alias_registry_key(alias_name: str | None, canonical_cnpj: str | None = None) -> tuple[str, str]:
    return normalize_company_text(alias_name) or "", clean_cnpj(canonical_cnpj) or ""


def _empty_company_alias_registry_payload() -> dict:
    return {
        "exact_name_map": {},
        "contains_rules": [],
        "regex_rules": [],
        "cnpj_map": {},
    }


def _bootstrap_company_alias_registry_rows() -> list[dict]:
    now_iso = _utc_now_iso()
    rows = []

    for pattern, canonical_name, grupo in EMPRESA_ALIASES:
        rows.append(
            {
                "alias_name": pattern,
                "canonical_name": canonical_name,
                "grupo": grupo or "",
                "canonical_cnpj": "",
                "match_mode": "REGEX",
                "is_active": "1",
                "source_system": "bootstrap_hardcoded",
                "first_seen_at": now_iso,
                "last_seen_at": now_iso,
            }
        )

    for canonical_cnpj, canonical_name in CONSULTORIA_CLIENTE_MAP.items():
        rows.append(
            {
                "alias_name": canonical_name,
                "canonical_name": canonical_name,
                "grupo": "CONSULTORIA",
                "canonical_cnpj": canonical_cnpj,
                "match_mode": "EXACT",
                "is_active": "1",
                "source_system": "bootstrap_cnpj",
                "first_seen_at": now_iso,
                "last_seen_at": now_iso,
            }
        )

    return rows


def _ensure_company_alias_registry_exists() -> str:
    path = company_alias_registry_path()
    parent = os.path.dirname(path)
    if parent:
        try:
            os.makedirs(parent, exist_ok=True)
        except Exception:
            dbutils_ref = globals().get("dbutils")
            if dbutils_ref is not None and parent.startswith("/Volumes/"):
                dbutils_ref.fs.mkdirs(parent.replace("/Volumes/", "dbfs:/Volumes/"))

    file_created = False
    file_seeded = False
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COMPANY_ALIAS_REGISTRY_COLUMNS)
            writer.writeheader()
            writer.writerows(_bootstrap_company_alias_registry_rows())
        file_created = True
        file_seeded = True

    if not file_created:
        with open(path, "r", newline="", encoding="utf-8") as handle:
            has_data_rows = any(True for _ in csv.DictReader(handle))
        if not has_data_rows:
            with open(path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=COMPANY_ALIAS_REGISTRY_COLUMNS)
                writer.writeheader()
                writer.writerows(_bootstrap_company_alias_registry_rows())
            file_seeded = True

    if file_seeded:
        print(f"[OK] seeded company alias registry at {path}")

    return path


def _load_company_alias_registry(force_reload: bool = False) -> dict:
    global _COMPANY_ALIAS_REGISTRY_CACHE, _COMPANY_ALIAS_REGISTRY_MTIME

    path = _ensure_company_alias_registry_exists()
    current_mtime = os.path.getmtime(path) if os.path.exists(path) else None

    if (
        not force_reload
        and _COMPANY_ALIAS_REGISTRY_CACHE is not None
        and _COMPANY_ALIAS_REGISTRY_MTIME == current_mtime
    ):
        return _COMPANY_ALIAS_REGISTRY_CACHE

    payload = _empty_company_alias_registry_payload()

    with open(path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not _truthy_csv_flag(row.get("is_active")):
                continue

            canonical_name = clean_text(row.get("canonical_name"))
            grupo = clean_text(row.get("grupo"))
            canonical_cnpj = clean_cnpj(row.get("canonical_cnpj"))
            match_mode = _normalize_match_mode(row.get("match_mode"))
            alias_raw = clean_text(row.get("alias_name"))
            alias_norm = normalize_company_text(alias_raw)

            if canonical_cnpj and canonical_name:
                payload["cnpj_map"][canonical_cnpj] = (canonical_name, grupo)

            if not alias_norm or not canonical_name:
                continue

            if match_mode == "REGEX":
                payload["regex_rules"].append((alias_raw, canonical_name, grupo))
            elif match_mode == "CONTAINS":
                payload["contains_rules"].append((alias_norm, canonical_name, grupo))
            else:
                payload["exact_name_map"][alias_norm] = (canonical_name, grupo)

    _COMPANY_ALIAS_REGISTRY_CACHE = payload
    _COMPANY_ALIAS_REGISTRY_MTIME = current_mtime
    return payload


def register_company_alias_candidates(records, source_system: str | None = None) -> int:
    """
    Registra aliases observados no CSV de referencia.

    Observacoes capturadas automaticamente nao viram regra ativa sozinhas.
    O arquivo funciona como fila de revisao:
    - alias conhecido: preenche sugestao canonica
    - alias novo: entra apenas como observado
    Em ambos os casos, `is_active` fica 0 ate revisao manual.
    """
    path = _ensure_company_alias_registry_exists()
    now_iso = _utc_now_iso()

    with open(path, "r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        existing_rows = []
        for row in reader:
            normalized = {col: clean_text(row.get(col)) for col in COMPANY_ALIAS_REGISTRY_COLUMNS}
            for col in COMPANY_ALIAS_REGISTRY_COLUMNS:
                normalized.setdefault(col, "")
            existing_rows.append(normalized)

    rows_by_key = {
        _company_alias_registry_key(row.get("alias_name"), row.get("canonical_cnpj")): row
        for row in existing_rows
        if row.get("alias_name") or row.get("canonical_cnpj")
    }

    created = 0
    touched = False

    for record in records or []:
        alias_name = clean_text((record or {}).get("alias_name"))
        canonical_cnpj = clean_cnpj((record or {}).get("canonical_cnpj"))
        grupo = clean_text((record or {}).get("grupo"))
        alias_key = _company_alias_registry_key(alias_name, canonical_cnpj)

        if not alias_key[0]:
            continue

        current = rows_by_key.get(alias_key)
        if current is not None:
            if current.get("last_seen_at") != now_iso:
                current["last_seen_at"] = now_iso
                touched = True
            if source_system and not current.get("source_system"):
                current["source_system"] = source_system
                touched = True
            if grupo and not current.get("grupo"):
                current["grupo"] = grupo
                touched = True
            if not current.get("canonical_name"):
                suggested_name, suggested_group, matched = _resolve_empresa_alias_fallback_only(alias_name, canonical_cnpj)
                if matched and suggested_name:
                    current["canonical_name"] = suggested_name
                    touched = True
                if suggested_group and not current.get("grupo"):
                    current["grupo"] = suggested_group
                    touched = True
            continue

        suggested_name, suggested_group, matched = _resolve_empresa_alias_fallback_only(alias_name, canonical_cnpj)
        row = {
            "alias_name": alias_name,
            "canonical_name": suggested_name if matched else "",
            "grupo": grupo or suggested_group or "",
            "canonical_cnpj": canonical_cnpj or "",
            "match_mode": "EXACT",
            "is_active": "0",
            "source_system": source_system or "",
            "first_seen_at": now_iso,
            "last_seen_at": now_iso,
        }
        existing_rows.append(row)
        rows_by_key[alias_key] = row
        created += 1
        touched = True

    if touched:
        existing_rows = sorted(
            existing_rows,
            key=lambda row: (
                normalize_company_text(row.get("alias_name")) or "",
                clean_cnpj(row.get("canonical_cnpj")) or "",
            ),
        )
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COMPANY_ALIAS_REGISTRY_COLUMNS)
            writer.writeheader()
            writer.writerows(existing_rows)
        _load_company_alias_registry(force_reload=True)

    return created


def register_company_alias_candidates_df(df, column_specs, source_system: str | None = None) -> int:
    """
    Extrai aliases distintos de um DataFrame Spark e registra no CSV de referencia.

    `column_specs`:
      (nome_coluna_alias, nome_coluna_cnpj_ou_None, grupo_sugerido_ou_None)
    """
    if df is None:
        return 0

    projections = []
    df_columns = set(df.columns)
    for alias_col, cnpj_col, grupo in column_specs or []:
        if alias_col not in df_columns:
            continue

        cnpj_expr = (
            F.col(cnpj_col).cast("string")
            if cnpj_col and cnpj_col in df_columns
            else F.lit(None).cast("string")
        )

        projections.append(
            df.select(
                F.col(alias_col).cast("string").alias("alias_name"),
                cnpj_expr.alias("canonical_cnpj"),
                F.lit(grupo).cast("string").alias("grupo"),
            )
            .filter(F.col("alias_name").isNotNull())
            .filter(F.trim(F.col("alias_name")) != "")
        )

    if not projections:
        return 0

    distinct_df = projections[0]
    for next_df in projections[1:]:
        distinct_df = distinct_df.unionByName(next_df)

    records = []
    for row in distinct_df.dropDuplicates().toLocalIterator():
        records.append(row.asDict())

    return register_company_alias_candidates(records, source_system=source_system)


# ──────────────────────────────────────────────────────────────────────────────
# Resolução de alias e cliente final
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_empresa_alias_fallback_only(nome_raw: str, cnpj: str = None) -> tuple[str, str, bool]:
    """
    Retorna (nome_canonico, grupo, match_encontrado).
    Prioridade: CNPJ no CONSULTORIA_CLIENTE_MAP → regex em EMPRESA_ALIASES → fallback.
    """
    nome_norm  = normalize_company_text(nome_raw) or ""
    cnpj_limpo = clean_cnpj(cnpj)

    if cnpj_limpo and cnpj_limpo in CONSULTORIA_CLIENTE_MAP:
        return CONSULTORIA_CLIENTE_MAP[cnpj_limpo], "CONSULTORIA", True

    for pattern, canonical, grupo in EMPRESA_ALIASES:
        if re.search(pattern, nome_norm):
            return canonical, grupo, True

    return nome_norm or "NAO_IDENTIFICADA", None, False


def resolve_empresa_alias(nome_raw: str, cnpj: str = None) -> tuple[str, str, bool]:
    """
    Retorna (nome_canonico, grupo, match_encontrado).
    Prioridade:
      1. CSV de referencia (CNPJ / alias exato / contains / regex)
      2. fallback hardcoded por regex
      3. fallback hardcoded por CNPJ
      4. nome normalizado
    """
    nome_norm = normalize_company_text(nome_raw) or ""
    cnpj_limpo = clean_cnpj(cnpj)

    registry = _load_company_alias_registry()

    if cnpj_limpo and cnpj_limpo in registry["cnpj_map"]:
        canonical, grupo = registry["cnpj_map"][cnpj_limpo]
        return canonical, grupo, True

    if cnpj_limpo and cnpj_limpo in CONSULTORIA_CLIENTE_MAP:
        return CONSULTORIA_CLIENTE_MAP[cnpj_limpo], "CONSULTORIA", True

    if nome_norm in registry["exact_name_map"]:
        canonical, grupo = registry["exact_name_map"][nome_norm]
        return canonical, grupo, True

    for alias_norm, canonical, grupo in registry["contains_rules"]:
        if alias_norm and alias_norm in nome_norm:
            return canonical, grupo, True

    for pattern, canonical, grupo in registry["regex_rules"]:
        if pattern and re.search(pattern, nome_norm):
            return canonical, grupo, True

    for pattern, canonical, grupo in EMPRESA_ALIASES:
        if re.search(pattern, nome_norm):
            return canonical, grupo, True

    return nome_norm or "NAO_IDENTIFICADA", None, False


# ──────────────────────────────────────────────────────────────────────────────
# Geração de chaves canônicas
# ──────────────────────────────────────────────────────────────────────────────

def build_empresa_keys(nome_raw: str, cnpj: str = None, nome_canonico: str = None) -> dict:
    """
    Retorna dict com:
      cnpj_limpo       → str | None
      nome_normalizado → str | None
      entity_key       → "<14d>" ou "NOME_<norm>"
      id_empresa       → BIGINT derivado do CNPJ quando houver
    """
    cnpj_limpo       = clean_cnpj(cnpj)
    nome_base        = nome_canonico or nome_raw
    nome_normalizado = normalize_company_text(nome_base)

    if cnpj_limpo:
        entity_key = cnpj_limpo
    elif nome_normalizado:
        entity_key = f"NOME_{nome_normalizado}"
    else:
        entity_key = None

    return {
        "cnpj_limpo":       cnpj_limpo,
        "nome_normalizado": nome_normalizado,
        "entity_key":       entity_key,
        "id_empresa":       int(cnpj_limpo) if cnpj_limpo else None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Expressões Spark (Column API) — para uso em withColumn
# ──────────────────────────────────────────────────────────────────────────────

def _spark_normalize_nome(col_expr):
    """Aplica normalização de nome via Column API."""
    s = F.upper(F.trim(col_expr.cast("string")))
    for orig, repl in [
        (r"[ÁÀÂÃÄ]", "A"), (r"[ÉÈÊË]", "E"), (r"[ÍÌÎÏ]", "I"),
        (r"[ÓÒÔÕÖ]", "O"), (r"[ÚÙÛÜ]",  "U"), (r"Ç",      "C"),
    ]:
        s = F.regexp_replace(s, orig, repl)
    s = F.regexp_replace(s, r"[&_]",           " ")
    s = F.regexp_replace(s, r"[^A-Z0-9/.\s-]", " ")
    s = F.regexp_replace(s, r"\bS\.?A\.?\b",   " SA ")
    s = F.regexp_replace(s, r"\bLTDA\.?\b",    " LTDA ")
    s = F.regexp_replace(s, r"\bS/A\b",        " SA ")
    s = F.regexp_replace(s, r"\bME\b",         " ")
    s = F.regexp_replace(s, r"\bEPP\b",        " ")
    s = F.regexp_replace(s, r"\s+",            " ")
    return F.trim(s)


def _spark_minimal_name_key(col_expr):
    s = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    s = F.regexp_replace(s, r"\b\d{6,}\b", " ")
    s = F.regexp_replace(s, r"[^A-Z0-9]", " ")
    s = F.regexp_replace(s, r"\b(S A|SA|S A\.|LTDA|LTD|ME|MEI|EIRELI|SS|S S|SLU)\b", " ")
    s = F.regexp_replace(s, r"\s+", "")
    return F.when(s != "", s)


def _spark_clean_cnpj(cnpj_col):
    """Limpa CNPJ via Column API, retorna 14 dígitos ou NULL.

    Lida com floats em notação científica (ex: 1.39538E+13) e zero-padding.
    """
    from pyspark.sql.types import StringType

    return (
        F.when(
            cnpj_col.isNull() | (F.trim(cnpj_col.cast(StringType())) == ""),
            F.lit(None)
        ).otherwise(
            F.lpad(
                F.regexp_replace(
                    F.regexp_replace(cnpj_col.cast(StringType()), r"\.0+$", "")
                    .cast("bigint")
                    .cast(StringType()),
                    r"\D", ""
                ),
                14, "0"
            )
        )
    )


def expr_empresa_keys(nome_raw_col: str, cnpj_col: str = None, nome_canonico_col: str = None):
    """
    Retorna tuple (cnpj_limpo_expr, nome_normalizado_expr, entity_key_expr, id_empresa_expr).
    Tudo como Column objects.
    """
    nome_base        = F.col(nome_canonico_col) if nome_canonico_col else F.col(nome_raw_col)
    nome_normalizado = _spark_normalize_nome(nome_base)

    if cnpj_col:
        cnpj_limpo = _spark_clean_cnpj(F.col(cnpj_col))
    else:
        cnpj_limpo = F.lit(None).cast("string")

    entity_key = (
        F.when(cnpj_limpo.isNotNull(), cnpj_limpo)
         .when(nome_normalizado.isNotNull() & (nome_normalizado != ""),
               F.concat(F.lit("NOME_"), nome_normalizado))
    )
    id_empresa = F.when(cnpj_limpo.isNotNull(), cnpj_limpo.cast("bigint"))

    return cnpj_limpo, nome_normalizado, entity_key, id_empresa


def apply_empresa_keys(df, nome_raw_col: str, cnpj_col: str = None, nome_canonico_col: str = None):
    """
    Adiciona ao DataFrame as colunas:
      nu_cnpj, nome_normalizado, entity_key, id_empresa
    """
    cnpj_expr, nome_expr, ek_expr, id_expr = expr_empresa_keys(
        nome_raw_col, cnpj_col, nome_canonico_col
    )
    return (
        df
        .withColumn("nu_cnpj",          cnpj_expr)
        .withColumn("nome_normalizado",  nome_expr)
        .withColumn("entity_key",        ek_expr)
        .withColumn("id_empresa",        id_expr)
    )


# ──────────────────────────────────────────────────────────────────────────────
# Lookup id_empresa (helper de uso frequente)
# ──────────────────────────────────────────────────────────────────────────────

def lookup_id_empresa(df, bronze_empresas_table: str, spark_session=None):
    """
    Faz join com bronze.empresas para resolver id_empresa priorizando:
    id_empresa -> nu_cnpj -> nome_normalizado.
    Retorna df com coluna id_empresa adicionada.
    """
    _spark = spark_session or spark  # fallback para variável global
    input_cols = set(df.columns)
    empresas_df = (
        _spark.table(bronze_empresas_table)
        .select(
            F.col("id_empresa").cast("bigint").alias("empresa_id_ref"),
            F.col("nu_cnpj").cast("string").alias("nu_cnpj_ref"),
            F.col("nome_normalizado").cast("string").alias("nome_normalizado_ref"),
        )
        .dropDuplicates(["empresa_id_ref"])
    )

    join_conditions = []
    if "id_empresa" in input_cols:
        join_conditions.append(
            F.col("i.id_empresa").isNotNull() & (F.col("i.id_empresa").cast("bigint") == F.col("e.empresa_id_ref"))
        )
    if "nu_cnpj" in input_cols:
        join_conditions.append(
            F.col("i.nu_cnpj").isNotNull() & (F.col("i.nu_cnpj").cast("string") == F.col("e.nu_cnpj_ref"))
        )
    if "nome_normalizado" in input_cols:
        join_conditions.append(
            F.col("i.nome_normalizado").isNotNull() & (F.col("i.nome_normalizado").cast("string") == F.col("e.nome_normalizado_ref"))
        )

    if not join_conditions:
        raise ValueError("lookup_id_empresa requer ao menos uma das colunas: id_empresa, nu_cnpj ou nome_normalizado")

    join_expr = join_conditions[0]
    for cond in join_conditions[1:]:
        join_expr = join_expr | cond

    coalesce_exprs = [F.col("e.empresa_id_ref").cast("bigint")]
    if "id_empresa" in input_cols:
        coalesce_exprs.insert(0, F.col("i.id_empresa").cast("bigint"))
    if "nu_cnpj" in input_cols:
        coalesce_exprs.append(
            F.when(
                F.length(F.regexp_replace(F.coalesce(F.col("i.nu_cnpj"), F.lit("")), r"\D", "")) == 14,
                F.regexp_replace(F.col("i.nu_cnpj"), r"\D", "").cast("bigint"),
            )
        )

    result = (
        df.alias("i")
        .join(F.broadcast(empresas_df).alias("e"), join_expr, "left")
        .withColumn("id_empresa", F.coalesce(*coalesce_exprs))
        .select("i.*", F.col("id_empresa"))
    )
    sem_id = result.filter(F.col("id_empresa").isNull()).count()
    if sem_id > 0:
        print(f"[WARN] {sem_id} registro(s) sem id_empresa — sem match confiavel em bronze.empresas.")
    else:
        print("[OK] Todos os registros resolveram id_empresa.")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Merge padrão em bronze.empresas
# ──────────────────────────────────────────────────────────────────────────────

def merge_bronze_empresas(df_empresas, bronze_empresas_table: str, ds_origem: str,
                           pipeline_run_id: str, spark_session=None):
    """
    Faz MERGE de um DataFrame de empresas na tabela-pai bronze.empresas.
    O DataFrame deve ter as colunas:
      id_empresa, nu_cnpj, nome_legal_canonico, nome_operacional,
      nome_normalizado, grupo_empresa, pipeline_run_id, ingested_at
    """
    _spark = spark_session or spark
    (
        df_empresas
        .filter(F.col("id_empresa").isNotNull() | F.col("nu_cnpj").isNotNull())
        .dropDuplicates(["id_empresa"])
        .withColumn("ds_origem",       F.lit(ds_origem))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("ingested_at",     F.current_timestamp())
        .createOrReplaceTempView("_v_merge_empresas")
    )

    _spark.sql(f"""
    MERGE INTO {bronze_empresas_table} t
    USING _v_merge_empresas s
    ON t.id_empresa = s.id_empresa
    WHEN MATCHED THEN UPDATE SET
        t.id_empresa          = coalesce(s.id_empresa,           t.id_empresa),
        t.nu_cnpj             = coalesce(s.nu_cnpj,             t.nu_cnpj),
        t.nome_legal_canonico = coalesce(s.nome_legal_canonico, t.nome_legal_canonico),
        t.nome_operacional    = coalesce(s.nome_operacional,    t.nome_operacional),
        t.nome_normalizado    = coalesce(s.nome_normalizado,    t.nome_normalizado),
        t.grupo_empresa       = coalesce(s.grupo_empresa,       t.grupo_empresa),
        t.fontes              = s.fontes,
        t.ds_origem           = s.ds_origem,
        t.pipeline_run_id     = s.pipeline_run_id,
        t.ingested_at         = s.ingested_at
    WHEN NOT MATCHED THEN INSERT (
        id_empresa, nu_cnpj, nome_legal_canonico, nome_operacional,
        nome_normalizado, grupo_empresa, qualidade_match, fontes,
        aliases_raw, aliases_padrao, ds_origem, pipeline_run_id, ingested_at
    ) VALUES (
        s.id_empresa, s.nu_cnpj, s.nome_legal_canonico, s.nome_operacional,
        s.nome_normalizado, s.grupo_empresa, s.qualidade_match, s.fontes,
        s.aliases_raw, s.aliases_padrao, s.ds_origem, s.pipeline_run_id, s.ingested_at
    )
    """)
    print(f"[OK] bronze.empresas atualizado (origem: {ds_origem})")


def cleanup_bronze_empresas(bronze_empresas_table: str, spark_session=None):
    _spark = spark_session or spark
    if not _spark.catalog.tableExists(bronze_empresas_table):
        return

    df = _spark.table(bronze_empresas_table)
    original_cols = df.columns

    name_base = F.coalesce(
        F.col("nome_legal_canonico"),
        F.col("nome_operacional"),
        F.col("nome_normalizado"),
    )

    w_name = Window.partitionBy("minimal_name_key")

    df_clean = (
        df
        .withColumn("name_base", name_base)
        .withColumn("minimal_name_key", _spark_minimal_name_key(F.col("name_base")))
        .withColumn(
            "is_junk_name",
            (
                F.trim(F.coalesce(F.col("name_base"), F.lit(""))) == ""
            ) |
            F.upper(F.coalesce(F.col("name_base"), F.lit(""))).isin(
                "RECOLHIMENTO",
                "AGRUPAMENTO DE CONTRATANTES/COOPERATIVAS",
            ) |
            F.upper(F.coalesce(F.col("name_base"), F.lit(""))).rlike(r".*\b\d{6,}\b.*")
        )
        .withColumn(
            "has_cnpj_group",
            F.max(F.when(F.col("nu_cnpj").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(w_name)
        )
        .filter(
            ~(
                F.col("nu_cnpj").isNull() &
                (
                    (F.coalesce(F.col("qualidade_match"), F.lit("")) == F.lit("BAIXA")) |
                    F.col("is_junk_name")
                )
            )
        )
        .filter(
            ~(
                F.col("nu_cnpj").isNull() &
                F.col("minimal_name_key").isNotNull() &
                (F.col("has_cnpj_group") == 1)
            )
        )
        .select(*original_cols)
    )

    (
        df_clean
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(bronze_empresas_table)
    )
    print(f"[OK] bronze.empresas limpo em {bronze_empresas_table}.")
