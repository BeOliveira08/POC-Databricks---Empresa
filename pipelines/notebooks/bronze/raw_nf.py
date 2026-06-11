# Databricks notebook source
# bronze: raw_nf

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/readers

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

import os
import re
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

BRONZE_RAW_NF = f"{CATALOG}.{BRONZE}.raw_nf"


def _canon(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\ufeff", "").strip().lower()
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )
    text = re.sub(r"\s+", " ", text)
    return text


def _pick_col(columns, *candidates):
    canon_map = {_canon(col): col for col in columns}
    for candidate in candidates:
        picked = canon_map.get(_canon(candidate))
        if picked:
            return picked
    return None


def _pick_col_by_tokens(columns, *token_groups):
    canon_map = {_canon(col): col for col in columns}
    for canon_name, original_name in canon_map.items():
        for token_group in token_groups:
            if all(token in canon_name for token in token_group):
                return original_name
    return None


def _col_or_null(columns, *candidates):
    picked = _pick_col(columns, *candidates)
    if picked:
        return F.col(picked).cast("string")
    return F.lit(None).cast("string")


def _col_from_aliases_or_tokens(columns, aliases, *token_groups):
    picked = _pick_col(columns, *aliases)
    if not picked:
        picked = _pick_col_by_tokens(columns, *token_groups)
    if picked:
        return F.col(picked).cast("string")
    return F.lit(None).cast("string")


def _read_source(path: str):
    if path.lower().endswith(".xlsx"):
        return read_xlsx_auto(path)
    return read_csv_auto(path)


def _fonte_from_file_name(arquivo_origem: str) -> str:
    lowered = arquivo_origem.lower()
    if "provider_a" in lowered:
        return "PROVIDER_A"
    if "portfolio" in lowered:
        return "PORTFOLIO"
    return "NAO_IDENTIFICADO"


def _project_raw(df_raw, arquivo_origem: str):
    cols = df_raw.columns
    return (
        df_raw.select(
            F.lit(_fonte_from_file_name(arquivo_origem)).cast("string").alias("fonte"),
            F.regexp_replace(
                _col_from_aliases_or_tokens(
                    cols,
                    ["NFeS", "NFS-e", "NFSe", "Numero", "Número", "nf_numero", "numero_nf"],
                    ("nfs",),
                    ("nfse",),
                    ("numero", "nota"),
                ),
                r"\.0$",
                "",
            ).alias("nf_numero"),
            _col_from_aliases_or_tokens(
                cols,
                [
                    "Data da Emissão",
                    "Data de Emissão",
                    "Data Emissão",
                    "Emissão",
                    "data_emissao",
                    "data de emissao",
                    "dt_emissao",
                ],
                ("data", "emissa"),
                ("emissa",),
            ).alias("data_emissao"),
            _col_from_aliases_or_tokens(
                cols,
                ["Nome Oficial do Tomador", "Nome do Tomador", "tomador_nome", "tomador"],
                ("tomador", "nome"),
                ("razao", "tomador"),
            ).alias("tomador_nome"),
            _col_from_aliases_or_tokens(
                cols,
                ["CPF/CNPJ do Tomador", "tomador_cnpj", "cnpj do tomador", "cpf/cnpj tomador"],
                ("tomador", "cnpj"),
                ("tomador", "cpf"),
            ).alias("tomador_cnpj"),
            _col_from_aliases_or_tokens(
                cols,
                ["CPF/CNPJ do Prestador", "prestador_cnpj", "cnpj do prestador", "cpf/cnpj prestador"],
                ("prestador", "cnpj"),
                ("prestador", "cpf"),
            ).alias("prestador_cnpj"),
            _col_from_aliases_or_tokens(
                cols,
                ["Valor dos Serviços", "Valor do Serviço", "valor_servicos", "valor dos servicos"],
                ("valor", "servic"),
            ).alias("valor_servicos"),
            _col_from_aliases_or_tokens(
                cols,
                ["Valor das Deduções", "Valor da Dedução", "valor_deducoes", "valor das deducoes"],
                ("valor", "deduc"),
            ).alias("valor_deducoes"),
            _col_from_aliases_or_tokens(
                cols,
                ["Valor do ISS", "valor_iss", "iss"],
                ("valor", "iss"),
            ).alias("valor_iss"),
            _col_from_aliases_or_tokens(
                cols,
                ["Retido", "iss_retido", "iss retido"],
                ("iss", "retido"),
                ("retido",),
            ).alias("iss_retido"),
            _col_from_aliases_or_tokens(
                cols,
                ["NFE Paga", "NF paga", "nf_paga", "nota paga", "paga"],
                ("nfe", "paga"),
                ("nota", "paga"),
                ("paga",),
            ).alias("nf_paga"),
            _col_from_aliases_or_tokens(
                cols,
                ["Número da Guia", "Numero da Guia", "numero_guia", "guia"],
                ("numero", "guia"),
                ("guia",),
            ).alias("numero_guia"),
            _col_from_aliases_or_tokens(
                cols,
                ["id", "id_externo", "codigo", "código"],
                ("id",),
            ).alias("id_externo"),
        )
        .withColumn("arquivo_origem", F.lit(arquivo_origem))
        .withColumn("ingested_at", F.current_timestamp())
        .filter(
            F.col("nf_numero").isNotNull()
            | F.col("data_emissao").isNotNull()
            | F.col("tomador_nome").isNotNull()
        )
    )


schema = StructType(
    [
        StructField("fonte", StringType(), True),
        StructField("nf_numero", StringType(), True),
        StructField("data_emissao", StringType(), True),
        StructField("tomador_nome", StringType(), True),
        StructField("tomador_cnpj", StringType(), True),
        StructField("prestador_cnpj", StringType(), True),
        StructField("valor_servicos", StringType(), True),
        StructField("valor_deducoes", StringType(), True),
        StructField("valor_iss", StringType(), True),
        StructField("iss_retido", StringType(), True),
        StructField("nf_paga", StringType(), True),
        StructField("numero_guia", StringType(), True),
        StructField("id_externo", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

paths = list_files_by_extension_recursive(NOTAS_EMITIDAS_DIR, [".csv", ".xlsx"])
dfs = []
for path in paths:
    dfs.append(_project_raw(_read_source(path), os.path.basename(path)))

if dfs:
    df_bronze = dfs[0]
    for df_next in dfs[1:]:
        df_bronze = df_bronze.unionByName(df_next)
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_RAW_NF, catalog_schema=f"{CATALOG}.{BRONZE}")
