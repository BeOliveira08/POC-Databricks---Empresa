# Databricks notebook source
# bronze: beneficios_empresa

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

BRONZE_BENEFICIOS = f"{CATALOG}.{BRONZE}.raw_beneficios"
BRONZE_BENEFICIOS_ITEM = f"{CATALOG}.{BRONZE}.raw_beneficios_item"


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


def _project(df_raw, arquivo_origem: str):
    cols = df_raw.columns
    return (
        df_raw.select(
            _col_or_null(cols, "id_empresa").alias("id_empresa_raw"),
            _col_or_null(cols, "cnpj").alias("cnpj_raw"),
            _col_or_null(cols, "empresa").alias("empresa_raw"),
            _col_or_null(cols, "empresa_corrigida").alias("empresa_corrigida_raw"),
            _col_or_null(cols, "nome_operacional_canonico").alias("nome_operacional_raw"),
            _col_or_null(cols, "status_mapeamento").alias("status_mapeamento_raw"),
            _col_or_null(cols, "observacao_mapeamento").alias("observacao_mapeamento_raw"),
            _col_or_null(cols, "plano_saude").alias("plano_saude_raw"),
            _col_or_null(cols, "plano_odontologico").alias("plano_odontologico_raw"),
            _col_or_null(cols, "wellhub").alias("wellhub_raw"),
            _col_or_null(cols, "lazer").alias("lazer_raw"),
            _col_or_null(cols, "day_off_aniversario").alias("day_off_aniversario_raw"),
            _col_or_null(cols, "seguro_vida").alias("seguro_vida_raw"),
            _col_or_null(cols, "vr_va").alias("vr_va_raw"),
            _col_or_null(cols, "nome _va/vr", "nome_va_vr", "nome  va/vr").alias("nome_vr_va_raw"),
            _col_or_null(cols, "bem_estar").alias("bem_estar_raw"),
            _col_or_null(cols, "idiomas").alias("idiomas_raw"),
            _col_or_null(cols, "participacao_lucro").alias("participacao_lucro_raw"),
        )
        .withColumn("arquivo_origem", F.lit(arquivo_origem))
        .withColumn("ingested_at", F.current_timestamp())
    )


def _explode_items(df_wide):
    specs = [
        ("PLANO_SAUDE", "SAUDE", "plano_saude_raw", None, "plano_saude_raw"),
        ("PLANO_ODONTOLOGICO", "SAUDE", "plano_odontologico_raw", None, "plano_odontologico_raw"),
        ("WELLHUB", "BEM_ESTAR", "wellhub_raw", None, "wellhub_raw"),
        ("LAZER", "BEM_ESTAR", "lazer_raw", None, "lazer_raw"),
        ("DAY_OFF_ANIVERSARIO", "TEMPO", "day_off_aniversario_raw", None, "day_off_aniversario_raw"),
        ("SEGURO_VIDA", "PROTECAO", "seguro_vida_raw", None, "seguro_vida_raw"),
        ("VR_VA", "ALIMENTACAO", "vr_va_raw", "nome_vr_va_raw", "vr_va_raw"),
        ("BEM_ESTAR", "BEM_ESTAR", "bem_estar_raw", None, "bem_estar_raw"),
        ("IDIOMAS", "DESENVOLVIMENTO", "idiomas_raw", None, "idiomas_raw"),
        ("PLR", "FINANCEIRO", "participacao_lucro_raw", None, "participacao_lucro_raw"),
    ]

    item_dfs = []
    for tipo_beneficio, grupo_beneficio, descricao_col, nome_col, valor_col in specs:
        item_dfs.append(
            df_wide.select(
                F.col("id_empresa_raw"),
                F.col("cnpj_raw"),
                F.col("empresa_raw"),
                F.col("empresa_corrigida_raw"),
                F.col("nome_operacional_raw"),
                F.col("status_mapeamento_raw"),
                F.col("observacao_mapeamento_raw"),
                F.lit(tipo_beneficio).cast("string").alias("tipo_beneficio_raw"),
                F.lit(grupo_beneficio).cast("string").alias("grupo_beneficio_raw"),
                F.lit(descricao_col).cast("string").alias("campo_origem_raw"),
                (F.col(nome_col).cast("string") if nome_col else F.lit(None).cast("string")).alias("beneficio_nome_raw"),
                F.col(descricao_col).cast("string").alias("beneficio_descricao_raw"),
                F.col(valor_col).cast("string").alias("beneficio_valor_raw"),
                F.col("arquivo_origem"),
                F.col("ingested_at"),
            )
        )

    if not item_dfs:
        return spark.createDataFrame([], schema=item_schema)

    df_items = item_dfs[0]
    for df_next in item_dfs[1:]:
        df_items = df_items.unionByName(df_next)

    return df_items.filter(
        F.trim(
            F.coalesce(
                F.col("beneficio_nome_raw"),
                F.col("beneficio_descricao_raw"),
                F.col("beneficio_valor_raw"),
                F.lit(""),
            )
        ) != ""
    )


schema = StructType(
    [
        StructField("id_empresa_raw", StringType(), True),
        StructField("cnpj_raw", StringType(), True),
        StructField("empresa_raw", StringType(), True),
        StructField("empresa_corrigida_raw", StringType(), True),
        StructField("nome_operacional_raw", StringType(), True),
        StructField("status_mapeamento_raw", StringType(), True),
        StructField("observacao_mapeamento_raw", StringType(), True),
        StructField("plano_saude_raw", StringType(), True),
        StructField("plano_odontologico_raw", StringType(), True),
        StructField("wellhub_raw", StringType(), True),
        StructField("lazer_raw", StringType(), True),
        StructField("day_off_aniversario_raw", StringType(), True),
        StructField("seguro_vida_raw", StringType(), True),
        StructField("vr_va_raw", StringType(), True),
        StructField("nome_vr_va_raw", StringType(), True),
        StructField("bem_estar_raw", StringType(), True),
        StructField("idiomas_raw", StringType(), True),
        StructField("participacao_lucro_raw", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

item_schema = StructType(
    [
        StructField("id_empresa_raw", StringType(), True),
        StructField("cnpj_raw", StringType(), True),
        StructField("empresa_raw", StringType(), True),
        StructField("empresa_corrigida_raw", StringType(), True),
        StructField("nome_operacional_raw", StringType(), True),
        StructField("status_mapeamento_raw", StringType(), True),
        StructField("observacao_mapeamento_raw", StringType(), True),
        StructField("tipo_beneficio_raw", StringType(), True),
        StructField("grupo_beneficio_raw", StringType(), True),
        StructField("campo_origem_raw", StringType(), True),
        StructField("beneficio_nome_raw", StringType(), True),
        StructField("beneficio_descricao_raw", StringType(), True),
        StructField("beneficio_valor_raw", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

paths = list_files_by_extension(BENEFICIOS_DIR, [".csv"])
dfs = []
for path in paths:
    dfs.append(_project(_read_csv_local(path), os.path.basename(path)))

if dfs:
    df_bronze = dfs[0]
    for df_next in dfs[1:]:
        df_bronze = df_bronze.unionByName(df_next)
else:
    df_bronze = spark.createDataFrame([], schema=schema)

df_bronze_itens = _explode_items(df_bronze) if df_bronze.columns else spark.createDataFrame([], schema=item_schema)

write_overwrite(df_bronze, BRONZE_BENEFICIOS, catalog_schema=f"{CATALOG}.{BRONZE}")
write_overwrite(df_bronze_itens, BRONZE_BENEFICIOS_ITEM, catalog_schema=f"{CATALOG}.{BRONZE}")
