# Databricks notebook source
# silver: empresa_cargo

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------
# MAGIC %run ../../../utils/transforms

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
SILVER_TB_CARGO = f"{CATALOG}.{SILVER}.tb_cargo"
SILVER_TB_EMPRESA_CARGO = f"{CATALOG}.{SILVER}.tb_empresa_cargo"
BRONZE_CONTRATOS = f"{CATALOG}.{BRONZE}.contratos_geral"

if not table_exists(SILVER_TB_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_EMPRESA}")

if not table_exists(SILVER_TB_CARGO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_CARGO}")

if not table_exists(BRONZE_CONTRATOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_CONTRATOS}")


def name_key_expr(col_name: str):
    return F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", "")


df_empresas = (
    spark.table(SILVER_TB_EMPRESA)
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        normalize_cnpj_expr(only_digits_expr("nu_cnpj")).alias("nu_cnpj"),
        F.col("nome_legal_canonico").cast("string").alias("nome_legal_canonico"),
        F.col("nome_normalizado").cast("string").alias("nome_normalizado"),
        F.col("aliases_raw").cast("string").alias("aliases_raw"),
        F.col("aliases_padrao").cast("string").alias("aliases_padrao"),
    )
    .withColumn("empresa_nome_legal_key", name_key_expr("nome_legal_canonico"))
    .withColumn("empresa_nome_norm_key", name_key_expr("nome_normalizado"))
    .withColumn("empresa_alias_raw_key", name_key_expr("aliases_raw"))
    .withColumn("empresa_alias_padrao_key", name_key_expr("aliases_padrao"))
)

df_cargos = (
    spark.table(SILVER_TB_CARGO)
    .select(
        F.col("id_cargo").cast("bigint").alias("id_cargo"),
        F.col("cargo").cast("string").alias("cargo"),
        F.col("senioridade").cast("string").alias("senioridade"),
    )
)

df_contratos_raw = spark.table(BRONZE_CONTRATOS)
cols_contratos = set(df_contratos_raw.columns)

df_contratos_base = (
    df_contratos_raw
    .select(
        normalize_cnpj_expr(first_digits_expr(cols_contratos, "cnpj_contratante", "cnpj_contratante_limpo")).alias("nu_cnpj"),
        norm_text("nome_contratante_origem").alias("consultoria_norm"),
        norm_text("cargo_tratado").alias("cargo"),
        norm_text("senioridade_tratada").alias("senioridade"),
        first_cast_expr(cols_contratos, "string", "pipeline_run_id").alias("pipeline_run_id_origem"),
        first_cast_expr(cols_contratos, "timestamp", "ingested_at").alias("ingested_at_origem"),
    )
    .withColumn("consultoria_norm", F.when(F.col("consultoria_norm") != "", F.col("consultoria_norm")))
    .withColumn("cargo", F.when(F.col("cargo") != "", F.col("cargo")))
    .withColumn("senioridade", F.when(F.col("senioridade") != "", F.col("senioridade")))
    .filter(F.col("nu_cnpj").isNotNull() & F.col("cargo").isNotNull())
    .withColumn("consultoria_key", name_key_expr("consultoria_norm"))
)

df_validado = (
    df_contratos_base
    .join(df_empresas, on="nu_cnpj", how="inner")
    .withColumn(
        "fl_nome_match",
        (
            (F.col("consultoria_key") != "") &
            (
                (F.col("consultoria_key") == F.col("empresa_nome_legal_key")) |
                (F.col("consultoria_key") == F.col("empresa_nome_norm_key")) |
                (((F.col("empresa_nome_legal_key") != "") & (F.instr(F.col("empresa_nome_legal_key"), F.col("consultoria_key")) > 0))) |
                (((F.col("empresa_nome_norm_key") != "") & (F.instr(F.col("empresa_nome_norm_key"), F.col("consultoria_key")) > 0))) |
                (((F.col("empresa_alias_raw_key") != "") & (F.instr(F.col("empresa_alias_raw_key"), F.col("consultoria_key")) > 0))) |
                (((F.col("empresa_alias_padrao_key") != "") & (F.instr(F.col("empresa_alias_padrao_key"), F.col("consultoria_key")) > 0)))
            )
        )
    )
    .filter(F.col("fl_nome_match"))
)

df_final = (
    df_validado
    .join(df_cargos, on=["cargo", "senioridade"], how="inner")
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("id_cargo").cast("bigint").alias("id_cargo"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
    )
    .dropDuplicates(["empresa_id", "id_cargo"])
    .withColumn(
        "id_empresa_cargo",
        F.row_number().over(
            Window.orderBy(
                F.col("empresa_id").asc(),
                F.col("id_cargo").asc(),
            )
        ).cast("bigint")
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_empresa_cargo").cast("bigint").alias("id_empresa_cargo"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("id_cargo").cast("bigint").alias("id_cargo"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

write_overwrite(df_final, SILVER_TB_EMPRESA_CARGO, catalog_schema=f"{CATALOG}.{SILVER}")

