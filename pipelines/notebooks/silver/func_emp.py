# Databricks notebook source
# silver: tb_func_emp

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------
# MAGIC %run ../../utils/company

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_FUNC_EMP = f"{CATALOG}.{BRONZE}.raw_func_emp"
SILVER_TB_FUNCIONARIO = f"{CATALOG}.{SILVER}.tb_funcionario"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
SILVER_TB_FUNC_EMP = f"{CATALOG}.{SILVER}.tb_func_emp"

if not table_exists(BRONZE_RAW_FUNC_EMP):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_RAW_FUNC_EMP}")
if not table_exists(SILVER_TB_FUNCIONARIO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_FUNCIONARIO}")
if not table_exists(SILVER_TB_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_EMPRESA}")

EMPLOYEE_NAME_ALIASES = {
    "ALEXANDER": "ALEXSANDER",
}


def canonical_company_udf(col_expr):
    return F.udf(
        lambda nome: (resolve_empresa_alias(nome)[0] if nome else None),
        "string",
    )(col_expr)


def normalized_lookup_expr(col_name: str):
    return F.udf(normalize_company_text, "string")(repair_text(col_name))


def normalized_text_expr(col_name: str):
    return F.upper(
        F.trim(
            F.regexp_replace(
                F.coalesce(repair_text(col_name), F.lit("")),
                r"\s+",
                " ",
            )
        )
    )


def nullable_text_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt == "") | txt.isin("NAO INFORMADO", "NÃƒO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def base_name_key_expr(col_name: str):
    txt = F.upper(F.coalesce(normalized_lookup_expr(col_name), F.lit("")))
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(
        txt,
        r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU|CONSULTORIA|CONSULTING|SERVICOS|TECNOLOGIA|INFORMATICA|DESENVOLVIMENTO|SOLUCOES|ENGENHARIA|ASSESSORIA|EMPRESARIAL|DIGITAL|SOFTWARE|SISTEMAS|COOPERATIVA|TRABALHO|PROFISSIONAIS|INFORMACAO|TELECOM|DO|DA|DE|E)\b",
        " ",
    )
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def canonical_employee_lookup_expr(col_name: str):
    lookup = normalized_lookup_expr(col_name)
    alias_pairs = []
    for alias_name, canonical_name in EMPLOYEE_NAME_ALIASES.items():
        alias_pairs.extend([F.lit(alias_name), F.lit(canonical_name)])
    alias_map = F.create_map(*alias_pairs)
    return F.coalesce(alias_map[lookup], lookup)


def normalized_status_expr():
    status = normalized_text_expr("status_raw")
    return (
        F.when(status.isin("ABERTO", "ATIVO", "VIGENTE"), F.lit("ATIVO"))
         .when(status.isin("FECHADO", "ENCERRADO", "FINALIZADO", "INATIVO"), F.lit("ENCERRADO"))
         .otherwise(F.lit(None).cast("string"))
    )


df_funcionario = (
    spark.table(SILVER_TB_FUNCIONARIO)
    .select(
        F.col("id_funcionario").cast("int").alias("id_funcionario"),
        F.col("nome_funcionario").cast("string").alias("nome_funcionario_ref"),
    )
    .withColumn("nome_funcionario_key", canonical_employee_lookup_expr("nome_funcionario_ref"))
)

df_empresa = (
    spark.table(SILVER_TB_EMPRESA)
    .select(
        F.col("empresa_id").cast("int").alias("empresa_id"),
        F.col("nm_empresa").cast("string").alias("nome_empresa_ref"),
    )
    .withColumn("nome_empresa_key", normalized_lookup_expr("nome_empresa_ref"))
    .withColumn("nome_empresa_base_key", base_name_key_expr("nome_empresa_ref"))
)

df_base = (
    spark.table(BRONZE_RAW_FUNC_EMP)
    .select(
        nullable_text_expr("nome_funcionario_raw").alias("nome_funcionario"),
        nullable_text_expr("empresa_funcionario_raw").alias("empresa_funcionario_raw"),
        normalized_status_expr().alias("status"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
    )
    .filter(F.trim(F.coalesce(F.col("nome_funcionario"), F.lit(""))) != "")
    .withColumn("func_emp_row_id", F.monotonically_increasing_id())
    .withColumn("nome_empresa", canonical_company_udf(F.col("empresa_funcionario_raw")))
    .withColumn("nome_funcionario_key", canonical_employee_lookup_expr("nome_funcionario"))
    .withColumn("nome_empresa_key", normalized_lookup_expr("nome_empresa"))
    .withColumn("nome_empresa_base_key", base_name_key_expr("nome_empresa"))
)

df_match = (
    df_base.alias("base")
    .join(df_funcionario.alias("func"), on="nome_funcionario_key", how="left")
    .join(
        F.broadcast(df_empresa).alias("emp"),
        (
            F.col("base.nome_empresa_key").isNotNull() &
            (F.col("base.nome_empresa_key") == F.col("emp.nome_empresa_key"))
        ) |
        (
            F.col("base.nome_empresa_base_key").isNotNull() &
            (F.col("base.nome_empresa_base_key") == F.col("emp.nome_empresa_base_key"))
        ),
        "left",
    )
    .withColumn(
        "empresa_match_score",
        F.when(
            F.col("base.nome_empresa_key").isNotNull() &
            (F.col("base.nome_empresa_key") == F.col("emp.nome_empresa_key")),
            F.lit(2),
        ).when(
            F.col("base.nome_empresa_base_key").isNotNull() &
            (F.col("base.nome_empresa_base_key") == F.col("emp.nome_empresa_base_key")),
            F.lit(1),
        ).otherwise(F.lit(0))
    )
)

w_match = Window.partitionBy("func_emp_row_id").orderBy(
    F.col("empresa_match_score").desc(),
    F.when(F.col("emp.empresa_id").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("emp.empresa_id").asc_nulls_last(),
)

df_final = (
    df_match
    .withColumn("rn_match", F.row_number().over(w_match))
    .filter(F.col("rn_match") == 1)
    .dropDuplicates(
        [
            "nome_funcionario",
            "nome_empresa",
            "status",
            "arquivo_origem",
        ]
    )
    .withColumn(
        "id_funcionario_empresa",
        F.row_number().over(
            Window.orderBy(
                F.coalesce(F.col("nome_funcionario"), F.lit("")),
                F.coalesce(F.col("nome_empresa"), F.lit("")),
                F.coalesce(F.col("status"), F.lit("")),
                F.coalesce(F.col("arquivo_origem"), F.lit("")),
            )
        )
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_funcionario_empresa").cast("int").alias("id_funcionario_empresa"),
        F.col("func.id_funcionario").cast("int").alias("id_funcionario"),
        F.col("emp.empresa_id").cast("int").alias("empresa_id"),
        F.col("nome_funcionario").cast("string").alias("nome_funcionario"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("status").cast("string").alias("status"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_FUNC_EMP, catalog_schema=f"{CATALOG}.{SILVER}")
