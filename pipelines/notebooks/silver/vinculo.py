# Databricks notebook source
# silver: tb_ctps

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_CTPS = f"{CATALOG}.{BRONZE}.raw_ctps"
SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
money_udf = F.udf(br_money_to_float, "double")
date_udf = F.udf(parse_flexible_date_to_iso, "string")


def parse_date_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")


def normalized_status_expr():
    status = F.upper(F.trim(F.coalesce(F.col("status").cast("string"), F.lit(""))))
    return (
        F.when(status.isin("ABERTO", "ATIVO", "VIGENTE"), F.lit("ABERTO"))
         .when(status.isin("FECHADO", "ENCERRADO", "FINALIZADO", "RESCINDIDO"), F.lit("FECHADO"))
         .otherwise(F.lit(None).cast("string"))
    )


def sane_salario_expr(col_name: str):
    valor = money_udf(col_name)
    return (
        F.when(valor.isNull(), F.lit(None).cast("double"))
         .when(valor >= F.lit(100000.0), F.round(valor / F.lit(100.0), 2))
         .otherwise(F.round(valor, 2))
    )


def empty_ctps_df():
    return empty_df_for_schema(
        """
        fonte string,
        nu_cnpj_empresa string,
        nome_empresa string,
        cargo string,
        dt_inicio date,
        dt_fim date,
        vl_salario double,
        status string,
        arquivo_origem string
        """
    )


def non_empty_expr(col_name: str):
    return F.when(F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit(""))) != "", F.col(col_name))


def cleaned_empresa_expr(col_name: str):
    cleaned = norm_text(col_name)
    cleaned = F.regexp_replace(F.coalesce(cleaned, F.lit("")), r"\{[^}]*\}", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCARTA PROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA COMERCIAL\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\b\d{4,}\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)
    return F.when(cleaned == "", F.lit(None).cast("string")).otherwise(cleaned)


def read_ctps_pdf():
    if not table_exists(BRONZE_RAW_CTPS):
        return empty_ctps_df()
    return (
        spark.table(BRONZE_RAW_CTPS)
        .select(
            F.coalesce(norm_text("fonte_referencia"), F.lit("DOCUMENTO")).alias("fonte"),
            normalize_cnpj_expr(only_digits_expr("nu_cnpj_empresa")).alias("nu_cnpj_empresa"),
            cleaned_empresa_expr("nome_empresa").alias("nome_empresa"),
            F.when(F.trim(F.coalesce(norm_text("cargo"), F.lit(""))) == "", F.lit("ENGENHEIRO DE DADOS"))
             .otherwise(norm_text("cargo")).alias("cargo"),
            parse_date_expr("dt_inicio").alias("dt_inicio"),
            parse_date_expr("dt_fim").alias("dt_fim"),
            sane_salario_expr("vl_salario").alias("vl_salario"),
            norm_text("status").alias("status"),
            F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_origem_referencia"), F.col("arquivo_pdf_validacao")).cast("string").alias("arquivo_origem"),
        )
    )


def read_ctps_planilha():
    return empty_ctps_df()


df_final = (
    read_ctps_pdf()
    .filter(F.col("nome_empresa").isNotNull() | F.col("nu_cnpj_empresa").isNotNull())
    .withColumn(
        "dt_fim",
        F.when(
            F.col("dt_inicio").isNotNull() & F.col("dt_fim").isNotNull() & (F.col("dt_fim") < F.col("dt_inicio")),
            F.lit(None).cast("date"),
        ).otherwise(F.col("dt_fim"))
    )
    .withColumn("status_normalizado", normalized_status_expr())
    .withColumn(
        "status",
        F.when(
            F.col("dt_fim").isNotNull() & (F.col("dt_fim") <= F.current_date()),
            F.lit("FECHADO"),
        ).when(
            F.col("status_normalizado").isNotNull(),
            F.col("status_normalizado"),
        ).when(
            F.col("dt_inicio").isNotNull(),
            F.lit("ABERTO"),
        ).otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "job_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("nu_cnpj_empresa"), F.col("nome_empresa"), F.lit("")),
            F.coalesce(F.col("cargo"), F.lit("")),
            F.coalesce(F.round(F.col("vl_salario"), 2).cast("string"), F.lit("")),
        ),
    )
    .withColumn(
        "completeness_score",
        F.when(F.col("dt_inicio").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("dt_fim").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("status").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )
)

w = Window.partitionBy(
    F.col("job_key"),
).orderBy(
    F.col("completeness_score").desc(),
    F.when(F.col("fonte") == "PDF", F.lit(0)).otherwise(F.lit(1)),
    F.col("dt_fim").desc_nulls_last(),
    F.col("vl_salario").desc_nulls_last(),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_id_ctps = Window.orderBy(
    F.coalesce(F.col("nu_cnpj_empresa"), F.lit("")),
    F.coalesce(F.col("nome_empresa"), F.lit("")),
    F.coalesce(F.col("cargo"), F.lit("")),
    F.coalesce(F.col("dt_inicio").cast("string"), F.lit("")),
    F.coalesce(F.col("arquivo_origem"), F.lit("")),
)

df_final = (
    df_final
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "job_key", "completeness_score", "status_normalizado")
    .withColumn("id_ctps", F.row_number().over(w_id_ctps))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_ctps").cast("int").alias("id_ctps"),
        F.col("fonte").cast("string").alias("fonte"),
        F.col("nu_cnpj_empresa").cast("string").alias("nu_cnpj_empresa"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("cargo").cast("string").alias("cargo"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.col("vl_salario").cast("double").alias("vl_salario"),
        F.col("status").cast("string").alias("status"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_CTPS, catalog_schema=f"{CATALOG}.{SILVER}")
