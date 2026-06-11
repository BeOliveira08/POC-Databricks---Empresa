# Databricks notebook source
# silver: tb_das

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

BRONZE_DAS = f"{CATALOG}.{BRONZE}.raw_das"
SILVER_TB_DAS = f"{CATALOG}.{SILVER}.tb_das"
money_udf = F.udf(br_money_to_float, "double")
date_udf = F.udf(parse_flexible_date_to_iso, "string")

if not table_exists(BRONZE_DAS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_DAS}")


def nullable_text_expr(col_name: str):
    txt = norm_text(col_name)
    return F.when(
        txt.isNull() | txt.isin("NAO INFORMADO", "NÃO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def parse_date_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")


def parse_competencia_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")

df_final = (
    spark.table(BRONZE_DAS)
    .select(
        normalize_cnpj_expr(only_digits_expr("cnpj_prestador")).alias("nu_cnpj_prestador"),
        nullable_text_expr("razao_social").alias("nm_prestador"),
        parse_competencia_expr("competencia").alias("competencia"),
        parse_date_expr("dt_vencimento").alias("dt_vencimento"),
        parse_date_expr("dt_arrecadacao").alias("dt_arrecadacao"),
        nullable_text_expr("nr_documento").alias("nr_documento"),
        nullable_text_expr("cd_tributo").alias("cd_tributo"),
        nullable_text_expr("ds_tributo").alias("ds_tributo"),
        money_udf("vl_principal").alias("vl_principal"),
        money_udf("vl_multa").alias("vl_multa"),
        money_udf("vl_juros").alias("vl_juros"),
        money_udf("vl_total").alias("vl_total"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .filter(F.col("nu_cnpj_prestador").isNotNull())
    .filter(F.col("competencia").isNotNull())
    .filter(F.col("cd_tributo").isNotNull())
)

w = Window.partitionBy(
    "nu_cnpj_prestador",
    "competencia",
    "nr_documento",
    "cd_tributo",
).orderBy(
    F.when(F.col("dt_arrecadacao").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("dt_arrecadacao").desc_nulls_last(),
    F.col("vl_total").desc_nulls_last(),
    F.col("ingested_at_origem").desc_nulls_last(),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_id_das = Window.orderBy(
    F.coalesce(F.col("nu_cnpj_prestador"), F.lit("")),
    F.coalesce(F.col("competencia").cast("string"), F.lit("")),
    F.coalesce(F.col("nr_documento"), F.lit("")),
    F.coalesce(F.col("cd_tributo"), F.lit("")),
)

df_final = (
    df_final
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("id_das", F.row_number().over(w_id_das))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_das").cast("int").alias("id_das"),
        F.col("nu_cnpj_prestador").cast("string").alias("nu_cnpj_prestador"),
        F.col("nm_prestador").cast("string").alias("nm_prestador"),
        F.col("competencia").cast("date").alias("competencia"),
        F.col("dt_vencimento").cast("date").alias("dt_vencimento"),
        F.col("dt_arrecadacao").cast("date").alias("dt_arrecadacao"),
        F.col("nr_documento").cast("string").alias("nr_documento"),
        F.col("cd_tributo").cast("string").alias("cd_tributo"),
        F.col("ds_tributo").cast("string").alias("ds_tributo"),
        F.col("vl_principal").cast("double").alias("vl_principal"),
        F.col("vl_multa").cast("double").alias("vl_multa"),
        F.col("vl_juros").cast("double").alias("vl_juros"),
        F.col("vl_total").cast("double").alias("vl_total"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_DAS, catalog_schema=f"{CATALOG}.{SILVER}")
