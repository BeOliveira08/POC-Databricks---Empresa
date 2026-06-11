# Databricks notebook source
# silver: vinculo_competencia

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------
# MAGIC %run ../../../utils/transforms

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_STG_VINCULO = f"{CATALOG}.{SILVER}.stg_vinculo"
SILVER_STG_INSS = f"{CATALOG}.{SILVER}.stg_inss"
SILVER_STG_FGTS = f"{CATALOG}.{SILVER}.stg_fgts"
SILVER_STG_VINCULO_MENSAL = f"{CATALOG}.{SILVER}.stg_vinculo_mensal"

df_vinc = (
    spark.table(SILVER_STG_VINCULO)
    .select(
        "vinculo_id", "pessoa_id", "empresa_id", "pk_empresa", "cargo_id", "tipo_vinculo_id", "origem_id",
        F.col("nu_cpf").cast("string").alias("nu_cpf"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("tipo_contrato").cast("string").alias("tipo_contrato"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.col("status_vinculo").cast("string").alias("status_vinculo"),
        F.coalesce(F.col("vl_remuneracao"), F.lit(0.0)).cast("double").alias("vl_remuneracao")
    )
    .filter(F.col("dt_inicio").isNotNull())
    .withColumn("competencia_inicio", truncate_to_month(F.col("dt_inicio")))
    .withColumn("competencia_fim_raw", F.to_date(F.date_trunc("month", F.coalesce(F.col("dt_fim"), F.current_date()))))
    .withColumn(
        "competencia_fim",
        F.when(F.col("competencia_fim_raw") < F.col("competencia_inicio"), F.col("competencia_inicio"))
         .otherwise(F.col("competencia_fim_raw"))
    )
    .withColumn("dt_competencia", F.explode(F.expr("sequence(competencia_inicio, competencia_fim, interval 1 month)")))
    .withColumn("fl_ativo_competencia", F.when(F.col("dt_fim").isNull() | (truncate_to_month(F.col("dt_fim")) >= F.col("dt_competencia")), F.lit(1)).otherwise(F.lit(0)))
)

if table_exists(SILVER_STG_INSS):
    df_inss_raw = spark.table(SILVER_STG_INSS)
    cols_inss = set(df_inss_raw.columns)
    df_inss = (
        df_inss_raw
        .select(
            first_non_empty_string_expr(cols_inss, "cpf", "nu_cpf").alias("nu_cpf"),
            first_non_empty_string_expr(cols_inss, "pk_empresa", "sk_empresa").alias("pk_empresa"),
            truncate_to_month(first_cast_expr(cols_inss, "date", "competencia", "dt_competencia")).alias("dt_competencia"),
            first_cast_expr(cols_inss, "double", "vl_remuneracao", "remuneracao_valor", "vl_remuneracao_inss").alias("vl_remuneracao_inss")
        )
        .filter(F.col("nu_cpf").isNotNull())
        .groupBy("nu_cpf", "pk_empresa", "dt_competencia")
        .agg(F.max("vl_remuneracao_inss").alias("vl_remuneracao_inss"))
    )
else:
    df_inss = None

if table_exists(SILVER_STG_FGTS):
    df_fgts_raw = spark.table(SILVER_STG_FGTS)
    cols_fgts = set(df_fgts_raw.columns)
    df_fgts = (
        df_fgts_raw
        .select(
            first_non_empty_string_expr(cols_fgts, "cpf", "nu_cpf").alias("nu_cpf"),
            first_non_empty_string_expr(cols_fgts, "pk_empresa", "sk_empresa").alias("pk_empresa"),
            truncate_to_month(first_cast_expr(cols_fgts, "date", "competencia", "dt_competencia")).alias("dt_competencia"),
            first_cast_expr(cols_fgts, "double", "valor", "vl_lancamento_fgts").alias("valor_fgts"),
            first_cast_expr(cols_fgts, "double", "saldo", "vl_saldo_fgts").alias("saldo_fgts")
        )
        .filter(F.col("nu_cpf").isNotNull())
        .groupBy("nu_cpf", "pk_empresa", "dt_competencia")
        .agg(F.round(F.sum("valor_fgts"), 2).alias("valor_fgts"), F.max("saldo_fgts").alias("saldo_fgts"))
    )
else:
    df_fgts = None

df_final = df_vinc

if df_inss is not None:
    df_final = df_final.join(
        df_inss,
        on=[
            df_final.nu_cpf == df_inss.nu_cpf,
            df_final.dt_competencia == df_inss.dt_competencia,
            ((df_final.pk_empresa == df_inss.pk_empresa) | df_inss.pk_empresa.isNull())
        ],
        how="left"
    ).select(df_final["*"], df_inss["vl_remuneracao_inss"])
else:
    df_final = df_final.withColumn("vl_remuneracao_inss", F.lit(None).cast("double"))

if df_fgts is not None:
    df_final = df_final.join(
        df_fgts,
        on=[
            df_final.nu_cpf == df_fgts.nu_cpf,
            df_final.dt_competencia == df_fgts.dt_competencia,
            ((df_final.pk_empresa == df_fgts.pk_empresa) | df_fgts.pk_empresa.isNull())
        ],
        how="left"
    ).select(df_final["*"], df_fgts["valor_fgts"], df_fgts["saldo_fgts"])
else:
    df_final = df_final.withColumn("valor_fgts", F.lit(None).cast("double")).withColumn("saldo_fgts", F.lit(None).cast("double"))

df_final = (
    df_final
    .withColumn("competencia_id", F.date_format(F.col("dt_competencia"), "yyyyMM"))
    .withColumn("vinculo_competencia_id", F.concat(F.col("vinculo_id"), F.lit("|COMPETENCIA:"), F.col("competencia_id")))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("vinculo_competencia_id").cast("string").alias("vinculo_competencia_id"),
        F.col("vinculo_id").cast("string").alias("vinculo_id"),
        F.col("pessoa_id").cast("string").alias("pessoa_id"),
        F.col("empresa_id").cast("string").alias("empresa_id"),
        F.col("pk_empresa").cast("string").alias("pk_empresa"),
        F.col("cargo_id").cast("string").alias("cargo_id"),
        F.col("tipo_vinculo_id").cast("string").alias("tipo_vinculo_id"),
        F.col("origem_id").cast("string").alias("origem_id"),
        F.col("nu_cpf").cast("string").alias("nu_cpf"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("tipo_contrato").cast("string").alias("tipo_contrato"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.col("dt_competencia").cast("date").alias("dt_competencia"),
        F.col("competencia_id").cast("string").alias("competencia_id"),
        F.col("fl_ativo_competencia").cast("int").alias("fl_ativo_competencia"),
        F.col("vl_remuneracao").cast("double").alias("vl_remuneracao"),
        F.col("vl_remuneracao_inss").cast("double").alias("vl_remuneracao_inss"),
        F.col("valor_fgts").cast("double").alias("valor_fgts"),
        F.col("saldo_fgts").cast("double").alias("saldo_fgts"),
        F.col("status_vinculo").cast("string").alias("status_vinculo"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at")
    )
)

write_overwrite(df_final, SILVER_STG_VINCULO_MENSAL, catalog_schema=f"{CATALOG}.{SILVER}")







