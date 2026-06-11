# Databricks notebook source
# silver: tb_funcionario

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

BRONZE_FUNCIONARIOS_REF = f"{CATALOG}.{BRONZE}.raw_funcionarios"
SILVER_TB_FUNCIONARIO = f"{CATALOG}.{SILVER}.tb_funcionario"

if not table_exists(BRONZE_FUNCIONARIOS_REF):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_FUNCIONARIOS_REF}")


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


def normalized_phone_expr(col_name: str):
    phone = F.regexp_replace(F.coalesce(repair_text(col_name), F.lit("")), r"\D+", "")
    phone = F.trim(phone)
    return F.when(F.length(phone) >= 10, phone).otherwise(F.lit(None).cast("string"))


df_final = (
    spark.table(BRONZE_FUNCIONARIOS_REF)
    .select(
        normalized_text_expr("nome_funcionario_raw").alias("nome_funcionario"),
        normalized_phone_expr("telefone_raw").alias("telefone"),
        normalized_text_expr("senioridade_raw").alias("senioridade"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
    )
    .filter(
        (F.trim(F.coalesce(F.col("nome_funcionario"), F.lit(""))) != "")
    )
    .withColumn(
        "nome_funcionario",
        F.when(F.trim(F.coalesce(F.col("nome_funcionario"), F.lit(""))) == "", F.lit(None).cast("string"))
         .otherwise(F.col("nome_funcionario"))
    )
    .withColumn(
        "senioridade",
        F.when(F.trim(F.coalesce(F.col("senioridade"), F.lit(""))) == "", F.lit("PLENO"))
         .otherwise(F.col("senioridade"))
    )
)

df_final = (
    df_final
    .dropDuplicates(["nome_funcionario", "telefone", "senioridade", "arquivo_origem"])
    .withColumn(
        "id_funcionario",
        F.row_number().over(
            Window.orderBy(
                F.coalesce(F.col("nome_funcionario"), F.lit("")),
                F.coalesce(F.col("telefone"), F.lit("")),
                F.coalesce(F.col("senioridade"), F.lit("")),
                F.coalesce(F.col("arquivo_origem"), F.lit("")),
            )
        )
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_funcionario").cast("int").alias("id_funcionario"),
        F.col("nome_funcionario").cast("string").alias("nome_funcionario"),
        F.col("telefone").cast("string").alias("telefone"),
        F.col("senioridade").cast("string").alias("senioridade"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_FUNCIONARIO, catalog_schema=f"{CATALOG}.{SILVER}")
