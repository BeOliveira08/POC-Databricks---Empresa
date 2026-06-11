# Databricks notebook source
# silver: informe_rendimentos

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_IRPF = f"{CATALOG}.{SILVER}.tb_irpf"
SILVER_TB_INFORME_RENDIMENTOS = f"{CATALOG}.{SILVER}.tb_informe_rendimentos"

if not table_exists(SILVER_TB_IRPF):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_IRPF}")

df_final = (
    spark.table(SILVER_TB_IRPF)
    .withColumn(
        "id_informe_rendimentos",
        F.row_number().over(
            Window.orderBy(
                F.col("cpf").asc_nulls_last(),
                F.col("ano_calendario").asc_nulls_last(),
                F.col("exercicio").asc_nulls_last(),
            )
        ).cast("bigint")
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_informe_rendimentos").cast("bigint").alias("id_informe_rendimentos"),
        F.col("cpf").cast("string").alias("cpf"),
        F.col("nome").cast("string").alias("nome"),
        F.col("exercicio").cast("int").alias("exercicio"),
        F.col("ano_calendario").cast("int").alias("ano_calendario"),
        F.col("rend_tributavel_total").cast("double").alias("rendimentos_tributaveis"),
        F.col("imposto_pago_total").cast("double").alias("imposto_retido_total"),
        F.col("imposto_restituir").cast("double").alias("imposto_restituir"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_INFORME_RENDIMENTOS, catalog_schema=f"{CATALOG}.{SILVER}")
