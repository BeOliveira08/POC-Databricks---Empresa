# Databricks notebook source
# silver: pessoa

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_PESSOA = f"{CATALOG}.{SILVER}.tb_pessoa"
SILVER_TB_FUNCIONARIO_EMPRESA = f"{CATALOG}.{SILVER}.tb_funcionario_empresa"

df_base = (
    spark.table(SILVER_TB_FUNCIONARIO_EMPRESA)
    .select(
        F.col("id_pessoa").cast("string").alias("id_pessoa"),
        F.col("nu_cpf_cnpj").cast("string").alias("nu_cpf_cnpj"),
        F.col("nome_profissional").cast("string").alias("nome_pessoa"),
        F.col("tipo_pessoa").cast("string").alias("tipo_pessoa"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_src"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_src")
    )
    .filter(F.col("id_pessoa").isNotNull() & F.col("nu_cpf_cnpj").isNotNull())
)

w_pessoa = Window.partitionBy("id_pessoa").orderBy(
    F.col("nome_pessoa").isNull().asc(),
    F.length(F.col("nome_pessoa")).desc_nulls_last(),
    F.col("ingested_at_src").desc_nulls_last()
)

df_nome = df_base.withColumn("rn", F.row_number().over(w_pessoa)).filter(F.col("rn") == 1)

df_meta = (
    df_base.groupBy("id_pessoa", "nu_cpf_cnpj")
    .agg(
        F.max("tipo_pessoa").alias("tipo_pessoa"),
        F.max("pipeline_run_id_src").alias("pipeline_run_id_origem"),
        F.max("ingested_at_src").alias("ingested_at_origem")
    )
)

df_final = (
    df_nome.alias("n")
    .join(df_meta.alias("f"), on=["id_pessoa", "nu_cpf_cnpj"], how="inner")
    .withColumn("juridica", F.col("f.tipo_pessoa") == F.lit("PJ"))
    .select(
        F.col("id_pessoa").cast("bigint").alias("id_pessoa"),
        F.col("nu_cpf_cnpj").cast("string").alias("nu_cpf_cnpj"),
        F.col("n.nome_pessoa").cast("string").alias("nome_pessoa"),
        F.col("f.tipo_pessoa").cast("string").alias("tipo_pessoa"),
        F.col("juridica").cast("boolean").alias("juridica"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().alias("ingested_at")
    )
)

write_overwrite(df_final, SILVER_TB_PESSOA, catalog_schema=f"{CATALOG}.{SILVER}")

