# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------

# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SILVER_STG_INSS = f"{CATALOG}.{SILVER}.stg_inss"
GOLD_FATO_INSS = f"{CATALOG}.{GOLD}.fato_inss"

# COMMAND ----------

df_gold = (
    spark.table(SILVER_STG_INSS)
    .withColumn(
        "sk_inss",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("cpf"), F.lit("")),
                F.coalesce(F.col("pk_empresa"), F.lit("")),
                F.coalesce(F.col("competencia").cast("string"), F.lit("")),
            ),
            256,
        ),
    )
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_inss",
        "cpf",
        "pk_empresa",
        "competencia",
        "remuneracao_valor",
        "nit",
        "nome",
        "dt_nascimento",
        "nome_mae",
        "pipeline_run_id",
        "ingested_at",
    )
)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_INSS} (
    sk_inss STRING,
    cpf STRING,
    pk_empresa STRING,
    competencia DATE,
    remuneracao_valor DOUBLE,
    nit STRING,
    nome STRING,
    dt_nascimento DATE,
    nome_mae STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

write_overwrite(df_gold, GOLD_FATO_INSS)

# COMMAND ----------
# MAGIC %run ../../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_INSS)
