# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------

# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SILVER_STG_DAS = f"{CATALOG}.{SILVER}.stg_das"
GOLD_FATO_DAS = f"{CATALOG}.{GOLD}.fato_das"

# COMMAND ----------

df_gold = (
    spark.table(SILVER_STG_DAS)
    .withColumn(
        "sk_das",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("cnpj"), F.lit("")),
                F.coalesce(F.col("competencia").cast("string"), F.lit("")),
                F.coalesce(F.col("nr_documento"), F.lit("")),
                F.coalesce(F.col("tp_lancamento"), F.lit("")),
            ),
            256,
        ),
    )
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_das",
        "cnpj",
        "razao_social",
        "competencia",
        "nr_documento",
        "tp_lancamento",
        "dt_vencimento",
        "dt_arrecadacao",
        "vl_principal",
        "vl_multa",
        "vl_juros",
        "vl_total",
        "pipeline_run_id",
        "ingested_at",
    )
)


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_DAS} (
    sk_das STRING,
    cnpj STRING,
    razao_social STRING,
    competencia DATE,
    nr_documento STRING,
    tp_lancamento STRING,
    dt_vencimento DATE,
    dt_arrecadacao DATE,
    vl_principal DOUBLE,
    vl_multa DOUBLE,
    vl_juros DOUBLE,
    vl_total DOUBLE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

write_overwrite(df_gold, GOLD_FATO_DAS)

# COMMAND ----------
# MAGIC %run ../../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_DAS)
