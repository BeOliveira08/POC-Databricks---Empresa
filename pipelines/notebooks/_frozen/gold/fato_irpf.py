# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------

# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SILVER_STG_IRPF = f"{CATALOG}.{SILVER}.stg_irpf"
GOLD_FATO_IRPF = f"{CATALOG}.{GOLD}.fato_irpf"

# COMMAND ----------

df_gold = (
    spark.table(SILVER_STG_IRPF)
    .withColumn(
        "sk_irpf",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("cpf"), F.lit("")),
                F.coalesce(F.col("exercicio"), F.lit("")),
                F.coalesce(F.col("ano_calendario"), F.lit("")),
            ),
            256,
        ),
    )
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_irpf",
        "cpf",
        "nome",
        "exercicio",
        "ano_calendario",
        "tipo_declaracao",
        "rend_tributavel_total",
        "ded_total",
        "base_calculo",
        "aliquota_efetiva_pct",
        "imposto_devido",
        "imposto_pago_total",
        "imposto_restituir",
        "imposto_a_pagar",
        "patrimonio_liquido_anterior",
        "patrimonio_liquido_atual",
        "pipeline_run_id",
        "ingested_at",
    )
)


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_IRPF} (
    sk_irpf STRING,
    cpf STRING,
    nome STRING,
    exercicio STRING,
    ano_calendario STRING,
    tipo_declaracao STRING,
    rend_tributavel_total DOUBLE,
    ded_total DOUBLE,
    base_calculo DOUBLE,
    aliquota_efetiva_pct DOUBLE,
    imposto_devido DOUBLE,
    imposto_pago_total DOUBLE,
    imposto_restituir DOUBLE,
    imposto_a_pagar DOUBLE,
    patrimonio_liquido_anterior DOUBLE,
    patrimonio_liquido_atual DOUBLE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

write_overwrite(df_gold, GOLD_FATO_IRPF)

# COMMAND ----------
# MAGIC %run ../../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_IRPF)
