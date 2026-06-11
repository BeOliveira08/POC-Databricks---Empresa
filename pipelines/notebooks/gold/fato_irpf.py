# Databricks notebook source
# gold: fato_irpf

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../config/domain_config

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TB_IRPF = f"{CATALOG}.{SILVER}.tb_irpf"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_FATO_IRPF = f"{CATALOG}.{GOLD}.fato_irpf"


def pessoa_nome_key(col_name: str):
    return F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", "")

if not table_exists(SILVER_TB_IRPF):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_IRPF}")
if not table_exists(GOLD_DIM_PESSOA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_PESSOA}")

df_pessoa = spark.table(GOLD_DIM_PESSOA).select(
    F.col("id_pessoa").cast("bigint").alias("id_pessoa"),
)
df_pessoa_irpf = (
    df_pessoa
    .filter(F.col("id_pessoa") == F.regexp_replace(F.lit(TITULAR_CPF), r"[^0-9]", "").cast("bigint"))
    .select(
        F.col("id_pessoa").cast("bigint").alias("sk_pessoa_irpf"),
        F.lit(TITULAR_CPF).cast("string").alias("nu_cpf_irpf"),
    )
)
df_pessoa_nome_match = (
    spark.table(GOLD_DIM_PESSOA)
    .select(
        F.col("id_pessoa").cast("bigint").alias("sk_pessoa"),
        F.explode(
            F.array_distinct(
                F.array(
                    pessoa_nome_key("nome_pessoa"),
                )
            )
        ).alias("pessoa_nome_key_match"),
    )
    .filter(F.col("pessoa_nome_key_match") != "")
    .groupBy("pessoa_nome_key_match")
    .agg(
        F.countDistinct("sk_pessoa").alias("qtd_pessoa_match"),
        F.max("sk_pessoa").cast("bigint").alias("sk_pessoa_by_nome"),
    )
    .filter(F.col("qtd_pessoa_match") == 1)
    .drop("qtd_pessoa_match")
)

df_gold = (
    spark.table(SILVER_TB_IRPF)
    .withColumn("nome_pessoa_key", pessoa_nome_key("nome"))
    .withColumn("nu_cpf_irpf", F.lit(TITULAR_CPF).cast("string"))
    .join(df_pessoa_irpf, on="nu_cpf_irpf", how="left")
    .join(
        df_pessoa_nome_match,
        F.col("nome_pessoa_key") == F.col("pessoa_nome_key_match"),
        "left",
    )
    .withColumn("sk_pessoa_resolvida", F.coalesce(F.col("sk_pessoa_irpf"), F.col("sk_pessoa_by_nome")))
    .withColumn(
        "sk_irpf",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("nu_cpf"), F.lit("")),
                F.coalesce(F.col("ano_exercicio").cast("string"), F.lit("")),
                F.coalesce(F.col("ano_calendario"), F.lit("")),
            ),
            256,
        ),
    )
    .select(
        F.col("sk_irpf").cast("string").alias("sk_irpf"),
        F.col("id_irpf").cast("bigint").alias("id_irpf"),
        F.col("sk_pessoa_resolvida").cast("bigint").alias("sk_pessoa"),
        F.col("ano_exercicio").cast("int").alias("exercicio"),
        F.col("ano_calendario").cast("int").alias("ano_calendario"),
        F.col("imposto_pago_total").cast("double").alias("imposto_pago_total"),
        F.col("imposto_restituir").cast("double").alias("imposto_restituir"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_IRPF} (
    sk_irpf STRING,
    id_irpf BIGINT,
    sk_pessoa BIGINT,
    exercicio INT,
    ano_calendario INT,
    imposto_pago_total DOUBLE,
    imposto_restituir DOUBLE,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_IRPF)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_IRPF)
