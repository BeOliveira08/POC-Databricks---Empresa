# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------
# MAGIC %run ../../../utils/transforms

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

BRONZE_IRPF_FONTES            = f"{CATALOG}.{BRONZE}.irpf_fontes_pagadoras"
GOLD_DIM_EMPRESA              = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_FATO_IRPF_FONTE_PAGADORA = f"{CATALOG}.{GOLD}.fato_irpf_fonte_pagadora"

# COMMAND ----------


# COMMAND ----------

if table_exists(BRONZE_IRPF_FONTES):
    df_fontes = (
        spark.table(BRONZE_IRPF_FONTES)
        .select(
            F.col("arquivo").cast("string").alias("arquivo"),
            only_digits_expr("cpf_titular").alias("cpf"),
            F.col("exercicio").cast("string").alias("exercicio"),
            F.col("ano_calendario").cast("string").alias("ano_calendario"),
            F.col("id_empresa").cast("long").alias("id_empresa"),
            F.col("pk_empresa").cast("string").alias("pk_empresa"),
            only_digits_expr("cnpj_fonte").alias("cnpj_fonte"),
            F.upper(F.trim(F.col("nome_fonte_raw"))).alias("nome_fonte_raw"),
            F.col("rend_recebido").cast("double").alias("rend_recebido"),
            F.col("inss_retido").cast("double").alias("inss_retido"),
            F.col("irrf_retido").cast("double").alias("irrf_retido"),
            F.col("decimo_terceiro").cast("double").alias("decimo_terceiro"),
            F.col("irrf_13").cast("double").alias("irrf_13"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id")
        )
        .filter(F.col("cpf") != "")
    )
else:
    df_fontes = empty_df_for_schema(
        """
        arquivo STRING,
        cpf STRING,
        exercicio STRING,
        ano_calendario STRING,
        id_empresa LONG,
        pk_empresa STRING,
        cnpj_fonte STRING,
        nome_fonte_raw STRING,
        rend_recebido DOUBLE,
        inss_retido DOUBLE,
        irrf_retido DOUBLE,
        decimo_terceiro DOUBLE,
        irrf_13 DOUBLE,
        pipeline_run_id STRING
        """
    )

# COMMAND ----------

if table_exists(GOLD_DIM_EMPRESA):
    df_empresa = (
        spark.table(GOLD_DIM_EMPRESA)
        .select(
            F.col("pk_empresa").cast("string").alias("pk_empresa"),
            F.col("id_empresa").cast("long").alias("id_empresa_dim"),
            only_digits_expr("nu_cnpj").alias("nu_cnpj"),
            F.coalesce(
                F.col("nome_legal_canonico"),
                F.col("nome_operacional"),
                F.col("nome_normalizado")
            ).cast("string").alias("nome_empresa")
        )
        .dropDuplicates(["pk_empresa"])
    )
else:
    df_empresa = empty_df_for_schema(
        "pk_empresa STRING, id_empresa_dim LONG, nu_cnpj STRING, nome_empresa STRING"
    )

# COMMAND ----------

df_gold = (
    df_fontes
    .join(df_empresa, on="pk_empresa", how="left")
    .withColumn("id_empresa", F.coalesce(F.col("id_empresa"), F.col("id_empresa_dim")).cast("long"))
    .withColumn(
        "sk_irpf_fonte_pagadora",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("cpf"), F.lit("")),
                F.coalesce(F.col("exercicio"), F.lit("")),
                F.coalesce(F.col("cnpj_fonte"), F.lit("")),
                F.coalesce(F.col("nome_fonte_raw"), F.lit(""))
            ),
            256
        )
    )
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_irpf_fonte_pagadora",
        "arquivo",
        "cpf",
        "exercicio",
        "ano_calendario",
        "id_empresa",
        "pk_empresa",
        F.coalesce(F.col("nu_cnpj"), F.col("cnpj_fonte")).alias("nu_cnpj"),
        "nome_empresa",
        "nome_fonte_raw",
        F.round("rend_recebido", 2).alias("rend_recebido"),
        F.round("inss_retido", 2).alias("inss_retido"),
        F.round("irrf_retido", 2).alias("irrf_retido"),
        F.round("decimo_terceiro", 2).alias("decimo_terceiro"),
        F.round("irrf_13", 2).alias("irrf_13"),
        "pipeline_run_id",
        "ingested_at"
    )
)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_IRPF_FONTE_PAGADORA} (
    sk_irpf_fonte_pagadora STRING,
    arquivo STRING,
    cpf STRING,
    exercicio STRING,
    ano_calendario STRING,
    id_empresa BIGINT,
    pk_empresa STRING,
    nu_cnpj STRING,
    nome_empresa STRING,
    nome_fonte_raw STRING,
    rend_recebido DOUBLE,
    inss_retido DOUBLE,
    irrf_retido DOUBLE,
    decimo_terceiro DOUBLE,
    irrf_13 DOUBLE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

write_overwrite(df_gold, GOLD_FATO_IRPF_FONTE_PAGADORA)

