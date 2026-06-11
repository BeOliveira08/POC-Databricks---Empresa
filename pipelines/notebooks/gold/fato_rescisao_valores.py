# Databricks notebook source
# gold: fato_rescisao_valores

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TB_RESCISOES = f"{CATALOG}.{SILVER}.tb_rescisoes"
GOLD_DIM_RESCISAO = f"{CATALOG}.{GOLD}.dim_rescisao"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_FATO_RESCISAO_VALORES = f"{CATALOG}.{GOLD}.fato_rescisao_valores"

for required in [SILVER_TB_RESCISOES, GOLD_DIM_RESCISAO, GOLD_DIM_PESSOA, GOLD_DIM_DATA]:
    if not table_exists(required):
        raise ValueError(f"Tabela obrigatoria nao encontrada: {required}")

df_resc_docs = (
    spark.table(SILVER_TB_RESCISOES)
    .filter(F.upper(F.coalesce(F.col("tipo_vinculo").cast("string"), F.lit(""))) == F.lit("CLT"))
    .select(
        F.regexp_replace(F.col("cpf").cast("string"), r"[^0-9]", "").alias("cpf"),
        F.regexp_replace(F.col("cnpj").cast("string"), r"[^0-9]", "").alias("cnpj"),
        F.col("dt_rescisao").cast("date").alias("dt_rescisao"),
        F.col("vl_rescisao_bruta").cast("double").alias("vl_rescisao_bruta_doc"),
        F.col("vl_rescisao_liquida").cast("double").alias("vl_rescisao_liquida_doc"),
        F.col("vl_multa_rescisoria").cast("double").alias("vl_multa_rescisoria_doc"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .filter(F.col("dt_rescisao").isNotNull())
)

df_dim_resc = spark.table(GOLD_DIM_RESCISAO).select(
    F.col("id_rescisao").cast("bigint").alias("id_rescisao"),
    F.col("empresa_id").cast("bigint").alias("empresa_id"),
    F.regexp_replace(F.col("cpf").cast("string"), r"[^0-9]", "").alias("cpf"),
    F.regexp_replace(F.col("cnpj").cast("string"), r"[^0-9]", "").alias("cnpj"),
    F.col("dt_rescisao").cast("date").alias("dt_rescisao"),
    F.col("qt_documentos_origem").cast("int").alias("qt_documentos_origem"),
)

df_pessoa = spark.table(GOLD_DIM_PESSOA).select(
    F.col("id_pessoa").cast("bigint").alias("sk_pessoa"),
)

df_data = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").cast("int").alias("sk_data_rescisao"),
    F.col("dt_referencia").cast("date").alias("dt_data_dim"),
)

df_vals = (
    df_resc_docs.groupBy("cpf", "cnpj", "dt_rescisao")
    .agg(
        F.max("vl_rescisao_bruta_doc").alias("vl_rescisao_bruta"),
        F.max("vl_rescisao_liquida_doc").alias("vl_rescisao_liquida"),
        F.max("vl_multa_rescisoria_doc").alias("vl_multa_rescisoria"),
        F.max("pipeline_run_id_origem").alias("pipeline_run_id_origem"),
        F.max("ingested_at_origem").alias("ingested_at_origem"),
    )
)

df_gold = (
    df_dim_resc.alias("d")
    .join(
        df_vals.alias("v"),
        [
            F.col("d.cpf") == F.col("v.cpf"),
            F.col("d.cnpj") == F.col("v.cnpj"),
            F.col("d.dt_rescisao") == F.col("v.dt_rescisao"),
        ],
        "left",
    )
    .join(
        df_pessoa.alias("p"),
        F.col("d.cpf").cast("bigint") == F.col("p.sk_pessoa"),
        "left",
    )
    .join(
        df_data.alias("dt"),
        F.col("d.dt_rescisao") == F.col("dt.dt_data_dim"),
        "left",
    )
    .withColumn("sk_rescisao", F.col("d.id_rescisao").cast("bigint"))
    .withColumn(
        "vl_total_recebido_calculado",
        F.coalesce(F.col("v.vl_rescisao_liquida"), F.col("v.vl_rescisao_bruta"), F.lit(0.0))
        + F.coalesce(F.col("v.vl_multa_rescisoria"), F.lit(0.0)),
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("sk_rescisao").cast("bigint").alias("sk_rescisao"),
        F.col("p.sk_pessoa").cast("bigint").alias("sk_pessoa"),
        F.col("d.empresa_id").cast("bigint").alias("empresa_id"),
        F.col("dt.sk_data_rescisao").cast("int").alias("sk_data_rescisao"),
        F.col("v.vl_rescisao_bruta").cast("double").alias("vl_rescisao_bruta"),
        F.col("v.vl_rescisao_liquida").cast("double").alias("vl_rescisao_liquida"),
        F.col("v.vl_multa_rescisoria").cast("double").alias("vl_multa_rescisoria"),
        F.col("vl_total_recebido_calculado").cast("double").alias("vl_total_recebido_calculado"),
        F.col("d.qt_documentos_origem").cast("int").alias("qt_documentos_origem"),
        F.col("v.pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("v.ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
    .filter(
        F.col("sk_rescisao").isNotNull()
        & F.col("sk_pessoa").isNotNull()
        & F.col("empresa_id").isNotNull()
        & F.col("sk_data_rescisao").isNotNull()
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_RESCISAO_VALORES} (
    sk_rescisao BIGINT,
    sk_pessoa BIGINT,
    empresa_id BIGINT,
    sk_data_rescisao INT,
    vl_rescisao_bruta DOUBLE,
    vl_rescisao_liquida DOUBLE,
    vl_multa_rescisoria DOUBLE,
    vl_total_recebido_calculado DOUBLE,
    qt_documentos_origem INT,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_RESCISAO_VALORES)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_RESCISAO_VALORES)
