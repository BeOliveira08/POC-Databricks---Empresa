# Databricks notebook source
# gold: fato_imposto_serv

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_NOTAS_FISCAIS = f"{CATALOG}.{SILVER}.tb_notas_fiscais"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_FATO_ISS = f"{CATALOG}.{GOLD}.fato_imposto_serv"

if not table_exists(SILVER_TB_NOTAS_FISCAIS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_NOTAS_FISCAIS}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")

df_nf = spark.table(SILVER_TB_NOTAS_FISCAIS)
df_data_competencia = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_competencia"),
    F.col("dt_referencia").alias("competencia_ref"),
)
df_data_emissao = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_emissao"),
    F.col("dt_referencia").alias("data_emissao_ref"),
)

if table_exists(SILVER_TB_EMPRESA):
    df_empresa = spark.table(SILVER_TB_EMPRESA).select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
    )
else:
    df_empresa = spark.createDataFrame([], "empresa_id bigint, nu_cnpj string")

df_gold = (
    df_nf.alias("n")
    .join(df_data_competencia, F.col("n.competencia").cast("date") == F.col("competencia_ref"), "left")
    .join(df_data_emissao, F.col("n.data_emissao").cast("date") == F.col("data_emissao_ref"), "left")
    .join(df_empresa.alias("prest"), F.col("n.prestador_cnpj") == F.col("prest.nu_cnpj"), "left")
    .join(df_empresa.alias("tom"), F.col("n.tomador_cnpj") == F.col("tom.nu_cnpj"), "left")
    .withColumn(
        "valor_base_iss",
        F.round(F.coalesce(F.col("n.valor_servicos"), F.lit(0.0)) - F.coalesce(F.col("n.valor_deducoes"), F.lit(0.0)), 2),
    )
    .withColumn(
        "pc_iss_sobre_servicos",
        F.round(
            F.when(
                F.coalesce(F.col("n.valor_servicos"), F.lit(0.0)) > 0,
                (F.coalesce(F.col("n.valor_iss"), F.lit(0.0)) / F.col("n.valor_servicos")) * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
            4,
        ),
    )
    .withColumn(
        "pc_iss_sobre_base",
        F.round(
            F.when(
                F.col("valor_base_iss") > 0,
                (F.coalesce(F.col("n.valor_iss"), F.lit(0.0)) / F.col("valor_base_iss")) * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double")),
            4,
        ),
    )
    .select(
        F.col("n.id_nota_fiscal").cast("bigint").alias("id_nota_fiscal"),
        F.col("n.nf_numero").cast("int").alias("nf_numero"),
        F.col("n.id_externo").cast("string").alias("id_externo"),
        F.col("prest.empresa_id").cast("bigint").alias("prestador_empresa_id"),
        F.col("tom.empresa_id").cast("bigint").alias("tomador_empresa_id"),
        F.col("sk_data_competencia").cast("int").alias("sk_data_competencia"),
        F.col("sk_data_emissao").cast("int").alias("sk_data_emissao"),
        F.round(F.col("valor_base_iss"), 2).cast("double").alias("valor_base_iss"),
        F.round(F.col("n.valor_iss"), 2).cast("double").alias("valor_iss"),
        F.col("pc_iss_sobre_base").cast("double").alias("pc_iss_sobre_base"),
        F.col("n.fl_iss_retido").cast("boolean").alias("fl_iss_retido"),
        F.col("n.pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("n.ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

w_iss = Window.partitionBy("id_nota_fiscal").orderBy(
    F.col("ingested_at_origem").desc_nulls_last(),
    F.col("id_nota_fiscal").asc_nulls_last(),
)

df_gold = (
    df_gold
    .withColumn("rn_iss", F.row_number().over(w_iss))
    .filter(F.col("rn_iss") == 1)
    .drop("rn_iss")
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_ISS} (
    id_nota_fiscal BIGINT,
    nf_numero INT,
    id_externo STRING,
    prestador_empresa_id BIGINT,
    tomador_empresa_id BIGINT,
    sk_data_competencia INT,
    sk_data_emissao INT,
    valor_base_iss DOUBLE,
    valor_iss DOUBLE,
    pc_iss_sobre_base DOUBLE,
    fl_iss_retido BOOLEAN,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_ISS)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_ISS)
