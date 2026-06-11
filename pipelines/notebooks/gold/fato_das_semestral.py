# Databricks notebook source
# gold: fato_das_semestral

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_FATO_DAS = f"{CATALOG}.{GOLD}.fato_das"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_FATO_DAS_SEMESTRAL = f"{CATALOG}.{GOLD}.fato_das_semestral"

if not table_exists(GOLD_FATO_DAS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {GOLD_FATO_DAS}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")

df_data = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").cast("int").alias("sk_data_competencia"),
    F.col("nr_ano").cast("int").alias("nr_ano"),
    F.col("nr_semestre").cast("int").alias("nr_semestre"),
)

df_dim_semestre = (
    spark.table(GOLD_DIM_DATA)
    .filter(F.col("nr_dia_mes") == 1)
    .filter(F.col("nr_mes").isin(1, 7))
    .select(
        F.col("id_data").cast("int").alias("sk_data_semestre"),
        F.col("nr_ano").cast("int").alias("nr_ano_semestre"),
        F.col("nr_semestre").cast("int").alias("nr_semestre_semestre"),
    )
)

df_gold = (
    spark.table(GOLD_FATO_DAS)
    .filter(F.col("empresa_id").isNotNull() & F.col("sk_data_competencia").isNotNull())
    .join(df_data, on="sk_data_competencia", how="left")
    .join(
        df_dim_semestre,
        (F.col("nr_ano") == F.col("nr_ano_semestre"))
        & (F.col("nr_semestre") == F.col("nr_semestre_semestre")),
        "left",
    )
    .groupBy("empresa_id", "sk_data_semestre", "nr_ano", "nr_semestre")
    .agg(
        F.countDistinct("sk_das").alias("qt_guias_das"),
        F.round(F.sum(F.coalesce(F.col("vl_principal"), F.lit(0.0))), 2).alias("vl_principal_total"),
        F.round(F.sum(F.coalesce(F.col("vl_multa"), F.lit(0.0))), 2).alias("vl_multa_total"),
        F.round(F.sum(F.coalesce(F.col("vl_juros"), F.lit(0.0))), 2).alias("vl_juros_total"),
        F.round(F.sum(F.coalesce(F.col("vl_total"), F.lit(0.0))), 2).alias("vl_total_das"),
    )
    .withColumn(
        "sk_das_semestral",
        F.row_number().over(
            Window.orderBy(
                F.col("empresa_id").asc_nulls_last(),
                F.col("sk_data_semestre").asc_nulls_last(),
            )
        ).cast("bigint"),
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.col("sk_das_semestral").cast("bigint").alias("sk_das_semestral"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("sk_data_semestre").cast("int").alias("sk_data_semestre"),
        F.col("nr_ano").cast("int").alias("ano_ref"),
        F.col("nr_semestre").cast("int").alias("semestre_ref"),
        F.col("qt_guias_das").cast("bigint").alias("qt_guias_das"),
        F.col("vl_principal_total").cast("double").alias("vl_principal_total"),
        F.col("vl_multa_total").cast("double").alias("vl_multa_total"),
        F.col("vl_juros_total").cast("double").alias("vl_juros_total"),
        F.col("vl_total_das").cast("double").alias("vl_total_das"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_DAS_SEMESTRAL} (
    sk_das_semestral BIGINT,
    empresa_id BIGINT,
    sk_data_semestre INT,
    ano_ref INT,
    semestre_ref INT,
    qt_guias_das BIGINT,
    vl_principal_total DOUBLE,
    vl_multa_total DOUBLE,
    vl_juros_total DOUBLE,
    vl_total_das DOUBLE,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_DAS_SEMESTRAL)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_DAS_SEMESTRAL)
