# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------

# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

SILVER_STG_FGTS = f"{CATALOG}.{SILVER}.stg_fgts"
GOLD_FATO_FGTS = f"{CATALOG}.{GOLD}.fato_fgts"

# COMMAND ----------

# Calcula saldo_fgts como saldo do Ãºltimo lanÃ§amento do mÃªs (nÃ£o max, que ignora saques)
_w_last = Window.partitionBy("cpf", "pk_empresa", "competencia").orderBy(
    F.col("dt_lancamento").asc_nulls_last()
)

_df_saldo = (
    spark.table(SILVER_STG_FGTS)
    .withColumn("_last_saldo", F.last("saldo", ignorenulls=True).over(
        _w_last.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ))
)

df_gold = (
    _df_saldo
    .groupBy("cpf", "pk_empresa", "competencia")
    .agg(
        F.sum("valor").alias("valor_fgts"),
        F.first("_last_saldo").alias("saldo_fgts"),
        F.count("*").alias("qt_eventos"),
    )
    .withColumn(
        "sk_fgts",
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
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_fgts",
        "cpf",
        "pk_empresa",
        "competencia",
        "valor_fgts",
        "saldo_fgts",
        "qt_eventos",
        "pipeline_run_id",
        "ingested_at",
    )
)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_FGTS} (
    sk_fgts STRING,
    cpf STRING,
    pk_empresa STRING,
    competencia DATE,
    valor_fgts DOUBLE,
    saldo_fgts DOUBLE,
    qt_eventos BIGINT,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

write_overwrite(df_gold, GOLD_FATO_FGTS)

# COMMAND ----------
# MAGIC %run ../../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_FGTS)
