# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------
# MAGIC %run ../../../utils/transforms

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

BRONZE_FGTS              = f"{CATALOG}.{BRONZE}.fgts_extratos"
GOLD_DIM_EMPRESA         = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_FATO_FGTS_DASHBOARD = f"{CATALOG}.{GOLD}.fato_fgts_dashboard"

# COMMAND ----------


# COMMAND ----------

if table_exists(BRONZE_FGTS):
    df_fgts = (
        spark.table(BRONZE_FGTS)
        .select(
            only_digits_expr("cpf").alias("cpf"),
            F.col("pk_empresa").cast("string").alias("pk_empresa"),
            truncate_to_month(F.col("competencia")).alias("competencia"),
            F.to_date(F.col("dt_lancamento")).alias("dt_lancamento"),
            F.lower(F.trim(F.coalesce(F.col("event_type"), F.lit("outros")))).alias("event_type"),
            F.col("valor").cast("double").alias("valor"),
            F.col("saldo").cast("double").alias("saldo"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id")
        )
        .filter(F.col("cpf") != "")
        .filter(F.col("pk_empresa").isNotNull())
        .filter(F.col("competencia").isNotNull())
    )
else:
    df_fgts = empty_df_for_schema(
        """
        cpf STRING,
        pk_empresa STRING,
        competencia DATE,
        dt_lancamento DATE,
        event_type STRING,
        valor DOUBLE,
        saldo DOUBLE,
        pipeline_run_id STRING
        """
    )

# COMMAND ----------

if table_exists(GOLD_DIM_EMPRESA):
    df_empresa = (
        spark.table(GOLD_DIM_EMPRESA)
        .select(
            F.col("pk_empresa").cast("string").alias("pk_empresa"),
            F.col("id_empresa").cast("long").alias("id_empresa"),
            F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
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
        "pk_empresa STRING, id_empresa LONG, nu_cnpj STRING, nome_empresa STRING"
    )

# COMMAND ----------

_w_last = Window.partitionBy("cpf", "pk_empresa", "competencia").orderBy(
    F.col("dt_lancamento").asc_nulls_last()
)

df_enriched = (
    df_fgts
    .withColumn(
        "_saldo_final_mes",
        F.last("saldo", ignorenulls=True).over(
            _w_last.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
    )
)

df_gold = (
    df_enriched
    .groupBy("cpf", "pk_empresa", "competencia")
    .agg(
        F.sum(
            F.when(F.col("event_type") == "deposito", F.coalesce(F.col("valor"), F.lit(0.0))).otherwise(F.lit(0.0))
        ).alias("vl_deposito"),
        F.sum(
            F.when(F.col("event_type").isin("rendimento_jam", "jam", "rendimento"), F.coalesce(F.col("valor"), F.lit(0.0))).otherwise(F.lit(0.0))
        ).alias("vl_rentabilidade"),
        F.sum(
            F.when(
                F.col("event_type").isin("saque", "rescisao", "multa_rescisoria"),
                F.coalesce(F.col("valor"), F.lit(0.0))
            ).otherwise(F.lit(0.0))
        ).alias("vl_saque"),
        F.sum(
            F.when(
                ~F.col("event_type").isin("deposito", "rendimento_jam", "jam", "rendimento", "saque", "rescisao", "multa_rescisoria"),
                F.coalesce(F.col("valor"), F.lit(0.0))
            ).otherwise(F.lit(0.0))
        ).alias("vl_outros"),
        F.first("_saldo_final_mes", ignorenulls=True).alias("saldo_atual"),
        F.count("*").alias("qt_eventos"),
        F.max("pipeline_run_id").alias("pipeline_run_id")
    )
    .join(df_empresa, on="pk_empresa", how="left")
    .withColumn(
        "sk_fgts_dashboard",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("cpf"), F.lit("")),
                F.coalesce(F.col("pk_empresa"), F.lit("")),
                F.coalesce(F.col("competencia").cast("string"), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "saldo_anterior",
        F.round(
            F.coalesce(F.col("saldo_atual"), F.lit(0.0))
            - F.coalesce(F.col("vl_deposito"), F.lit(0.0))
            - F.coalesce(F.col("vl_rentabilidade"), F.lit(0.0))
            - F.coalesce(F.col("vl_outros"), F.lit(0.0))
            - F.coalesce(F.col("vl_saque"), F.lit(0.0)),
            2
        )
    )
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_fgts_dashboard",
        "cpf",
        "id_empresa",
        "pk_empresa",
        "nu_cnpj",
        "nome_empresa",
        "competencia",
        F.round("saldo_anterior", 2).alias("saldo_anterior"),
        F.round("vl_deposito", 2).alias("vl_deposito"),
        F.round("vl_rentabilidade", 2).alias("vl_rentabilidade"),
        F.round("vl_saque", 2).alias("vl_saque"),
        F.round("vl_outros", 2).alias("vl_outros"),
        F.round("saldo_atual", 2).alias("saldo_atual"),
        "qt_eventos",
        "pipeline_run_id",
        "ingested_at"
    )
)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_FGTS_DASHBOARD} (
    sk_fgts_dashboard STRING,
    cpf STRING,
    id_empresa BIGINT,
    pk_empresa STRING,
    nu_cnpj STRING,
    nome_empresa STRING,
    competencia DATE,
    saldo_anterior DOUBLE,
    vl_deposito DOUBLE,
    vl_rentabilidade DOUBLE,
    vl_saque DOUBLE,
    vl_outros DOUBLE,
    saldo_atual DOUBLE,
    qt_eventos BIGINT,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

write_overwrite(df_gold, GOLD_FATO_FGTS_DASHBOARD)

