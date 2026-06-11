# Databricks notebook source
# silver: tb_cliente_referencia

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_CLIENTES = f"{CATALOG}.{BRONZE}.raw_clientes"
SILVER_TB_CLIENTE_REFERENCIA = f"{CATALOG}.{SILVER}.tb_cliente_referencia"

if not table_exists(BRONZE_RAW_CLIENTES):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_RAW_CLIENTES}")


def normalized_text_expr(col_name: str):
    return F.upper(
        F.trim(
            F.regexp_replace(
                F.coalesce(repair_text(col_name), F.lit("")),
                r"\s+",
                " ",
            )
        )
    )


def nullable_text_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt == "") | txt.isin("NULL", "N/A", "NAO INFORMADO", "-", "N/D"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def parse_date_expr(col_name: str):
    return F.to_date(F.col(col_name).cast("string"), "yyyy-MM-dd")


df_base = (
    spark.table(BRONZE_RAW_CLIENTES)
    .select(
        nullable_text_expr("consultoria_raw").alias("nm_consultoria"),
        nullable_text_expr("empresa_cliente_raw").alias("nm_cliente"),
        parse_date_expr("dt_inicio_raw").alias("dt_inicio"),
        parse_date_expr("dt_fim_raw").alias("dt_fim"),
        nullable_text_expr("tipo_contrato_raw").alias("tp_contrato"),
        nullable_text_expr("fontes_origem_raw").alias("fontes_origem"),
        nullable_text_expr("observacao_raw").alias("observacao"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
    )
    .filter(F.col("nm_cliente").isNotNull())
)

df_agg = (
    df_base
    .groupBy("nm_consultoria", "nm_cliente")
    .agg(
        F.min("dt_inicio").alias("dt_inicio"),
        F.max("dt_fim").alias("dt_fim"),
        F.concat_ws(" | ", F.sort_array(F.collect_set(F.col("tp_contrato")))).alias("tp_contrato"),
        F.concat_ws(" | ", F.sort_array(F.collect_set(F.col("fontes_origem")))).alias("fontes_origem"),
        F.first("observacao", ignorenulls=True).alias("observacao"),
        F.first("arquivo_origem", ignorenulls=True).alias("arquivo_origem"),
    )
    .withColumn(
        "fl_periodo_provisorio",
        F.when(
            F.upper(F.coalesce(F.col("observacao"), F.lit(""))).contains("PROVIS"),
            F.lit(1),
        ).otherwise(F.lit(0))
    )
)

df_final = (
    df_agg
    .withColumn(
        "cliente_referencia_id",
        F.row_number().over(
            Window.orderBy(
                F.coalesce(F.col("nm_consultoria"), F.lit("")),
                F.coalesce(F.col("nm_cliente"), F.lit("")),
                F.coalesce(F.col("dt_inicio"), F.to_date(F.lit("1900-01-01"))),
                F.coalesce(F.col("dt_fim"), F.to_date(F.lit("1900-01-01"))),
            )
        )
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("cliente_referencia_id").cast("int").alias("cliente_referencia_id"),
        F.col("nm_consultoria").cast("string").alias("nm_consultoria"),
        F.col("nm_cliente").cast("string").alias("nm_cliente"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.col("tp_contrato").cast("string").alias("tp_contrato"),
        F.col("fontes_origem").cast("string").alias("fontes_origem"),
        F.col("observacao").cast("string").alias("observacao"),
        F.col("fl_periodo_provisorio").cast("int").alias("fl_periodo_provisorio"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_CLIENTE_REFERENCIA, catalog_schema=f"{CATALOG}.{SILVER}")
