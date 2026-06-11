# Databricks notebook source
# silver: tb_fgts

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------
# MAGIC %run ../../utils/fgts

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_FGTS = f"{CATALOG}.{BRONZE}.raw_fgts"
SILVER_TB_FGTS = f"{CATALOG}.{SILVER}.tb_fgts"
money_udf = F.udf(br_money_to_float, "double")
date_udf = F.udf(parse_flexible_date_to_iso, "string")
classify_event_udf = F.udf(classify_event, "string")


def empty_fgts_df():
    return empty_df_for_schema(
        """
        id_fgts int,
        nome string,
        nr_pis_pasep string,
        nr_cnpj_contratante string,
        nr_conta_fgts string,
        nome_empresa string,
        dt_lancamento date,
        competencia date,
        descricao string,
        tp_evento string,
        valor double,
        saldo double,
        arquivo_origem string,
        ingested_at_origem timestamp,
        pipeline_run_id string,
        ingested_at timestamp
        """
    )


def nullable_text_expr(col_name: str):
    txt = norm_text(col_name)
    return F.when(
        txt.isNull() | txt.isin("NAO INFORMADO", "NÃO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def parse_date_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")

if not table_exists(BRONZE_FGTS):
    df_final = empty_fgts_df()
else:
    df_final = (
        spark.table(BRONZE_FGTS)
        .select(
            nullable_text_expr("nome").alias("nome"),
            only_digits_expr("pis_pasep").alias("nr_pis_pasep"),
            normalize_cnpj_expr(only_digits_expr("cnpj")).alias("nr_cnpj_contratante"),
            only_digits_expr("conta_fgts").alias("nr_conta_fgts"),
            nullable_text_expr("empresa").alias("nome_empresa"),
            parse_date_expr("dt_lancamento").alias("dt_lancamento"),
            F.trunc(parse_date_expr("dt_lancamento"), "month").alias("competencia"),
            nullable_text_expr("descricao").alias("descricao"),
            classify_event_udf("descricao").alias("tp_evento"),
            money_udf("valor").alias("valor"),
            money_udf("saldo").alias("saldo"),
            F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        )
        .filter(F.col("dt_lancamento").isNotNull())
        .filter(F.col("descricao").isNotNull())
    )

if table_exists(BRONZE_FGTS):
    w = Window.partitionBy(
        F.coalesce(F.col("nr_pis_pasep"), F.col("nome"), F.lit("")),
        F.coalesce(F.col("nr_cnpj_contratante"), F.col("nome_empresa"), F.lit("")),
        F.coalesce(F.col("dt_lancamento").cast("string"), F.lit("")),
        F.coalesce(F.col("descricao"), F.lit("")),
        F.coalesce(F.col("valor").cast("string"), F.lit("")),
    ).orderBy(
        F.col("saldo").desc_nulls_last(),
        F.col("ingested_at_origem").desc_nulls_last(),
        F.col("arquivo_origem").desc_nulls_last(),
    )

    w_id_fgts = Window.orderBy(
        F.coalesce(F.col("nr_pis_pasep"), F.lit("")),
        F.coalesce(F.col("nome"), F.lit("")),
        F.coalesce(F.col("nr_cnpj_contratante"), F.lit("")),
        F.coalesce(F.col("dt_lancamento").cast("string"), F.lit("")),
        F.coalesce(F.col("descricao"), F.lit("")),
        F.coalesce(F.col("valor").cast("string"), F.lit("")),
    )

    df_final = (
        df_final
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("id_fgts", F.row_number().over(w_id_fgts))
        .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
        .withColumn("ingested_at", F.current_timestamp())
        .select(
            F.col("id_fgts").cast("int").alias("id_fgts"),
            F.col("nome").cast("string").alias("nome"),
            F.col("nr_pis_pasep").cast("string").alias("nr_pis_pasep"),
            F.col("nr_cnpj_contratante").cast("string").alias("nr_cnpj_contratante"),
            F.col("nr_conta_fgts").cast("string").alias("nr_conta_fgts"),
            F.col("nome_empresa").cast("string").alias("nome_empresa"),
            F.col("dt_lancamento").cast("date").alias("dt_lancamento"),
            F.col("competencia").cast("date").alias("competencia"),
            F.col("descricao").cast("string").alias("descricao"),
            F.col("tp_evento").cast("string").alias("tp_evento"),
            F.col("valor").cast("double").alias("valor"),
            F.col("saldo").cast("double").alias("saldo"),
            F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
            F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        )
    )

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_FGTS, catalog_schema=f"{CATALOG}.{SILVER}")
