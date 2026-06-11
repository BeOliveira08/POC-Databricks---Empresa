# Databricks notebook source
# silver: tb_notas_fiscais

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

import re
from decimal import Decimal, InvalidOperation

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_NF = f"{CATALOG}.{BRONZE}.raw_nf"
SILVER_TB_NOTAS_FISCAIS = f"{CATALOG}.{SILVER}.tb_notas_fiscais"


def parse_nf_money_to_float(value):
    if value is None:
        return None

    raw = str(value).strip()
    if not raw or raw.upper() in {"NULL", "N/A", "NAO INFORMADO", "NÃO INFORMADO", "-"}:
        return None

    raw = raw.replace("\u00a0", " ").strip()

    # Excel numerics often arrive as strings like "14960.0".
    if re.fullmatch(r"-?\d+\.0+", raw):
        try:
            return float(Decimal(raw))
        except (InvalidOperation, ValueError):
            pass

    # Preserve standard decimal notation from spreadsheets such as "14960.50".
    if "," not in raw and re.fullmatch(r"-?\d+\.\d{1,2}", raw):
        try:
            return float(Decimal(raw))
        except (InvalidOperation, ValueError):
            pass

    return br_money_to_float(raw)


nf_money_udf = F.udf(parse_nf_money_to_float, "double")


def empty_nf_df():
    return empty_df_for_schema(
        """
        fonte string,
        nf_numero string,
        data_emissao string,
        tomador_nome string,
        tomador_cnpj string,
        prestador_cnpj string,
        valor_servicos string,
        valor_deducoes string,
        valor_iss string,
        iss_retido string,
        nf_paga string,
        numero_guia string,
        id_externo string,
        arquivo_origem string
        """
    )


def read_nf(table_name: str):
    if not table_exists(table_name):
        return empty_nf_df()
    return spark.table(table_name)


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
        (txt == "") | txt.isNull() | txt.isin("NAO INFORMADO", "NÃƒO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def nullable_numeric_text_expr(col_name: str):
    txt = F.trim(
        F.regexp_replace(
            F.coalesce(repair_text(col_name), F.lit("")),
            r"\.0+$",
            "",
        )
    )
    return F.when(
        (txt == "") | F.upper(txt).isin("NAO INFORMADO", "NÃƒO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def bool_flag_expr(col_name: str):
    txt = F.lower(F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit(""))))
    return F.when(txt.isin("sim", "s", "true", "1", "yes", "y"), F.lit(True)).otherwise(F.lit(False))


df_final = (
    read_nf(BRONZE_RAW_NF)
    .select(
        nullable_text_expr("fonte").alias("fonte"),
        F.regexp_extract(F.coalesce(F.col("nf_numero").cast("string"), F.lit("")), r"(\d+)", 1).cast("int").alias("nf_numero"),
        F.col("data_emissao").cast("string").alias("data_emissao"),
        nullable_text_expr("tomador_nome").alias("tomador_nome"),
        normalize_cnpj_expr(only_digits_expr("tomador_cnpj")).alias("tomador_cnpj"),
        normalize_cnpj_expr(only_digits_expr("prestador_cnpj")).alias("prestador_cnpj"),
        nf_money_udf("valor_servicos").alias("valor_servicos"),
        nf_money_udf("valor_deducoes").alias("valor_deducoes"),
        nf_money_udf("valor_iss").alias("valor_iss"),
        bool_flag_expr("iss_retido").alias("fl_iss_retido"),
        bool_flag_expr("nf_paga").alias("fl_nf_paga"),
        nullable_numeric_text_expr("numero_guia").alias("numero_guia"),
        nullable_numeric_text_expr("id_externo").alias("id_externo"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .withColumn(
        "data_emissao",
        F.coalesce(
            F.expr("try_to_date(data_emissao, 'dd/MM/yyyy')"),
            F.expr("try_to_date(data_emissao, 'yyyy-MM-dd')"),
            F.to_date(F.expr("try_to_timestamp(data_emissao, 'yyyy-MM-dd HH:mm:ss')")),
            F.to_date(F.expr("try_to_timestamp(data_emissao)")),
        ),
    )
    .withColumn("competencia", F.trunc(F.col("data_emissao"), "month"))
    .withColumn("valor_liquido", F.col("valor_servicos") - F.coalesce(F.col("valor_deducoes"), F.lit(0.0)))
    .filter(F.col("nf_numero").isNotNull())
    .filter(F.col("prestador_cnpj").isNotNull())
)

w = Window.partitionBy(
    "fonte",
    "prestador_cnpj",
    "nf_numero",
    F.coalesce(F.col("id_externo"), F.lit("")),
).orderBy(
    F.col("data_emissao").desc_nulls_last(),
    F.col("ingested_at_origem").desc_nulls_last(),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_id_nf = Window.orderBy(
    F.coalesce(F.col("fonte"), F.lit("")),
    F.coalesce(F.col("prestador_cnpj"), F.lit("")),
    F.coalesce(F.col("nf_numero").cast("string"), F.lit("")),
    F.coalesce(F.col("data_emissao").cast("string"), F.lit("")),
)

df_final = (
    df_final
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("id_nota_fiscal", F.row_number().over(w_id_nf))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_nota_fiscal").cast("int").alias("id_nota_fiscal"),
        F.col("fonte").cast("string").alias("fonte"),
        F.col("nf_numero").cast("int").alias("nf_numero"),
        F.col("competencia").cast("date").alias("competencia"),
        F.col("data_emissao").cast("date").alias("data_emissao"),
        F.col("tomador_nome").cast("string").alias("tomador_nome"),
        F.col("tomador_cnpj").cast("string").alias("tomador_cnpj"),
        F.col("prestador_cnpj").cast("string").alias("prestador_cnpj"),
        F.col("valor_servicos").cast("double").alias("valor_servicos"),
        F.col("valor_deducoes").cast("double").alias("valor_deducoes"),
        F.col("valor_iss").cast("double").alias("valor_iss"),
        F.col("valor_liquido").cast("double").alias("valor_liquido"),
        F.col("fl_iss_retido").cast("boolean").alias("fl_iss_retido"),
        F.col("fl_nf_paga").cast("boolean").alias("fl_nf_paga"),
        F.col("numero_guia").cast("string").alias("numero_guia"),
        F.col("id_externo").cast("string").alias("id_externo"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_NOTAS_FISCAIS, catalog_schema=f"{CATALOG}.{SILVER}")
