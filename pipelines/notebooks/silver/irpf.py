# Databricks notebook source
# silver: tb_irpf

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

SILVER_TB_IRPF = f"{CATALOG}.{SILVER}.tb_irpf"
BRONZE_IRPF_DECL_TABLE = f"{CATALOG}.{BRONZE}.raw_irpf"
BRONZE_RAW_CTPS = f"{CATALOG}.{BRONZE}.raw_ctps"

if not table_exists(BRONZE_IRPF_DECL_TABLE):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_IRPF_DECL_TABLE}")


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


def cleaned_nome_expr(col_name: str):
    nome = normalized_text_expr(col_name)
    return F.when(
        (nome == "")
        | nome.isNull()
        | (nome == "DO DECLARANTE")
        | nome.startswith("IMPOSTO SOBRE A RENDA")
        | nome.contains("DECLARAC")
        | nome.contains("PESSOA F"),
        F.lit(None).cast("string"),
    ).otherwise(nome)


if table_exists(BRONZE_RAW_CTPS):
    w_nome_ref = Window.partitionBy("nu_cpf").orderBy(
        F.length(F.col("nome_ref")).desc_nulls_last(),
        F.col("nome_ref").asc_nulls_last(),
    )

    df_nome_ref = (
        spark.table(BRONZE_RAW_CTPS)
        .select(
            normalize_cpf_expr(only_digits_expr("nu_cpf")).alias("nu_cpf"),
            cleaned_nome_expr("nome").alias("nome_ref"),
        )
        .filter(F.col("nu_cpf").isNotNull() & F.col("nome_ref").isNotNull())
        .withColumn("rn", F.row_number().over(w_nome_ref))
        .filter(F.col("rn") == 1)
        .select("nu_cpf", "nome_ref")
    )
else:
    df_nome_ref = spark.createDataFrame([], "nu_cpf string, nome_ref string")

df_final = (
    spark.table(BRONZE_IRPF_DECL_TABLE)
    .select(
        normalize_cpf_expr(only_digits_expr("cpf")).alias("nu_cpf"),
        cleaned_nome_expr("nome").alias("nome"),
        F.col("ano_exercicio").cast("int").alias("ano_exercicio"),
        F.col("ano_calendario").cast("int").alias("ano_calendario"),
        normalized_text_expr("tipo_documento").alias("tipo_documento"),
        normalized_text_expr("tipo_arquivo").alias("tipo_arquivo"),
        normalized_text_expr("tipo_declaracao").alias("tipo_declaracao"),
        F.col("imposto_pago_total").cast("double").alias("imposto_pago_total"),
        F.col("imposto_restituir").cast("double").alias("imposto_restituir"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .join(df_nome_ref, on="nu_cpf", how="left")
    .withColumn("nome", F.coalesce(F.col("nome"), F.col("nome_ref")))
    .withColumn(
        "nome",
        F.when(F.col("nu_cpf") == F.lit("00000000000"), F.lit("TITULAR GENERICO")).otherwise(F.col("nome"))
    )
    .drop("nome_ref")
    .filter(F.col("nu_cpf").isNotNull() | F.col("arquivo_origem").isNotNull())
)

w = Window.partitionBy(
    F.coalesce(F.col("nu_cpf"), F.lit("")),
    F.coalesce(F.col("ano_exercicio").cast("string"), F.lit("")),
    F.coalesce(F.col("ano_calendario").cast("string"), F.lit("")),
).orderBy(
    F.when(F.col("tipo_arquivo") == "DEC", F.lit(0)).otherwise(F.lit(1)).asc(),
    F.col("tipo_declaracao").desc_nulls_last(),
    F.col("arquivo_origem").asc_nulls_last(),
)

w_id_irpf = Window.orderBy(
    F.coalesce(F.col("nu_cpf"), F.lit("")),
    F.coalesce(F.col("ano_exercicio").cast("string"), F.lit("")),
    F.coalesce(F.col("ano_calendario").cast("string"), F.lit("")),
)

df_final = (
    df_final
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("id_irpf", F.row_number().over(w_id_irpf))
    .withColumn(
        "tipo_declaracao",
        F.coalesce(F.col("tipo_declaracao"), F.lit("DECLARACAO DE AJUSTE ANUAL"))
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_irpf").cast("int").alias("id_irpf"),
        F.col("nu_cpf").cast("string").alias("nu_cpf"),
        F.col("nome").cast("string").alias("nome"),
        F.col("ano_exercicio").cast("int").alias("ano_exercicio"),
        F.col("ano_calendario").cast("int").alias("ano_calendario"),
        F.col("tipo_documento").cast("string").alias("tipo_documento"),
        F.col("tipo_arquivo").cast("string").alias("tipo_arquivo"),
        F.col("tipo_declaracao").cast("string").alias("tipo_declaracao"),
        F.col("imposto_pago_total").cast("double").alias("imposto_pago_total"),
        F.col("imposto_restituir").cast("double").alias("imposto_restituir"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_IRPF, catalog_schema=f"{CATALOG}.{SILVER}")
