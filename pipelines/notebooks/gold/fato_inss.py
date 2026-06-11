# Databricks notebook source
# gold: fato_inss

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../config/domain_config

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_INSS = f"{CATALOG}.{SILVER}.tb_inss"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"
GOLD_BRIDGE_VINCULO_MES = f"{CATALOG}.{GOLD}.bridge_vinculo_mes"
GOLD_FATO_INSS = f"{CATALOG}.{GOLD}.fato_inss"


def empresa_nome_key(col_name: str):
    return F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", "")


if not table_exists(SILVER_TB_INSS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_INSS}")
for dim_table in [GOLD_DIM_PESSOA, GOLD_DIM_DATA]:
    if not table_exists(dim_table):
        raise ValueError(f"Dimensao obrigatoria nao encontrada: {dim_table}")

df_inss = spark.table(SILVER_TB_INSS)
df_pessoa = spark.table(GOLD_DIM_PESSOA).select(
    F.col("id_pessoa").cast("bigint").alias("id_pessoa"),
)
df_pessoa_inss = (
    df_pessoa
    .filter(F.col("id_pessoa") == F.regexp_replace(F.lit(TITULAR_CPF), r"[^0-9]", "").cast("bigint"))
    .select(
        F.col("id_pessoa").cast("bigint").alias("sk_pessoa_inss"),
        F.lit(TITULAR_CPF).cast("string").alias("nu_cpf_inss"),
    )
)
df_data_competencia = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_competencia"),
    F.col("dt_referencia").alias("competencia_ref"),
)
df_data_inicio = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_inicio"),
    F.col("dt_referencia").alias("dt_inicio_ref"),
)
df_data_fim = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_fim"),
    F.col("dt_referencia").alias("dt_fim_ref"),
)
if table_exists(SILVER_TB_EMPRESA):
    df_empresa = spark.table(SILVER_TB_EMPRESA).select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj_empresa"),
        F.col("nm_empresa").cast("string").alias("nm_empresa"),
    )
else:
    df_empresa = spark.createDataFrame(
        [],
        "empresa_id bigint, nu_cnpj_empresa string, nm_empresa string",
    )

df_empresa_nome_match = (
    df_empresa
    .select(
        "empresa_id",
        F.explode(
            F.array_distinct(
                F.array(
                    empresa_nome_key("nm_empresa"),
                )
            )
        ).alias("empresa_nome_key_match"),
    )
    .filter(F.col("empresa_nome_key_match") != "")
    .groupBy("empresa_nome_key_match")
    .agg(
        F.countDistinct("empresa_id").alias("qtd_empresa_match"),
        F.max("empresa_id").cast("bigint").alias("empresa_id_by_nome"),
    )
    .filter(F.col("qtd_empresa_match") == 1)
    .drop("qtd_empresa_match")
)

if table_exists(GOLD_BRIDGE_VINCULO_MES) and table_exists(GOLD_DIM_TIPO_CONTRATO):
    df_empresa_unica_pessoa_mes = (
        spark.table(GOLD_BRIDGE_VINCULO_MES)
        .join(
            spark.table(GOLD_DIM_TIPO_CONTRATO).select(F.col("id_tipo_contrato").alias("sk_tipo_contrato"), "tipo_pessoa", "tipo_contrato"),
            on="sk_tipo_contrato",
            how="left",
        )
        .filter(
            (F.upper(F.coalesce(F.col("tipo_pessoa"), F.lit(""))) == F.lit("CLT")) |
            (
                F.upper(F.coalesce(F.col("tipo_pessoa"), F.lit(""))).isin("", "NAO_INFORMADO") &
                (~F.upper(F.coalesce(F.col("tipo_contrato"), F.lit(""))).like("%PJ%"))
            )
        )
        .groupBy(
            F.col("sk_pessoa").alias("sk_pessoa_vinculo_mes"),
            F.col("sk_data_mes").alias("sk_data_competencia_vinculo_mes"),
        )
        .agg(
            F.countDistinct("empresa_id").alias("qtd_empresa_mes"),
            F.max("empresa_id").cast("bigint").alias("empresa_id_unica_pessoa_mes"),
        )
        .filter(F.col("qtd_empresa_mes") == 1)
        .drop("qtd_empresa_mes")
    )
else:
    df_empresa_unica_pessoa_mes = spark.createDataFrame(
        [],
        "sk_pessoa_vinculo_mes bigint, sk_data_competencia_vinculo_mes int, empresa_id_unica_pessoa_mes bigint",
    )

df_inss_resolvido = (
    df_inss
    .filter(
        F.col("competencia").cast("date").between(
            F.to_date(F.lit("2021-01-01")),
            F.current_date(),
        )
    )
    .withColumn("vl_contribuicao_inss_base", F.coalesce(F.col("vl_contribuicao_inss"), F.lit(0.0)))
    .withColumn("nome_empresa_key", empresa_nome_key("nome_empresa"))
    .withColumn("nu_cpf_inss", F.lit(TITULAR_CPF).cast("string"))
    .join(df_pessoa_inss, on="nu_cpf_inss", how="left")
    .join(df_data_competencia, F.col("competencia").cast("date") == F.col("competencia_ref"), "left")
    .join(df_data_inicio, F.col("dt_inicio").cast("date") == F.col("dt_inicio_ref"), "left")
    .join(df_data_fim, F.col("dt_fim").cast("date") == F.col("dt_fim_ref"), "left")
    .join(df_empresa.alias("e_cnpj"), df_inss["nu_cnpj_empresa"] == F.col("e_cnpj.nu_cnpj_empresa"), "left")
    .join(
        df_empresa_nome_match,
        F.col("nome_empresa_key") == F.col("empresa_nome_key_match"),
        "left",
    )
    .join(
        df_empresa_unica_pessoa_mes,
        (F.col("sk_pessoa_inss") == df_empresa_unica_pessoa_mes["sk_pessoa_vinculo_mes"]) &
        (F.col("sk_data_competencia") == df_empresa_unica_pessoa_mes["sk_data_competencia_vinculo_mes"]),
        how="left",
    )
    .withColumn(
        "empresa_id_resolvida",
        F.coalesce(F.col("e_cnpj.empresa_id"), F.col("empresa_id_by_nome"), F.col("empresa_id_unica_pessoa_mes")),
    )
)

df_inss_consolidado = (
    df_inss_resolvido
    .groupBy(F.col("sk_pessoa_inss").alias("sk_pessoa"), "empresa_id_resolvida", "sk_data_competencia", "competencia")
    .agg(
        F.max("dt_nascimento").cast("date").alias("dt_nascimento"),
        F.min("seq").cast("int").alias("seq"),
        F.min("dt_inicio").alias("dt_inicio"),
        F.max("dt_fim").alias("dt_fim"),
        F.min("sk_data_inicio").cast("int").alias("sk_data_inicio"),
        F.max("sk_data_fim").cast("int").alias("sk_data_fim"),
        F.round(F.sum(F.coalesce(F.col("vl_base_inss"), F.lit(0.0))), 2).alias("vl_remuneracao"),
        F.round(F.sum(F.coalesce(F.col("vl_contribuicao_inss_base"), F.lit(0.0))), 2).alias("vl_contribuicao_inss_base"),
        F.max("arquivo_origem").alias("doc_id"),
    )
)

df_inss_acumulado_mes = (
    df_inss_consolidado
    .groupBy("sk_pessoa", "competencia")
    .agg(
        F.max("sk_data_competencia").cast("int").alias("sk_data_competencia"),
        F.round(F.sum(F.coalesce(F.col("vl_contribuicao_inss_base"), F.lit(0.0))), 2).alias("vl_contribuicao_inss_mes_total"),
    )
    .withColumn(
        "vl_contribuicao_inss_acumulada",
        F.round(
            F.sum(F.col("vl_contribuicao_inss_mes_total")).over(
                Window.partitionBy("sk_pessoa").orderBy(F.col("competencia").asc_nulls_last()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
            2,
        ),
    )
    .withColumn(
        "qt_meses_contribuidos_acumulado",
        F.sum(
            F.when(F.col("vl_contribuicao_inss_mes_total") > 0, F.lit(1)).otherwise(F.lit(0))
        ).over(
            Window.partitionBy("sk_pessoa").orderBy(F.col("competencia").asc_nulls_last()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ).cast("int"),
    )
)

df_gold = (
    df_inss_consolidado
    .join(
        df_inss_acumulado_mes.select(
            "sk_pessoa",
            "competencia",
            "vl_contribuicao_inss_acumulada",
            "qt_meses_contribuidos_acumulado",
        ),
        on=["sk_pessoa", "competencia"],
        how="left",
    )
    .withColumn(
        "pc_aliquota_efetiva_inss",
        F.round(
            F.when(
                F.col("vl_remuneracao") > 0,
                (F.col("vl_contribuicao_inss_base") / F.col("vl_remuneracao")) * F.lit(100.0),
            ).otherwise(F.lit(0.0)),
            2,
        ),
    )
    .withColumn(
        "vl_salario_liquido_apos_inss",
        F.round(
            F.when(
                F.col("vl_remuneracao").isNotNull(),
                F.col("vl_remuneracao") - F.col("vl_contribuicao_inss_base"),
            ).otherwise(F.lit(0.0)),
            2,
        ),
    )
    .withColumn(
        "sk_inss",
        F.row_number().over(
            Window.orderBy(
                F.col("sk_pessoa").asc_nulls_last(),
                F.col("competencia").asc_nulls_last(),
                F.col("empresa_id_resolvida").asc_nulls_last(),
            )
        ).cast("bigint"),
    )
    .select(
        F.col("sk_inss").cast("bigint").alias("sk_inss"),
        F.col("sk_pessoa").cast("bigint").alias("sk_pessoa"),
        F.coalesce(F.col("dt_nascimento").cast("date"), F.to_date(F.lit(TITULAR_DT_NASCIMENTO))).alias("dt_nascimento"),
        F.col("empresa_id_resolvida").cast("bigint").alias("empresa_id"),
        F.col("seq").cast("int").alias("seq"),
        F.col("sk_data_inicio").cast("int").alias("sk_data_inicio"),
        F.col("sk_data_fim").cast("int").alias("sk_data_fim"),
        F.col("sk_data_competencia").cast("int").alias("sk_data_competencia"),
        F.col("vl_remuneracao").cast("double").alias("vl_remuneracao"),
        F.col("vl_contribuicao_inss_base").cast("double").alias("vl_contribuicao_inss"),
        F.col("pc_aliquota_efetiva_inss").cast("double").alias("pc_aliquota_efetiva_inss"),
        F.col("vl_salario_liquido_apos_inss").cast("double").alias("vl_salario_liquido_apos_inss"),
        F.col("qt_meses_contribuidos_acumulado").cast("int").alias("qt_meses_contribuidos_acumulado"),
        F.col("vl_contribuicao_inss_acumulada").cast("double").alias("vl_contribuicao_inss_acumulada"),
        F.col("doc_id").cast("string").alias("doc_id"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_INSS} (
    sk_inss BIGINT,
    sk_pessoa BIGINT,
    dt_nascimento DATE,
    empresa_id BIGINT,
    seq INT,
    sk_data_inicio INT,
    sk_data_fim INT,
    sk_data_competencia INT,
    vl_remuneracao DOUBLE,
    vl_contribuicao_inss DOUBLE,
    pc_aliquota_efetiva_inss DOUBLE,
    vl_salario_liquido_apos_inss DOUBLE,
    qt_meses_contribuidos_acumulado INT,
    vl_contribuicao_inss_acumulada DOUBLE,
    doc_id STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_INSS)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_INSS)
