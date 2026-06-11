# Databricks notebook source
# gold: fato_fgts

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../config/domain_config

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_FGTS = f"{CATALOG}.{SILVER}.tb_fgts"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"
GOLD_BRIDGE_VINCULO_MES = f"{CATALOG}.{GOLD}.bridge_vinculo_mes"
GOLD_FATO_FGTS = f"{CATALOG}.{GOLD}.fato_fgts"


def empresa_nome_key(col_name: str):
    return F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", "")

if not table_exists(SILVER_TB_FGTS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_FGTS}")
for dim_table in [GOLD_DIM_PESSOA, GOLD_DIM_DATA]:
    if not table_exists(dim_table):
        raise ValueError(f"Dimensao obrigatoria nao encontrada: {dim_table}")

df_fgts = spark.table(SILVER_TB_FGTS)
df_pessoa = spark.table(GOLD_DIM_PESSOA).select(
    F.col("id_pessoa").cast("bigint").alias("id_pessoa"),
)
df_pessoa_fgts = (
    df_pessoa
    .filter(F.col("id_pessoa") == F.regexp_replace(F.lit(TITULAR_CPF), r"[^0-9]", "").cast("bigint"))
    .select(
        F.col("id_pessoa").cast("bigint").alias("sk_pessoa_fgts"),
        F.lit(TITULAR_CPF).cast("string").alias("nu_cpf_fgts"),
    )
)
df_data = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_competencia_fgts"),
    F.col("dt_referencia").alias("competencia_ref"),
)
df_data_vencimento = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_vencimento"),
    F.col("dt_referencia").alias("dt_vencimento_ref"),
)
df_data_deposito = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_primeiro_deposito"),
    F.col("dt_referencia").alias("dt_primeiro_deposito_ref"),
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
        "nu_cnpj_empresa",
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
        F.max("nu_cnpj_empresa").cast("string").alias("nu_cnpj_empresa_by_nome"),
    )
    .filter(F.col("qtd_empresa_match") == 1)
    .drop("qtd_empresa_match")
)

if table_exists(GOLD_BRIDGE_VINCULO_MES) and table_exists(GOLD_DIM_TIPO_CONTRATO):
    df_fgts_esperado = (
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
        .groupBy("sk_pessoa", "empresa_id", F.col("sk_data_mes").alias("sk_data_competencia"))
        .agg(
            F.round(
                F.sum(F.coalesce(F.col("vl_remuneracao_mes_proporcional"), F.lit(0.0))) * F.lit(0.08),
                2,
            ).alias("valor_fgts_esperado")
        )
    )
    df_fgts_esperado_unico_pessoa_mes = (
        df_fgts_esperado
        .groupBy("sk_pessoa", "sk_data_competencia")
        .agg(
            F.countDistinct("empresa_id").alias("qtd_empresa_mes"),
            F.max("empresa_id").cast("bigint").alias("empresa_id_unica_pessoa_mes"),
            F.round(F.sum("valor_fgts_esperado"), 2).alias("valor_fgts_esperado_unico_pessoa_mes"),
        )
        .filter(F.col("qtd_empresa_mes") == 1)
        .drop("qtd_empresa_mes")
        .withColumnRenamed("sk_pessoa", "sk_pessoa_unica_pessoa_mes_key")
        .withColumnRenamed("sk_data_competencia", "sk_data_competencia_unica_pessoa_mes_key")
    )
    df_fgts_esperado_unico_empresa_mes = (
        df_fgts_esperado
        .groupBy("empresa_id", "sk_data_competencia")
        .agg(
            F.countDistinct("sk_pessoa").alias("qtd_pessoa_mes"),
            F.max("sk_pessoa").cast("bigint").alias("sk_pessoa_unica_empresa_mes"),
        )
        .filter(F.col("qtd_pessoa_mes") == 1)
        .drop("qtd_pessoa_mes")
        .withColumnRenamed("empresa_id", "empresa_id_unica_empresa_mes_key")
        .withColumnRenamed("sk_data_competencia", "sk_data_competencia_unica_empresa_mes_key")
    )
    df_fgts_esperado_exact = df_fgts_esperado.select(
        F.col("sk_pessoa").cast("bigint").alias("exp_sk_pessoa"),
        F.col("empresa_id").cast("bigint").alias("exp_empresa_id"),
        F.col("sk_data_competencia").cast("int").alias("exp_sk_data_competencia"),
        F.col("valor_fgts_esperado").cast("double").alias("valor_fgts_esperado_exact"),
    )
else:
    df_fgts_esperado = spark.createDataFrame(
        [],
        "sk_pessoa bigint, empresa_id bigint, sk_data_competencia int, valor_fgts_esperado double",
    )
    df_fgts_esperado_unico_pessoa_mes = spark.createDataFrame(
        [],
        "sk_pessoa_unica_pessoa_mes_key bigint, sk_data_competencia_unica_pessoa_mes_key int, empresa_id_unica_pessoa_mes bigint, valor_fgts_esperado_unico_pessoa_mes double",
    )
    df_fgts_esperado_unico_empresa_mes = spark.createDataFrame(
        [],
        "empresa_id_unica_empresa_mes_key bigint, sk_data_competencia_unica_empresa_mes_key int, sk_pessoa_unica_empresa_mes bigint",
    )
    df_fgts_esperado_exact = spark.createDataFrame(
        [],
        "exp_sk_pessoa bigint, exp_empresa_id bigint, exp_sk_data_competencia int, valor_fgts_esperado_exact double",
    )

w = Window.partitionBy("nome", "nr_cnpj_contratante", "competencia").orderBy(F.col("dt_lancamento").asc_nulls_last())
w_fgts_pessoa_mes = Window.partitionBy("sk_pessoa_fgts", "sk_data_competencia_fgts")

df_gold = (
    df_fgts
    .withColumn("tp_evento_norm", F.lower(F.trim(F.coalesce(F.col("tp_evento"), F.lit("outros")))))
    .withColumn("nome_empresa_key", empresa_nome_key("nome_empresa"))
    .withColumn(
        "fl_evento_deposito",
        F.when(F.col("tp_evento_norm").contains("deposit"), F.lit(1)).otherwise(F.lit(0)),
    )
    .withColumn("_saldo_final_mes", F.last("saldo", ignorenulls=True).over(w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
    .groupBy("nome", "nr_cnpj_contratante", "nome_empresa", "nome_empresa_key", "competencia")
    .agg(
        F.sum(F.coalesce(F.col("valor"), F.lit(0.0))).alias("valor_fgts"),
        F.sum(F.when(F.col("fl_evento_deposito") == 1, F.coalesce(F.col("valor"), F.lit(0.0))).otherwise(F.lit(0.0))).alias("valor_depositado"),
        F.sum(
            F.when(
                F.col("tp_evento_norm").isin("rendimento_jam", "jam", "rendimento"),
                F.coalesce(F.col("valor"), F.lit(0.0)),
            ).otherwise(F.lit(0.0))
        ).alias("valor_rendimento"),
        F.sum(
            F.when(
                F.col("tp_evento_norm").isin("saque", "rescisao", "multa_rescisoria"),
                F.coalesce(F.col("valor"), F.lit(0.0)),
            ).otherwise(F.lit(0.0))
        ).alias("valor_saque"),
        F.max(F.col("_saldo_final_mes")).alias("saldo_fgts"),
        F.count("*").alias("qt_eventos"),
        F.min(F.when(F.col("fl_evento_deposito") == 1, F.col("dt_lancamento"))).alias("dt_primeiro_deposito"),
    )
    .withColumn("nu_cpf_fgts", F.lit(TITULAR_CPF).cast("string"))
    .join(df_pessoa_fgts, on="nu_cpf_fgts", how="left")
    .join(df_data, F.col("competencia").cast("date") == F.col("competencia_ref"), "left")
    .withColumn(
        "qt_fgts_rows_pessoa_mes",
        F.when(
            F.col("sk_pessoa_fgts").isNotNull() & F.col("sk_data_competencia_fgts").isNotNull(),
            F.count(F.lit(1)).over(w_fgts_pessoa_mes),
        ).otherwise(F.lit(None).cast("bigint")),
    )
    .join(df_empresa.alias("e_cnpj"), F.col("nr_cnpj_contratante") == F.col("e_cnpj.nu_cnpj_empresa"), "left")
    .join(
        df_empresa_nome_match,
        F.col("nome_empresa_key") == F.col("empresa_nome_key_match"),
        "left",
    )
    .withColumn(
        "empresa_id_resolvida",
        F.coalesce(F.col("e_cnpj.empresa_id"), F.col("empresa_id_by_nome")),
    )
    .withColumn(
        "nu_cnpj_contratante_resolvido",
        F.coalesce(F.col("nr_cnpj_contratante"), F.col("nu_cnpj_empresa_by_nome")),
    )
    .join(
        df_fgts_esperado_unico_pessoa_mes,
        (F.col("sk_pessoa_fgts") == F.col("sk_pessoa_unica_pessoa_mes_key")) &
        (F.col("sk_data_competencia_fgts") == F.col("sk_data_competencia_unica_pessoa_mes_key")),
        how="left",
    )
    .withColumn(
        "empresa_id_resolvida",
        F.coalesce(
            F.col("empresa_id_resolvida"),
            F.when(F.col("qt_fgts_rows_pessoa_mes") == 1, F.col("empresa_id_unica_pessoa_mes")),
        ),
    )
    .join(
        df_fgts_esperado_unico_empresa_mes,
        (F.col("empresa_id_resolvida") == F.col("empresa_id_unica_empresa_mes_key")) &
        (F.col("sk_data_competencia_fgts") == F.col("sk_data_competencia_unica_empresa_mes_key")),
        "left",
    )
    .withColumn(
        "sk_pessoa_resolvida",
        F.coalesce(F.col("sk_pessoa_fgts"), F.col("sk_pessoa_unica_empresa_mes")),
    )
    .join(
        df_fgts_esperado_exact,
        (F.col("sk_pessoa_resolvida") == F.col("exp_sk_pessoa")) &
        (F.col("empresa_id_resolvida") == F.col("exp_empresa_id")) &
        (F.col("sk_data_competencia_fgts") == F.col("exp_sk_data_competencia")),
        how="left",
    )
    .withColumn(
        "dt_vencimento_fgts",
        F.date_add(F.trunc(F.add_months(F.col("competencia"), 1), "month"), 6).cast("date"),
    )
    .join(df_data_vencimento, F.col("dt_vencimento_fgts") == F.col("dt_vencimento_ref"), "left")
    .join(df_data_deposito, F.col("dt_primeiro_deposito") == F.col("dt_primeiro_deposito_ref"), "left")
    .withColumn(
        "valor_fgts_esperado",
        F.round(
            F.coalesce(
                F.col("valor_fgts_esperado_exact"),
                F.when(F.col("qt_fgts_rows_pessoa_mes") == 1, F.col("valor_fgts_esperado_unico_pessoa_mes")),
                F.lit(0.0),
            ),
            2,
        ),
    )
    .withColumn("valor_depositado", F.round(F.coalesce(F.col("valor_depositado"), F.lit(0.0)), 2))
    .withColumn(
        "valor_gap_recolhimento",
        F.round(F.greatest(F.col("valor_fgts_esperado") - F.col("valor_depositado"), F.lit(0.0)), 2),
    )
    .withColumn(
        "sk_fgts",
        F.abs(
            F.xxhash64(
                F.coalesce(F.col("competencia").cast("string"), F.lit("")),
                F.coalesce(F.col("nu_cnpj_contratante_resolvido"), F.lit("")),
                F.coalesce(F.col("nome_empresa"), F.lit("")),
                F.coalesce(F.col("sk_pessoa_resolvida").cast("string"), F.lit("")),
            )
        ).cast("bigint"),
    )
    .select(
        F.col("sk_fgts").cast("bigint").alias("sk_fgts"),
        F.col("sk_pessoa_resolvida").cast("bigint").alias("sk_pessoa"),
        F.col("empresa_id_resolvida").cast("bigint").alias("empresa_id"),
        F.col("sk_data_competencia_fgts").cast("int").alias("sk_data_competencia"),
        F.round(F.col("valor_fgts"), 2).cast("double").alias("valor_fgts"),
        F.col("sk_data_vencimento").cast("int").alias("sk_data_vencimento"),
        F.col("sk_data_primeiro_deposito").cast("int").alias("sk_data_primeiro_deposito"),
        F.col("valor_depositado").cast("double").alias("valor_depositado"),
        F.round(F.col("valor_rendimento"), 2).cast("double").alias("valor_rendimento"),
        F.round(F.col("valor_saque"), 2).cast("double").alias("valor_saque"),
        F.col("valor_fgts_esperado").cast("double").alias("valor_fgts_esperado"),
        F.col("valor_gap_recolhimento").cast("double").alias("valor_gap_recolhimento"),
        F.round(F.col("saldo_fgts"), 2).cast("double").alias("saldo_fgts"),
        F.col("qt_eventos").cast("bigint").alias("qt_eventos"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_FGTS} (
    sk_fgts BIGINT,
    sk_pessoa BIGINT,
    empresa_id BIGINT,
    sk_data_competencia INT,
    valor_fgts DOUBLE,
    sk_data_vencimento INT,
    sk_data_primeiro_deposito INT,
    valor_depositado DOUBLE,
    valor_rendimento DOUBLE,
    valor_saque DOUBLE,
    valor_fgts_esperado DOUBLE,
    valor_gap_recolhimento DOUBLE,
    saldo_fgts DOUBLE,
    qt_eventos BIGINT,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_FGTS)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_FGTS)
