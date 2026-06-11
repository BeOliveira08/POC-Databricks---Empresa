# Databricks notebook source
# gold: diagnostico_fgts_nulos

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TB_FGTS = f"{CATALOG}.{SILVER}.tb_fgts"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_FATO_FGTS = f"{CATALOG}.{GOLD}.fato_fgts"
GOLD_BRIDGE_VINCULO_MES = f"{CATALOG}.{GOLD}.bridge_vinculo_mes"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"

for required_table in [SILVER_TB_FGTS, GOLD_DIM_PESSOA]:
    if not table_exists(required_table):
        raise ValueError(f"Tabela obrigatoria nao encontrada: {required_table}")


def show_or_print(title: str, df):
    print(f"\n=== {title} ===")
    display(df)


df_fgts = spark.table(SILVER_TB_FGTS)
df_pessoa = spark.table(GOLD_DIM_PESSOA).select("sk_pessoa", "pessoa_key")

if table_exists(SILVER_TB_EMPRESA):
    df_empresa = spark.table(SILVER_TB_EMPRESA).select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj_empresa"),
        F.col("nm_empresa").cast("string").alias("nm_empresa_ref"),
    )
else:
    df_empresa = empty_df_for_schema("empresa_id bigint, nu_cnpj_empresa string, nm_empresa_ref string")

if table_exists(GOLD_FATO_FGTS):
    df_fato_fgts = spark.table(GOLD_FATO_FGTS)
else:
    df_fato_fgts = empty_df_for_schema(
        """
        sk_fgts string,
        sk_pessoa bigint,
        empresa_id bigint,
        sk_data_competencia int,
        competencia date,
        valor_fgts double,
        sk_data_vencimento int,
        dt_vencimento_fgts date,
        sk_data_primeiro_deposito int,
        dt_primeiro_deposito date,
        valor_depositado double,
        valor_rendimento double,
        valor_saque double,
        valor_fgts_esperado double,
        valor_gap_recolhimento double,
        pc_indice_deposito double,
        pc_gap_recolhimento double,
        qt_dias_atraso_deposito int,
        fl_sem_deposito_esperado int,
        fl_deposito_atrasado int,
        fl_recolhimento_integral int,
        saldo_fgts double,
        qt_eventos bigint,
        pipeline_run_id string,
        ingested_at timestamp
        """
    )

show_or_print(
    "Resumo do silver.tb_fgts",
    df_fgts.agg(
        F.count("*").alias("qtd_total"),
        F.sum(F.when(F.col("nr_cnpj_contratante").isNull(), 1).otherwise(0)).alias("qtd_sem_cnpj"),
        F.sum(F.when(F.col("nome_empresa").isNull(), 1).otherwise(0)).alias("qtd_sem_nome_empresa"),
        F.countDistinct("nome").alias("qtd_titulares_distintos"),
        F.countDistinct("nr_cnpj_contratante").alias("qtd_cnpj_distintos"),
        F.min("competencia").alias("min_competencia"),
        F.max("competencia").alias("max_competencia"),
    ),
)

df_fgts_match = (
    df_fgts
    .withColumn("pessoa_key_join", F.lit(f"DOC|{TITULAR_CPF}"))
    .join(df_pessoa, F.col("pessoa_key_join") == F.col("pessoa_key"), "left")
    .join(df_empresa, F.col("nr_cnpj_contratante") == F.col("nu_cnpj_empresa"), "left")
)

show_or_print(
    "Cobertura de match do FGTS antes da fato",
    df_fgts_match.agg(
        F.count("*").alias("qtd_total"),
        F.sum(F.when(F.col("sk_pessoa").isNull(), 1).otherwise(0)).alias("qtd_sem_match_pessoa"),
        F.sum(F.when(F.col("empresa_id").isNull(), 1).otherwise(0)).alias("qtd_sem_match_empresa"),
        F.sum(
            F.when(
                F.col("sk_pessoa").isNull() | F.col("empresa_id").isNull(),
                1,
            ).otherwise(0)
        ).alias("qtd_com_falha_match"),
    ),
)

show_or_print(
    "Titular do FGTS sem match em dim_pessoa",
    df_fgts_match
    .filter(F.col("sk_pessoa").isNull())
    .groupBy("nome")
    .agg(
        F.count("*").alias("qtd_registros"),
        F.min("competencia").alias("min_competencia"),
        F.max("competencia").alias("max_competencia"),
    )
    .orderBy(F.col("qtd_registros").desc(), F.col("nome").asc_nulls_last())
)

show_or_print(
    "CNPJs do FGTS sem match em tb_empresa",
    df_fgts_match
    .filter(F.col("nr_cnpj_contratante").isNotNull() & F.col("empresa_id").isNull())
    .groupBy("nr_cnpj_contratante", "nome_empresa")
    .agg(
        F.count("*").alias("qtd_registros"),
        F.min("competencia").alias("min_competencia"),
        F.max("competencia").alias("max_competencia"),
    )
    .orderBy(F.col("qtd_registros").desc(), F.col("nr_cnpj_contratante").asc())
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

    show_or_print(
        "Cobertura da fato_fgts",
        df_fato_fgts.agg(
            F.count("*").alias("qtd_total"),
            F.sum(F.when(F.col("sk_pessoa").isNull(), 1).otherwise(0)).alias("qtd_sk_pessoa_nulo"),
            F.sum(F.when(F.col("empresa_id").isNull(), 1).otherwise(0)).alias("qtd_empresa_id_nulo"),
            F.sum(F.when(F.col("sk_data_competencia").isNull(), 1).otherwise(0)).alias("qtd_sk_data_nulo"),
            F.sum(F.when(F.col("valor_fgts_esperado").isNull(), 1).otherwise(0)).alias("qtd_esperado_nulo"),
            F.sum(F.when(F.coalesce(F.col("valor_fgts_esperado"), F.lit(0.0)) == 0, 1).otherwise(0)).alias("qtd_esperado_zero"),
        ),
    )

    show_or_print(
        "Registros da fato_fgts sem valor esperado casado",
        df_fato_fgts
        .filter(F.coalesce(F.col("valor_fgts_esperado"), F.lit(0.0)) == 0)
        .select(
            "sk_fgts",
            "sk_pessoa",
            "empresa_id",
            "sk_data_competencia",
            "competencia",
            "valor_fgts",
            "valor_depositado",
            "valor_fgts_esperado",
        )
        .orderBy(F.col("competencia").desc_nulls_last())
    )

    show_or_print(
        "Combinações esperadas de FGTS pela bridge",
        df_fgts_esperado
        .groupBy()
        .agg(
            F.count("*").alias("qtd_chaves_esperadas"),
            F.countDistinct("sk_pessoa").alias("qtd_pessoas"),
            F.countDistinct("empresa_id").alias("qtd_empresas"),
        ),
    )
