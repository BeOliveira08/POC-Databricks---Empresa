# Databricks notebook source
# gold: fato_ctps

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_DIM_CARGO = f"{CATALOG}.{GOLD}.dim_cargo"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"
GOLD_FATO_CTPS = f"{CATALOG}.{GOLD}.fato_ctps"


def empresa_nome_key(col_name: str):
    return F.regexp_replace(F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit(""))), r"[^A-Z0-9]", "")


def normalized_activity_date(col_expr):
    date_col = col_expr.cast("date")
    return F.when(
        date_col.isNull() | (date_col <= F.to_date(F.lit("1900-01-01"))),
        F.lit(None).cast("date"),
    ).otherwise(date_col)


def suspicious_exclusivity_text(col_name: str):
    txt = F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    return (
        txt.contains("LGPD")
        | txt.contains("DADOS PESSOAIS")
        | txt.contains("CONFIDENCIAL")
        | txt.contains("INFORMACOES CONFIDENCIAIS")
        | txt.contains("PROPRIEDADE INTELECTUAL")
        | txt.contains("DIREITOS AUTORAIS")
        | txt.contains("USO DE IMAGEM")
        | txt.contains("EXCLUSIVA RESPONSABILIDADE")
    )


if not table_exists(SILVER_TB_CTPS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_CTPS}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")

df_ctps = spark.table(SILVER_TB_CTPS).withColumn("ctps_row_id", F.monotonically_increasing_id())
df_data = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").cast("int").alias("sk_data"),
    F.col("dt_referencia").cast("date").alias("dt_referencia"),
)
df_data_inicio = df_data.select(
    F.col("sk_data").alias("sk_data_inicio"),
    F.col("dt_referencia").alias("dt_inicio_ref"),
)
df_data_fim = df_data.select(
    F.col("sk_data").alias("sk_data_fim"),
    F.col("dt_referencia").alias("dt_fim_ref"),
)

if table_exists(SILVER_TB_EMPRESA):
    df_empresa = spark.table(SILVER_TB_EMPRESA).select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj_empresa"),
        F.col("nm_empresa").cast("string").alias("nm_empresa"),
    )
else:
    df_empresa = spark.createDataFrame([], "empresa_id bigint, nu_cnpj_empresa string, nm_empresa string")

df_empresa_nome_match = (
    df_empresa
    .withColumn("empresa_nome_key_match", empresa_nome_key("nm_empresa"))
    .filter(F.col("empresa_nome_key_match") != "")
    .groupBy("empresa_nome_key_match")
    .agg(
        F.countDistinct("empresa_id").alias("qtd_empresa_match"),
        F.max("empresa_id").cast("bigint").alias("empresa_id_by_nome"),
    )
    .filter(F.col("qtd_empresa_match") == 1)
    .drop("qtd_empresa_match")
)

if table_exists(GOLD_DIM_CARGO):
    df_cargo = spark.table(GOLD_DIM_CARGO).select(
        F.col("id_cargo").cast("bigint").alias("sk_cargo"),
        F.col("cargo").cast("string").alias("cargo_dim"),
    )
else:
    df_cargo = spark.createDataFrame([], "sk_cargo bigint, cargo_dim string")

if table_exists(GOLD_DIM_TIPO_CONTRATO):
    df_tipo_contrato = (
        spark.table(GOLD_DIM_TIPO_CONTRATO)
        .select(
            F.col("id_tipo_contrato").cast("bigint").alias("sk_tipo_contrato"),
            F.upper(F.col("tipo_contrato").cast("string")).alias("tipo_contrato"),
        )
        .filter(F.col("tipo_contrato") == "CLT")
        .limit(1)
    )
else:
    df_tipo_contrato = spark.createDataFrame([], "sk_tipo_contrato bigint, tipo_contrato string")

default_sk_tipo_contrato = (
    df_tipo_contrato.collect()[0]["sk_tipo_contrato"]
    if df_tipo_contrato.count() > 0
    else None
)

df_ctps_resolvida = (
    df_ctps.alias("c")
    .withColumn("nome_empresa_key", empresa_nome_key("c.nome_empresa"))
    .join(df_empresa.alias("e_cnpj"), F.col("c.nu_cnpj_empresa") == F.col("e_cnpj.nu_cnpj_empresa"), "left")
    .join(df_empresa_nome_match.alias("e_nome"), F.col("nome_empresa_key") == F.col("e_nome.empresa_nome_key_match"), "left")
    .withColumn("empresa_id_resolvida", F.coalesce(F.col("e_cnpj.empresa_id"), F.col("empresa_id_by_nome")))
    .join(df_data_inicio.alias("di"), F.col("c.dt_inicio").cast("date") == F.col("di.dt_inicio_ref"), "left")
    .join(df_data_fim.alias("df"), normalized_activity_date(F.col("c.dt_fim")) == F.col("df.dt_fim_ref"), "left")
    .join(df_cargo.alias("cg"), F.col("c.cargo") == F.col("cg.cargo_dim"), "left")
    .select(
        F.col("c.ctps_row_id").alias("ctps_row_id"),
        F.col("c.id_ctps").alias("id_ctps"),
        F.col("c.nome_empresa").alias("nome_empresa"),
        F.col("c.nu_cnpj_empresa").alias("nu_cnpj_empresa_ctps"),
        F.col("c.cargo").alias("cargo"),
        F.lit("CLT").cast("string").alias("tipo_contrato"),
        F.col("c.dt_inicio").alias("dt_inicio"),
        normalized_activity_date(F.col("c.dt_fim")).alias("dt_fim"),
        F.col("c.vl_salario").alias("vl_salario"),
        F.col("c.status").alias("status"),
        F.col("c.arquivo_origem").alias("arquivo_origem"),
        F.col("c.pipeline_run_id").alias("pipeline_run_id"),
        F.col("c.ingested_at").alias("ingested_at"),
        F.col("nome_empresa_key"),
        F.col("empresa_id_resolvida"),
        F.col("di.sk_data_inicio").alias("sk_data_inicio"),
        F.col("df.sk_data_fim").alias("sk_data_fim"),
        F.col("cg.sk_cargo").alias("sk_cargo"),
    )
)

if table_exists(SILVER_TB_CONTRATOS):
    df_contratos_clt = (
        spark.table(SILVER_TB_CONTRATOS)
        .filter(F.upper(F.coalesce(F.col("tp_vinculo"), F.lit(""))) == "CLT")
        .withColumn("contrato_row_id", F.monotonically_increasing_id())
        .withColumn("contrato_nome_key", empresa_nome_key("consultoria"))
        .select(
            "contrato_row_id",
            F.col("consultoria").cast("string").alias("consultoria_contrato"),
            F.col("cnpj_consultoria").cast("string").alias("cnpj_consultoria"),
            F.col("dt_inicio").cast("date").alias("dt_inicio_contrato"),
            F.col("dt_fim").cast("date").alias("dt_fim_contrato"),
            F.col("fl_clausula_exclusividade").cast("int").alias("fl_clausula_exclusividade"),
            F.col("fl_clausula_nao_concorrencia").cast("int").alias("fl_clausula_nao_concorrencia"),
            F.col("tp_clausula_exclusividade").cast("string").alias("tp_clausula_exclusividade"),
            F.col("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade"),
            F.col("criterio_extracao_clausula").cast("string").alias("criterio_extracao_clausula"),
            F.col("score_extracao_clausula").cast("int").alias("score_extracao_clausula"),
            F.col("arquivo_origem").cast("string").alias("arquivo_origem_contrato"),
            F.col("criterio_match_documento").cast("string").alias("criterio_match_documento"),
            F.col("contrato_nome_key").cast("string").alias("contrato_nome_key"),
        )
    )

    cnpj_match = (
        F.col("c.nu_cnpj_empresa_ctps").isNotNull()
        & F.col("k.cnpj_consultoria").isNotNull()
        & (F.col("c.nu_cnpj_empresa_ctps") == F.col("k.cnpj_consultoria"))
    )
    nome_match = (
        F.col("c.nome_empresa_key").isNotNull()
        & F.col("k.contrato_nome_key").isNotNull()
        & (F.col("c.nome_empresa_key") == F.col("k.contrato_nome_key"))
    )

    df_ctps_match = (
        df_ctps_resolvida.alias("c")
        .join(df_contratos_clt.alias("k"), cnpj_match | nome_match, "left")
        .withColumn(
            "match_priority",
            F.when(cnpj_match, F.lit(0))
            .when(nome_match, F.lit(1))
            .otherwise(F.lit(99)),
        )
        .withColumn(
            "date_distance",
            F.when(
                F.col("c.dt_inicio").isNotNull() & F.col("k.dt_inicio_contrato").isNotNull(),
                F.abs(F.datediff(F.col("c.dt_inicio"), F.col("k.dt_inicio_contrato"))),
            ).otherwise(F.lit(999999)),
        )
    )

    w_contrato = Window.partitionBy("c.ctps_row_id").orderBy(
        F.col("match_priority").asc(),
        F.col("date_distance").asc(),
        F.col("k.score_extracao_clausula").desc_nulls_last(),
        F.col("k.arquivo_origem_contrato").desc_nulls_last(),
    )

    df_best_contrato = (
        df_ctps_match
        .withColumn("rn_contrato", F.row_number().over(w_contrato))
        .filter(F.col("rn_contrato") == 1)
        .select(
            F.col("c.ctps_row_id").alias("ctps_row_id"),
            F.col("k.fl_clausula_exclusividade").alias("fl_clausula_exclusividade"),
            F.col("k.fl_clausula_nao_concorrencia").alias("fl_clausula_nao_concorrencia"),
            F.col("k.tp_clausula_exclusividade").alias("tp_clausula_exclusividade"),
            F.col("k.trecho_clausula_exclusividade").alias("trecho_clausula_exclusividade"),
            F.col("k.criterio_extracao_clausula").alias("criterio_extracao_clausula"),
            F.col("k.score_extracao_clausula").alias("score_extracao_clausula"),
            F.col("k.arquivo_origem_contrato").alias("arquivo_origem_contrato"),
            F.col("k.criterio_match_documento").alias("criterio_match_documento"),
            F.col("match_priority").alias("match_priority"),
        )
    )
else:
    df_best_contrato = spark.createDataFrame(
        [],
        """
        ctps_row_id long,
        fl_clausula_exclusividade int,
        fl_clausula_nao_concorrencia int,
        tp_clausula_exclusividade string,
        trecho_clausula_exclusividade string,
        criterio_extracao_clausula string,
        score_extracao_clausula int,
        arquivo_origem_contrato string,
        criterio_match_documento string,
        match_priority int
        """,
    )

df_gold = (
    df_ctps_resolvida
    .join(df_best_contrato, on="ctps_row_id", how="left")
    .withColumn(
        "criterio_match_contrato",
        F.when(F.col("arquivo_origem_contrato").isNull(), F.lit("SEM_CONTRATO_CASADO"))
        .when(F.col("match_priority") == 0, F.lit("CNPJ"))
        .when(F.col("match_priority") == 1, F.lit("NOME_EMPRESA"))
        .otherwise(F.lit("OUTRO")),
    )
    .select(
        F.col("id_ctps").cast("bigint").alias("id_ctps"),
        F.col("empresa_id_resolvida").cast("bigint").alias("empresa_id"),
        F.col("sk_cargo").cast("bigint").alias("sk_cargo"),
        F.lit(default_sk_tipo_contrato).cast("bigint").alias("sk_tipo_contrato"),
        F.col("sk_data_inicio").cast("int").alias("sk_data_inicio"),
        F.col("sk_data_fim").cast("int").alias("sk_data_fim"),
        F.col("vl_salario").cast("double").alias("vl_salario"),
        F.col("status").cast("string").alias("status_ctps"),
        F.when(
            suspicious_exclusivity_text("trecho_clausula_exclusividade"),
            F.lit(0),
        ).otherwise(F.coalesce(F.col("fl_clausula_exclusividade"), F.lit(0))).cast("int").alias("fl_exclusividade_contrato"),
        F.coalesce(F.col("fl_clausula_nao_concorrencia"), F.lit(0)).cast("int").alias("fl_nao_concorrencia"),
        F.when(
            suspicious_exclusivity_text("trecho_clausula_exclusividade"),
            F.lit("NAO_INFORMADO"),
        ).otherwise(F.col("tp_clausula_exclusividade")).cast("string").alias("tp_clausula_exclusividade"),
        F.col("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade"),
        F.col("criterio_extracao_clausula").cast("string").alias("criterio_extracao_clausula"),
        F.coalesce(F.col("score_extracao_clausula"), F.lit(0)).cast("int").alias("score_extracao_clausula"),
        F.coalesce(F.col("arquivo_origem_contrato"), F.col("arquivo_origem")).cast("string").alias("doc_id"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_CTPS} (
    id_ctps BIGINT,
    empresa_id BIGINT,
    sk_cargo BIGINT,
    sk_tipo_contrato BIGINT,
    sk_data_inicio INT,
    sk_data_fim INT,
    vl_salario DOUBLE,
    status_ctps STRING,
    fl_exclusividade_contrato INT,
    fl_nao_concorrencia INT,
    tp_clausula_exclusividade STRING,
    trecho_clausula_exclusividade STRING,
    criterio_extracao_clausula STRING,
    score_extracao_clausula INT,
    doc_id STRING,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_CTPS)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_CTPS)
