# Databricks notebook source
# gold: fato_vinculo_empresa

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_FUNCIONARIO_EMPRESA = f"{CATALOG}.{SILVER}.tb_func_emp"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
SILVER_TB_CLIENTE_REFERENCIA = f"{CATALOG}.{SILVER}.tb_cliente_referencia"
GOLD_DIM_FUNCIONARIO = f"{CATALOG}.{GOLD}.dim_funcionario"
GOLD_DIM_CLIENTE = f"{CATALOG}.{GOLD}.dim_cliente"
GOLD_DIM_CARGO = f"{CATALOG}.{GOLD}.dim_cargo"
GOLD_DIM_TIPO_CONTRATO = f"{CATALOG}.{GOLD}.dim_tipo_contrato"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_FATO_VINCULO_EMPRESA = f"{CATALOG}.{GOLD}.fato_vinculo_empresa"


def normalized_activity_date(col_expr):
    date_col = col_expr.cast("date")
    return F.when(
        date_col.isNull() | (date_col <= F.to_date(F.lit("1900-01-01"))),
        F.lit(None).cast("date"),
    ).otherwise(date_col)


def text_key_expr(col_expr):
    txt = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(
        txt,
        (
            r"\b("
            r"SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU|"
            r"DO|DA|DE|DAS|DOS|E|EM|"
            r"CONSULTORIA|CONSULTING|ASSESSORIA|SERVICOS|SERVICO|"
            r"SOLUCOES|SOLUCAO|TECNOLOGIA|TECNOLOGIAS|INFORMATICA|"
            r"INFORMACAO|COMUNICACAO|SOFTWARE|DESENVOLVIMENTO|SISTEMAS|"
            r"NEGOCIOS|OUTSOURCING|RECURSOS|HUMANOS|PESSOAS|BUSINESS|"
            r"CENTER|ENGENHARIA|TREINAMENTOS|COMERCIO|INTELIGENCIA|DADOS"
            r")\b"
        ),
        " ",
    )
    txt = F.regexp_replace(txt, r"\b[A-Z0-9]\b", " ")
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def has_token(col_name: str, token: str):
    return F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit(""))).contains(token)


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


def normalized_tipo_contrato_expr(col_expr):
    txt = F.upper(F.trim(F.coalesce(col_expr.cast("string"), F.lit(""))))
    return (
        F.when(txt.contains("COOPER"), F.lit("COOPERATIVA"))
        .when(txt.contains("CLT"), F.lit("CLT"))
        .when(txt.contains("PJ"), F.lit("PJ"))
        .otherwise(txt)
    )


if not table_exists(SILVER_TB_FUNCIONARIO_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_FUNCIONARIO_EMPRESA}")
if not table_exists(SILVER_TB_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_EMPRESA}")

df_vinculo = spark.table(SILVER_TB_FUNCIONARIO_EMPRESA)
df_empresa = (
    spark.table(SILVER_TB_EMPRESA)
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id_ref"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj_empresa"),
        F.col("nm_empresa").cast("string").alias("nm_empresa_ref"),
        F.col("tp_empresa").cast("string").alias("tp_empresa_ref"),
        F.col("tp_empresa_papeis").cast("string").alias("tp_empresa_papeis_ref"),
        normalized_activity_date(F.col("data_inicio")).alias("dt_inicio_empresa"),
        normalized_activity_date(F.col("data_fim")).alias("dt_fim_empresa"),
    )
    .withColumn("empresa_nome_key", text_key_expr(F.col("nm_empresa_ref")))
)

if table_exists(GOLD_DIM_FUNCIONARIO):
    df_funcionario = spark.table(GOLD_DIM_FUNCIONARIO).select(
        F.col("id_funcionario").cast("bigint").alias("id_funcionario_dim"),
    )
else:
    df_funcionario = empty_df_for_schema("id_funcionario_dim bigint")

if table_exists(GOLD_DIM_CLIENTE):
    w_cliente_dim = Window.partitionBy("cliente_nome_key_dim").orderBy(
        F.length(F.coalesce(F.col("nm_cliente_dim"), F.lit(""))).asc(),
        F.col("cliente_id").asc_nulls_last(),
    )
    df_cliente_dim = (
        spark.table(GOLD_DIM_CLIENTE)
        .select(
            F.col("cliente_id").cast("bigint").alias("cliente_id"),
            F.col("nm_cliente").cast("string").alias("nm_cliente_dim"),
        )
        .withColumn("cliente_nome_key_dim", text_key_expr(F.col("nm_cliente_dim")))
        .withColumn("rn_cliente_dim", F.row_number().over(w_cliente_dim))
        .filter(F.col("rn_cliente_dim") == 1)
        .drop("rn_cliente_dim")
    )
else:
    df_cliente_dim = empty_df_for_schema("cliente_id bigint, nm_cliente_dim string, cliente_nome_key_dim string")

if table_exists(GOLD_DIM_CARGO):
    df_cargo = spark.table(GOLD_DIM_CARGO).select(
        F.col("id_cargo").cast("bigint").alias("sk_cargo"),
        F.upper(F.col("cargo").cast("string")).alias("cargo_dim"),
    )
else:
    df_cargo = empty_df_for_schema("sk_cargo bigint, cargo_dim string")

if table_exists(GOLD_DIM_TIPO_CONTRATO):
    df_tipo_contrato = spark.table(GOLD_DIM_TIPO_CONTRATO).select(
        F.col("id_tipo_contrato").cast("bigint").alias("sk_tipo_contrato"),
        F.upper(F.col("tipo_contrato").cast("string")).alias("tipo_contrato"),
    )
else:
    df_tipo_contrato = empty_df_for_schema("sk_tipo_contrato bigint, tipo_contrato string")

if table_exists(GOLD_DIM_DATA):
    df_data = spark.table(GOLD_DIM_DATA).select(
        F.col("id_data").cast("int").alias("sk_data"),
        F.col("dt_referencia").cast("date").alias("dt_referencia"),
    )
else:
    df_data = empty_df_for_schema("sk_data int, dt_referencia date")

df_data_inicio = df_data.select(
    F.col("sk_data").alias("sk_data_inicio"),
    F.col("dt_referencia").alias("dt_inicio_ref"),
)
df_data_fim = df_data.select(
    F.col("sk_data").alias("sk_data_fim"),
    F.col("dt_referencia").alias("dt_fim_ref"),
)

tipo_lookup = {
    row["tipo_contrato"]: row["sk_tipo_contrato"]
    for row in df_tipo_contrato.collect()
}
default_sk_tipo_contrato = tipo_lookup.get("NAO_INFORMADO")
sk_tipo_contrato_clt = tipo_lookup.get("CLT")
sk_tipo_contrato_pj = tipo_lookup.get("PJ")
sk_tipo_contrato_cooperativa = tipo_lookup.get("COOPERATIVA")

df_vinculo_base = (
    df_vinculo.alias("v")
    .join(df_empresa.alias("e"), F.col("v.empresa_id") == F.col("e.empresa_id_ref"), "left")
    .join(
        df_funcionario.alias("f"),
        F.col("v.id_funcionario").cast("bigint") == F.col("f.id_funcionario_dim"),
        "left",
    )
    .select(
        F.col("v.id_funcionario_empresa").cast("bigint").alias("funcionario_empresa_id"),
        F.col("f.id_funcionario_dim").cast("bigint").alias("sk_pessoa"),
        F.col("v.empresa_id").cast("bigint").alias("empresa_id"),
        F.col("v.nome_empresa").cast("string").alias("nome_empresa_vinculo"),
        F.col("e.nu_cnpj_empresa").cast("string").alias("nu_cnpj_empresa"),
        F.col("e.nm_empresa_ref").cast("string").alias("nm_empresa_ref"),
        F.col("e.tp_empresa_ref").cast("string").alias("tp_empresa_ref"),
        F.col("e.tp_empresa_papeis_ref").cast("string").alias("tp_empresa_papeis_ref"),
        normalized_activity_date(F.col("e.dt_inicio_empresa")).alias("dt_inicio_empresa"),
        normalized_activity_date(F.col("e.dt_fim_empresa")).alias("dt_fim_empresa"),
        F.col("v.status").cast("string").alias("status_func_emp"),
        F.col("v.pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("v.ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .withColumn("empresa_nome_key", text_key_expr(F.coalesce(F.col("nm_empresa_ref"), F.col("nome_empresa_vinculo"))))
    .withColumn("consultoria_identity_key", F.coalesce(F.col("nu_cnpj_empresa"), F.col("empresa_nome_key")))
    .withColumn(
        "fl_empresa_consultoria_contexto",
        F.when(
            has_token("tp_empresa_ref", "CONSULTORIA") | has_token("tp_empresa_papeis_ref", "CONSULTORIA"),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
)

if table_exists(SILVER_TB_CLIENTE_REFERENCIA):
    df_cliente_ref = (
        spark.table(SILVER_TB_CLIENTE_REFERENCIA)
        .select(
            F.col("cliente_referencia_id").cast("bigint").alias("cliente_referencia_id"),
            F.col("nm_consultoria").cast("string").alias("nm_consultoria"),
            F.col("nm_cliente").cast("string").alias("nm_cliente"),
            normalized_activity_date(F.col("dt_inicio")).alias("dt_inicio_cliente_ref"),
            normalized_activity_date(F.col("dt_fim")).alias("dt_fim_cliente_ref"),
            F.col("tp_contrato").cast("string").alias("tp_contrato_cliente_ref"),
            F.col("fontes_origem").cast("string").alias("fontes_origem_cliente_ref"),
            F.col("observacao").cast("string").alias("observacao_cliente_ref"),
            F.col("fl_periodo_provisorio").cast("int").alias("fl_periodo_cliente_provisorio"),
        )
        .withColumn("consultoria_nome_key", text_key_expr(F.col("nm_consultoria")))
        .withColumn("cliente_nome_key", text_key_expr(F.col("nm_cliente")))
    )
else:
    df_cliente_ref = empty_df_for_schema(
        """
        cliente_referencia_id bigint,
        nm_consultoria string,
        nm_cliente string,
        dt_inicio_cliente_ref date,
        dt_fim_cliente_ref date,
        tp_contrato_cliente_ref string,
        fontes_origem_cliente_ref string,
        observacao_cliente_ref string,
        fl_periodo_cliente_provisorio int,
        consultoria_nome_key string,
        cliente_nome_key string
        """
    )

if table_exists(SILVER_TB_CTPS):
    df_ctps_empresa = (
        spark.table(SILVER_TB_CTPS)
        .withColumn("empresa_nome_key", text_key_expr(F.col("nome_empresa")))
        .groupBy("nu_cnpj_empresa", "empresa_nome_key")
        .agg(
            F.min("dt_inicio").cast("date").alias("dt_inicio_ctps"),
            F.max("dt_fim").cast("date").alias("dt_fim_ctps"),
            F.max("vl_salario").cast("double").alias("vl_salario_ctps"),
            F.max("cargo").cast("string").alias("cargo_ctps"),
            F.max("arquivo_origem").cast("string").alias("arquivo_origem_ctps"),
        )
    )
else:
    df_ctps_empresa = empty_df_for_schema(
        """
        nu_cnpj_empresa string,
        empresa_nome_key string,
        dt_inicio_ctps date,
        dt_fim_ctps date,
        vl_salario_ctps double,
        cargo_ctps string,
        arquivo_origem_ctps string
        """
    )

if table_exists(SILVER_TB_CONTRATOS):
    df_contratos_base = (
        spark.table(SILVER_TB_CONTRATOS)
        .select(
            normalized_tipo_contrato_expr(F.col("tp_vinculo")).alias("tp_vinculo"),
            F.col("cnpj_consultoria").cast("string").alias("cnpj_consultoria"),
            F.col("consultoria").cast("string").alias("consultoria"),
            F.col("empresa_cliente").cast("string").alias("empresa_cliente"),
            F.col("dt_inicio").cast("date").alias("dt_inicio"),
            F.col("dt_fim").cast("date").alias("dt_fim"),
            F.col("vl_valor_mensal").cast("double").alias("vl_valor_mensal"),
            F.col("cargo").cast("string").alias("cargo"),
            F.col("fl_clausula_exclusividade").cast("int").alias("fl_clausula_exclusividade"),
            F.col("fl_clausula_nao_concorrencia").cast("int").alias("fl_clausula_nao_concorrencia"),
            F.col("tp_clausula_exclusividade").cast("string").alias("tp_clausula_exclusividade"),
            F.col("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade"),
            F.col("criterio_extracao_clausula").cast("string").alias("criterio_extracao_clausula"),
            F.col("score_extracao_clausula").cast("int").alias("score_extracao_clausula"),
            F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        )
        .withColumn("consultoria_nome_key", text_key_expr(F.col("consultoria")))
        .withColumn("empresa_cliente_key", text_key_expr(F.col("empresa_cliente")))
        .withColumn("consultoria_identity_key", F.coalesce(F.col("cnpj_consultoria"), F.col("consultoria_nome_key")))
    )

    df_contrato_cliente = (
        df_contratos_base
        .groupBy("consultoria_identity_key", "empresa_cliente_key")
        .agg(
            F.min("dt_inicio").cast("date").alias("dt_inicio_contrato_cliente"),
            F.max("dt_fim").cast("date").alias("dt_fim_contrato_cliente"),
            F.max("vl_valor_mensal").cast("double").alias("vl_valor_mensal_contrato_cliente"),
            F.max("cargo").cast("string").alias("cargo_contrato_cliente"),
            F.max("fl_clausula_exclusividade").cast("int").alias("fl_clausula_exclusividade_cliente"),
            F.max("fl_clausula_nao_concorrencia").cast("int").alias("fl_clausula_nao_concorrencia_cliente"),
            F.max("tp_clausula_exclusividade").cast("string").alias("tp_clausula_exclusividade_cliente"),
            F.max("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade_cliente"),
            F.max("criterio_extracao_clausula").cast("string").alias("criterio_extracao_clausula_cliente"),
            F.max("score_extracao_clausula").cast("int").alias("score_extracao_clausula_cliente"),
            F.max("arquivo_origem").cast("string").alias("arquivo_origem_contrato_cliente"),
            F.max(F.when(F.col("tp_vinculo") == F.lit("CLT"), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_clt_cliente"),
            F.max(F.when(F.col("tp_vinculo") == F.lit("PJ"), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_pj_cliente"),
            F.max(F.when(F.col("tp_vinculo") == F.lit("COOPERATIVA"), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_cooperativa_cliente"),
        )
    )

    df_contrato_consultoria = (
        df_contratos_base
        .groupBy("consultoria_identity_key")
        .agg(
            F.min("dt_inicio").cast("date").alias("dt_inicio_contrato_consultoria"),
            F.max("dt_fim").cast("date").alias("dt_fim_contrato_consultoria"),
            F.max("vl_valor_mensal").cast("double").alias("vl_valor_mensal_contrato_consultoria"),
            F.max("cargo").cast("string").alias("cargo_contrato_consultoria"),
            F.max("fl_clausula_exclusividade").cast("int").alias("fl_clausula_exclusividade_consultoria"),
            F.max("fl_clausula_nao_concorrencia").cast("int").alias("fl_clausula_nao_concorrencia_consultoria"),
            F.max("tp_clausula_exclusividade").cast("string").alias("tp_clausula_exclusividade_consultoria"),
            F.max("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade_consultoria"),
            F.max("criterio_extracao_clausula").cast("string").alias("criterio_extracao_clausula_consultoria"),
            F.max("score_extracao_clausula").cast("int").alias("score_extracao_clausula_consultoria"),
            F.max("arquivo_origem").cast("string").alias("arquivo_origem_contrato_consultoria"),
            F.max(F.when(F.col("tp_vinculo") == F.lit("CLT"), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_clt_consultoria"),
            F.max(F.when(F.col("tp_vinculo") == F.lit("PJ"), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_pj_consultoria"),
            F.max(F.when(F.col("tp_vinculo") == F.lit("COOPERATIVA"), F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_cooperativa_consultoria"),
        )
    )
else:
    df_contrato_cliente = empty_df_for_schema(
        """
        consultoria_identity_key string,
        empresa_cliente_key string,
        dt_inicio_contrato_cliente date,
        dt_fim_contrato_cliente date,
        vl_valor_mensal_contrato_cliente double,
        cargo_contrato_cliente string,
        fl_clausula_exclusividade_cliente int,
        fl_clausula_nao_concorrencia_cliente int,
        tp_clausula_exclusividade_cliente string,
        trecho_clausula_exclusividade_cliente string,
        criterio_extracao_clausula_cliente string,
        score_extracao_clausula_cliente int,
        arquivo_origem_contrato_cliente string,
        fl_tem_clt_cliente int,
        fl_tem_pj_cliente int,
        fl_tem_cooperativa_cliente int
        """
    )
    df_contrato_consultoria = empty_df_for_schema(
        """
        consultoria_identity_key string,
        dt_inicio_contrato_consultoria date,
        dt_fim_contrato_consultoria date,
        vl_valor_mensal_contrato_consultoria double,
        cargo_contrato_consultoria string,
        fl_clausula_exclusividade_consultoria int,
        fl_clausula_nao_concorrencia_consultoria int,
        tp_clausula_exclusividade_consultoria string,
        trecho_clausula_exclusividade_consultoria string,
        criterio_extracao_clausula_consultoria string,
        score_extracao_clausula_consultoria int,
        arquivo_origem_contrato_consultoria string,
        fl_tem_clt_consultoria int,
        fl_tem_pj_consultoria int,
        fl_tem_cooperativa_consultoria int
        """
    )

df_contexto = (
    df_vinculo_base.alias("v")
    .join(
        df_cliente_ref.alias("cr"),
        (F.col("v.fl_empresa_consultoria_contexto") == F.lit(1))
        & (F.col("v.empresa_nome_key") == F.col("cr.consultoria_nome_key")),
        "left",
    )
    .join(
        df_ctps_empresa.alias("c_cnpj"),
        F.col("v.nu_cnpj_empresa") == F.col("c_cnpj.nu_cnpj_empresa"),
        "left",
    )
    .join(
        df_ctps_empresa.alias("c_nome"),
        F.col("v.empresa_nome_key") == F.col("c_nome.empresa_nome_key"),
        "left",
    )
    .join(
        df_contrato_cliente.alias("k_cliente"),
        (F.col("v.consultoria_identity_key") == F.col("k_cliente.consultoria_identity_key"))
        & (F.col("cr.cliente_nome_key") == F.col("k_cliente.empresa_cliente_key")),
        "left",
    )
    .join(
        df_contrato_consultoria.alias("k_consultoria"),
        F.col("v.consultoria_identity_key") == F.col("k_consultoria.consultoria_identity_key"),
        "left",
    )
    .select(
        F.col("v.*"),
        F.col("cr.cliente_referencia_id").alias("cliente_referencia_id"),
        F.col("cr.nm_consultoria").alias("nm_consultoria_ref"),
        F.col("cr.nm_cliente").alias("nm_cliente"),
        F.col("cr.dt_inicio_cliente_ref").alias("dt_inicio_cliente_ref"),
        F.col("cr.dt_fim_cliente_ref").alias("dt_fim_cliente_ref"),
        F.col("cr.tp_contrato_cliente_ref").alias("tp_contrato_cliente_ref"),
        F.col("cr.fontes_origem_cliente_ref").alias("fontes_origem_cliente_ref"),
        F.col("cr.observacao_cliente_ref").alias("observacao_cliente_ref"),
        F.col("cr.fl_periodo_cliente_provisorio").alias("fl_periodo_cliente_provisorio"),
        F.col("cr.consultoria_nome_key").alias("consultoria_nome_key_ref"),
        F.col("cr.cliente_nome_key").alias("cliente_nome_key_ref"),
        F.col("c_cnpj.dt_inicio_ctps").alias("dt_inicio_ctps_cnpj"),
        F.col("c_cnpj.dt_fim_ctps").alias("dt_fim_ctps_cnpj"),
        F.col("c_cnpj.vl_salario_ctps").alias("vl_salario_ctps_cnpj"),
        F.col("c_cnpj.cargo_ctps").alias("cargo_ctps_cnpj"),
        F.col("c_cnpj.arquivo_origem_ctps").alias("arquivo_origem_ctps_cnpj"),
        F.col("c_nome.dt_inicio_ctps").alias("dt_inicio_ctps_nome"),
        F.col("c_nome.dt_fim_ctps").alias("dt_fim_ctps_nome"),
        F.col("c_nome.vl_salario_ctps").alias("vl_salario_ctps_nome"),
        F.col("c_nome.cargo_ctps").alias("cargo_ctps_nome"),
        F.col("c_nome.arquivo_origem_ctps").alias("arquivo_origem_ctps_nome"),
        F.col("k_cliente.dt_inicio_contrato_cliente").alias("dt_inicio_contrato_cliente"),
        F.col("k_cliente.dt_fim_contrato_cliente").alias("dt_fim_contrato_cliente"),
        F.col("k_cliente.vl_valor_mensal_contrato_cliente").alias("vl_valor_mensal_contrato_cliente"),
        F.col("k_cliente.cargo_contrato_cliente").alias("cargo_contrato_cliente"),
        F.col("k_cliente.fl_clausula_exclusividade_cliente").alias("fl_clausula_exclusividade_cliente"),
        F.col("k_cliente.fl_clausula_nao_concorrencia_cliente").alias("fl_clausula_nao_concorrencia_cliente"),
        F.col("k_cliente.tp_clausula_exclusividade_cliente").alias("tp_clausula_exclusividade_cliente"),
        F.col("k_cliente.trecho_clausula_exclusividade_cliente").alias("trecho_clausula_exclusividade_cliente"),
        F.col("k_cliente.criterio_extracao_clausula_cliente").alias("criterio_extracao_clausula_cliente"),
        F.col("k_cliente.score_extracao_clausula_cliente").alias("score_extracao_clausula_cliente"),
        F.col("k_cliente.arquivo_origem_contrato_cliente").alias("arquivo_origem_contrato_cliente"),
        F.col("k_cliente.fl_tem_clt_cliente").alias("fl_tem_clt_cliente"),
        F.col("k_cliente.fl_tem_pj_cliente").alias("fl_tem_pj_cliente"),
        F.col("k_consultoria.dt_inicio_contrato_consultoria").alias("dt_inicio_contrato_consultoria"),
        F.col("k_consultoria.dt_fim_contrato_consultoria").alias("dt_fim_contrato_consultoria"),
        F.col("k_consultoria.vl_valor_mensal_contrato_consultoria").alias("vl_valor_mensal_contrato_consultoria"),
        F.col("k_consultoria.cargo_contrato_consultoria").alias("cargo_contrato_consultoria"),
        F.col("k_consultoria.fl_clausula_exclusividade_consultoria").alias("fl_clausula_exclusividade_consultoria"),
        F.col("k_consultoria.fl_clausula_nao_concorrencia_consultoria").alias("fl_clausula_nao_concorrencia_consultoria"),
        F.col("k_consultoria.tp_clausula_exclusividade_consultoria").alias("tp_clausula_exclusividade_consultoria"),
        F.col("k_consultoria.trecho_clausula_exclusividade_consultoria").alias("trecho_clausula_exclusividade_consultoria"),
        F.col("k_consultoria.criterio_extracao_clausula_consultoria").alias("criterio_extracao_clausula_consultoria"),
        F.col("k_consultoria.score_extracao_clausula_consultoria").alias("score_extracao_clausula_consultoria"),
        F.col("k_consultoria.arquivo_origem_contrato_consultoria").alias("arquivo_origem_contrato_consultoria"),
        F.col("k_consultoria.fl_tem_clt_consultoria").alias("fl_tem_clt_consultoria"),
        F.col("k_consultoria.fl_tem_pj_consultoria").alias("fl_tem_pj_consultoria"),
    )
    .withColumn(
        "dt_inicio",
        F.coalesce(
            F.col("dt_inicio_ctps_cnpj"),
            F.col("dt_inicio_ctps_nome"),
            F.col("dt_inicio_contrato_cliente"),
            F.col("dt_inicio_contrato_consultoria"),
            F.col("dt_inicio_empresa"),
        ),
    )
    .withColumn(
        "dt_fim",
        normalized_activity_date(
            F.coalesce(
                F.col("dt_fim_ctps_cnpj"),
                F.col("dt_fim_ctps_nome"),
                F.col("dt_fim_contrato_cliente"),
                F.col("dt_fim_contrato_consultoria"),
                F.col("dt_fim_empresa"),
            )
        ),
    )
    .withColumn(
        "dt_inicio_cliente",
        normalized_activity_date(
            F.coalesce(
                F.col("dt_inicio_cliente_ref"),
                F.col("dt_inicio_contrato_cliente"),
            )
        ),
    )
    .withColumn(
        "dt_fim_cliente",
        normalized_activity_date(
            F.coalesce(
                F.col("dt_fim_cliente_ref"),
                F.col("dt_fim_contrato_cliente"),
            )
        ),
    )
    .withColumn(
        "vl_remuneracao",
        F.coalesce(
            F.col("vl_salario_ctps_cnpj"),
            F.col("vl_salario_ctps_nome"),
            F.col("vl_valor_mensal_contrato_cliente"),
            F.col("vl_valor_mensal_contrato_consultoria"),
        ),
    )
    .withColumn(
        "cargo_resolvido",
        F.coalesce(
            F.col("cargo_ctps_cnpj"),
            F.col("cargo_ctps_nome"),
            F.col("cargo_contrato_cliente"),
            F.col("cargo_contrato_consultoria"),
        ),
    )
    .withColumn(
        "tp_vinculo_resolvido",
        F.when(F.col("dt_inicio_ctps_cnpj").isNotNull() | F.col("dt_inicio_ctps_nome").isNotNull(), F.lit("CLT"))
        .when((F.col("fl_tem_clt_cliente") == 1) & (F.coalesce(F.col("fl_tem_pj_cliente"), F.lit(0)) == 0), F.lit("CLT"))
        .when((F.col("fl_tem_cooperativa_cliente") == 1) & (F.coalesce(F.col("fl_tem_clt_cliente"), F.lit(0)) == 0), F.lit("COOPERATIVA"))
        .when((F.col("fl_tem_pj_cliente") == 1) & (F.coalesce(F.col("fl_tem_clt_cliente"), F.lit(0)) == 0), F.lit("PJ"))
        .when((F.col("fl_tem_clt_consultoria") == 1) & (F.coalesce(F.col("fl_tem_pj_consultoria"), F.lit(0)) == 0), F.lit("CLT"))
        .when((F.col("fl_tem_cooperativa_consultoria") == 1) & (F.coalesce(F.col("fl_tem_clt_consultoria"), F.lit(0)) == 0), F.lit("COOPERATIVA"))
        .when((F.col("fl_tem_pj_consultoria") == 1) & (F.coalesce(F.col("fl_tem_clt_consultoria"), F.lit(0)) == 0), F.lit("PJ"))
        .when(F.upper(F.coalesce(F.col("tp_contrato_cliente_ref"), F.lit(""))).contains("CLT") & ~F.upper(F.coalesce(F.col("tp_contrato_cliente_ref"), F.lit(""))).contains("PJ"), F.lit("CLT"))
        .when(F.upper(F.coalesce(F.col("tp_contrato_cliente_ref"), F.lit(""))).contains("COOPER"), F.lit("COOPERATIVA"))
        .when(F.upper(F.coalesce(F.col("tp_contrato_cliente_ref"), F.lit(""))).contains("PJ") & ~F.upper(F.coalesce(F.col("tp_contrato_cliente_ref"), F.lit(""))).contains("CLT"), F.lit("PJ"))
        .otherwise(F.lit("NAO_INFORMADO")),
    )
    .withColumn(
        "fl_exclusividade_contrato",
        F.when(
            suspicious_exclusivity_text("trecho_clausula_exclusividade_cliente")
            | suspicious_exclusivity_text("trecho_clausula_exclusividade_consultoria"),
            F.lit(0),
        ).otherwise(
            F.coalesce(
                F.col("fl_clausula_exclusividade_cliente"),
                F.col("fl_clausula_exclusividade_consultoria"),
                F.lit(0),
            )
        ),
    )
    .withColumn(
        "fl_nao_concorrencia",
        F.coalesce(
            F.col("fl_clausula_nao_concorrencia_cliente"),
            F.col("fl_clausula_nao_concorrencia_consultoria"),
            F.lit(0),
        ),
    )
    .withColumn(
        "tp_clausula_exclusividade",
        F.when(
            suspicious_exclusivity_text("trecho_clausula_exclusividade_cliente")
            | suspicious_exclusivity_text("trecho_clausula_exclusividade_consultoria"),
            F.lit("NAO_INFORMADO"),
        ).otherwise(
            F.coalesce(
                F.col("tp_clausula_exclusividade_cliente"),
                F.col("tp_clausula_exclusividade_consultoria"),
            )
        ),
    )
    .withColumn(
        "trecho_clausula_exclusividade",
        F.coalesce(
            F.col("trecho_clausula_exclusividade_cliente"),
            F.col("trecho_clausula_exclusividade_consultoria"),
        ),
    )
    .withColumn(
        "criterio_extracao_clausula",
        F.coalesce(
            F.col("criterio_extracao_clausula_cliente"),
            F.col("criterio_extracao_clausula_consultoria"),
        ),
    )
    .withColumn(
        "score_extracao_clausula",
        F.coalesce(
            F.col("score_extracao_clausula_cliente"),
            F.col("score_extracao_clausula_consultoria"),
            F.lit(0),
        ),
    )
    .withColumn(
        "arquivo_origem_contrato",
        F.coalesce(
            F.col("arquivo_origem_contrato_cliente"),
            F.col("arquivo_origem_contrato_consultoria"),
        ),
    )
    .withColumn(
        "arquivo_origem_ctps",
        F.coalesce(F.col("arquivo_origem_ctps_cnpj"), F.col("arquivo_origem_ctps_nome")),
    )
    .withColumn(
        "fonte_vinculo",
        F.when(F.col("arquivo_origem_ctps").isNotNull() & F.col("arquivo_origem_contrato").isNotNull(), F.lit("CTPS_CONTRATO"))
        .when(F.col("arquivo_origem_ctps").isNotNull(), F.lit("CTPS"))
        .when(F.col("arquivo_origem_contrato").isNotNull(), F.lit("CONTRATO"))
        .otherwise(F.lit("FUNCIONARIO_EMPRESA")),
    )
    .withColumn(
        "nivel_confianca_vinculo",
        F.when(F.col("arquivo_origem_ctps").isNotNull(), F.lit("ALTA"))
        .when(F.col("arquivo_origem_contrato").isNotNull(), F.lit("MEDIA"))
        .otherwise(F.lit("BAIXA")),
    )
    .withColumn(
        "fonte_cliente_contexto",
        F.when(F.col("cliente_referencia_id").isNotNull() & F.col("arquivo_origem_contrato").isNotNull(), F.lit("CLIENTE_REFERENCIA_CONTRATO"))
        .when(F.col("cliente_referencia_id").isNotNull(), F.lit("CLIENTE_REFERENCIA"))
        .when(F.col("arquivo_origem_contrato").isNotNull() & F.col("nm_cliente").isNotNull(), F.lit("CONTRATO"))
        .otherwise(F.lit(None).cast("string")),
    )
    .withColumn(
        "fl_validacao_minima_vinculo",
        F.when(F.col("sk_pessoa").isNotNull() & F.col("empresa_id").isNotNull() & F.col("dt_inicio").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )
    .withColumn(
        "sk_tipo_contrato",
        F.when(F.col("tp_vinculo_resolvido") == F.lit("CLT"), F.lit(sk_tipo_contrato_clt))
        .when(F.col("tp_vinculo_resolvido") == F.lit("COOPERATIVA"), F.lit(sk_tipo_contrato_cooperativa))
        .when(F.col("tp_vinculo_resolvido") == F.lit("PJ"), F.lit(sk_tipo_contrato_pj))
        .otherwise(F.lit(default_sk_tipo_contrato))
        .cast("bigint"),
    )
    .withColumn(
        "cliente_nome_key_resolvida",
        F.coalesce(F.col("cliente_nome_key_ref"), text_key_expr(F.col("nm_cliente"))),
    )
)

df_match = (
    df_contexto
    .join(df_cliente_dim, F.col("cliente_nome_key_resolvida") == F.col("cliente_nome_key_dim"), "left")
    .join(df_cargo, F.upper(F.col("cargo_resolvido")) == F.col("cargo_dim"), "left")
    .join(df_data_inicio, F.col("dt_inicio") == F.col("dt_inicio_ref"), "left")
    .join(df_data_fim, F.col("dt_fim") == F.col("dt_fim_ref"), "left")
    .withColumn(
        "fl_cliente_contexto_incompativel",
        F.when(
            F.col("dt_inicio_cliente").isNotNull()
            & F.col("dt_inicio").isNotNull()
            & (
                (F.coalesce(F.col("dt_fim_cliente"), F.to_date(F.lit("2999-12-31"))) < F.col("dt_inicio"))
                | (F.col("dt_inicio_cliente") > F.coalesce(F.col("dt_fim"), F.to_date(F.lit("2999-12-31"))))
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "cliente_id",
        F.when(F.col("fl_cliente_contexto_incompativel") == 1, F.lit(None).cast("bigint")).otherwise(F.col("cliente_id")),
    )
    .withColumn(
        "cliente_referencia_id",
        F.when(F.col("fl_cliente_contexto_incompativel") == 1, F.lit(None).cast("bigint")).otherwise(F.col("cliente_referencia_id")),
    )
    .withColumn(
        "dt_inicio_cliente",
        F.when(F.col("fl_cliente_contexto_incompativel") == 1, F.lit(None).cast("date")).otherwise(F.col("dt_inicio_cliente")),
    )
    .withColumn(
        "dt_fim_cliente",
        F.when(F.col("fl_cliente_contexto_incompativel") == 1, F.lit(None).cast("date")).otherwise(F.col("dt_fim_cliente")),
    )
    .withColumn(
        "fl_periodo_cliente_provisorio",
        F.when(F.col("fl_cliente_contexto_incompativel") == 1, F.lit(None).cast("int")).otherwise(F.col("fl_periodo_cliente_provisorio")),
    )
)

w_vinculo_best = Window.partitionBy("funcionario_empresa_id").orderBy(
    F.col("fl_cliente_contexto_incompativel").asc(),
    F.col("cliente_id").isNull().asc(),
    F.col("cliente_referencia_id").isNull().asc(),
    F.coalesce(F.col("fl_periodo_cliente_provisorio"), F.lit(0)).asc(),
    F.col("dt_inicio_cliente").isNull().asc(),
    F.col("dt_fim_cliente").isNull().asc(),
    F.col("ingested_at_origem").desc_nulls_last(),
)

df_gold = (
    df_match
    .withColumn("rn_vinculo_best", F.row_number().over(w_vinculo_best))
    .filter(F.col("rn_vinculo_best") == 1)
    .drop("rn_vinculo_best")
    .withColumn(
        "consultoria_empresa_id",
        F.when(F.col("fl_empresa_consultoria_contexto") == 1, F.col("empresa_id")).otherwise(F.lit(None).cast("bigint")),
    )
    .withColumn(
        "cliente_contexto_id",
        F.concat_ws(
            " | ",
            F.when(F.col("cliente_id").isNotNull(), F.concat(F.lit("DIM|"), F.col("cliente_id").cast("string"))),
            F.when(F.col("cliente_referencia_id").isNotNull(), F.concat(F.lit("REF|"), F.col("cliente_referencia_id").cast("string"))),
        ),
    )
    .select(
        F.col("funcionario_empresa_id").cast("bigint").alias("sk_vinculo"),
        F.col("sk_pessoa").cast("bigint").alias("sk_pessoa"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("consultoria_empresa_id").cast("bigint").alias("consultoria_empresa_id"),
        F.when(F.col("cliente_contexto_id") != "", F.col("cliente_contexto_id")).otherwise(F.lit(None).cast("string")).alias("cliente_contexto_id"),
        F.col("fonte_cliente_contexto").cast("string").alias("fonte_cliente_contexto"),
        F.col("dt_inicio_cliente").cast("date").alias("dt_inicio_cliente"),
        F.col("dt_fim_cliente").cast("date").alias("dt_fim_cliente"),
        F.col("sk_cargo").cast("bigint").alias("sk_cargo"),
        F.col("sk_tipo_contrato").cast("bigint").alias("sk_tipo_contrato"),
        F.col("sk_data_inicio").cast("int").alias("sk_data_inicio"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("sk_data_fim").cast("int").alias("sk_data_fim"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.round(F.col("vl_remuneracao"), 2).cast("double").alias("vl_remuneracao"),
        F.col("fl_exclusividade_contrato").cast("int").alias("fl_exclusividade_contrato"),
        F.col("fl_nao_concorrencia").cast("int").alias("fl_nao_concorrencia"),
        F.col("tp_clausula_exclusividade").cast("string").alias("tp_clausula_exclusividade"),
        F.col("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade"),
        F.col("criterio_extracao_clausula").cast("string").alias("criterio_extracao_clausula"),
        F.col("score_extracao_clausula").cast("int").alias("score_extracao_clausula"),
        F.coalesce(F.col("arquivo_origem_contrato"), F.col("arquivo_origem_ctps")).cast("string").alias("doc_id"),
        F.col("fonte_vinculo").cast("string").alias("fonte_vinculo"),
        F.col("fl_validacao_minima_vinculo").cast("int").alias("fl_validacao_minima_vinculo"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_VINCULO_EMPRESA} (
    sk_vinculo BIGINT,
    sk_pessoa BIGINT,
    empresa_id BIGINT,
    consultoria_empresa_id BIGINT,
    cliente_contexto_id STRING,
    fonte_cliente_contexto STRING,
    dt_inicio_cliente DATE,
    dt_fim_cliente DATE,
    sk_cargo BIGINT,
    sk_tipo_contrato BIGINT,
    sk_data_inicio INT,
    dt_inicio DATE,
    sk_data_fim INT,
    dt_fim DATE,
    vl_remuneracao DOUBLE,
    fl_exclusividade_contrato INT,
    fl_nao_concorrencia INT,
    tp_clausula_exclusividade STRING,
    trecho_clausula_exclusividade STRING,
    criterio_extracao_clausula STRING,
    score_extracao_clausula INT,
    doc_id STRING,
    fonte_vinculo STRING,
    fl_validacao_minima_vinculo INT,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_VINCULO_EMPRESA)
optimize_table(
    GOLD_FATO_VINCULO_EMPRESA,
    zorder_cols=["empresa_id", "sk_pessoa", "sk_tipo_contrato", "dt_inicio"],
)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_VINCULO_EMPRESA)
