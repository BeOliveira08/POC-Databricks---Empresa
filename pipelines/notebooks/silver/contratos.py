# Databricks notebook source
# silver: tb_contratos

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------
# MAGIC %run ../../utils/company

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_CONTRATOS = f"{CATALOG}.{BRONZE}.raw_contratos"
SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
TITULAR_NOME_PRESTADOR = "TITULAR GENERICO"
TITULAR_CPF_PRESTADOR = "00000000000"

money_udf = F.udf(br_money_to_float, "double")
date_udf = F.udf(parse_flexible_date_to_iso, "string")


def canonical_company_name(nome_raw, cnpj_raw):
    if nome_raw is None:
        return None
    nome_txt = str(nome_raw).strip()
    if not nome_txt or nome_txt.upper() in {"NAO_INFORMADO", "NULL", "-"}:
        return None
    nome_canonico, _, matched = resolve_empresa_alias(nome_txt, cnpj_raw)
    if matched:
        return nome_canonico
    nome_norm = normalize_company_text(nome_txt)
    return nome_norm or None


canonical_company_udf = F.udf(canonical_company_name, "string")

if not table_exists(BRONZE_RAW_CONTRATOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_RAW_CONTRATOS}")


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


def cleaned_empresa_expr(col_name: str):
    cleaned = normalized_text_expr(col_name)
    cleaned = F.regexp_replace(cleaned, r"\{[^}]*\}", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCARTA PROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA COMERCIAL\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\b\d{4,}\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)
    return F.when(cleaned == "", F.lit(None).cast("string")).otherwise(cleaned)


def nullable_text_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt == "") |
        txt.isin("NAO INFORMADO", "NÃO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def parse_date_expr(col_name: str):
    parsed = F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")
    return F.when(parsed <= F.lit("1900-01-01").cast("date"), F.lit(None).cast("date")).otherwise(parsed)


def normalized_status_expr():
    status = normalized_text_expr("status")
    return (
        F.when(status.isin("ABERTO", "ATIVO", "VIGENTE"), F.lit("ABERTO"))
         .when(status.isin("FECHADO", "ENCERRADO", "FINALIZADO", "RESCINDIDO"), F.lit("FECHADO"))
         .otherwise(F.lit(None).cast("string"))
    )


def sane_valor_expr(col_name: str):
    valor = money_udf(col_name)
    return (
        F.when(valor.isNull(), F.lit(None).cast("double"))
         .when(valor >= F.lit(100000.0), F.round(valor / F.lit(100.0), 2))
         .otherwise(F.round(valor, 2))
    )


def senioridade_expr():
    cargo = F.upper(F.coalesce(repair_text("cargo"), F.lit("")))
    return (
        F.when(cargo.rlike(r"\b(JR|JUNIOR)\b"), F.lit("JUNIOR"))
         .when(cargo.rlike(r"\b(SR|SENIOR|SÊNIOR)\b"), F.lit("SENIOR"))
         .when(cargo.rlike(r"\b(PL|PLENO)\b"), F.lit("PLENO"))
         .when(cargo.rlike(r"\b(ESPECIALISTA|LEAD|LIDER|COORDENADOR|GERENTE|HEAD|PRINCIPAL|STAFF)\b"), F.lit("ESPECIALISTA"))
         .otherwise(F.lit(None).cast("string"))
    )


def name_key_expr(col_name: str):
    txt = F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\b\d{6,}\b", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9]", " ")
    txt = F.regexp_replace(txt, r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU)\b", " ")
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def company_anchor_key_expr(col_name: str):
    txt = F.upper(F.coalesce(F.col(col_name).cast("string"), F.lit("")))
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
    txt = F.regexp_replace(txt, r"\s+", " ")
    txt = F.trim(txt)
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt).otherwise(name_key_expr(col_name))


def is_titular_marker_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return (
        txt.rlike(r"\bTITULAR\b")
        & txt.rlike(r"\bJUNIOR\b")
        & txt.rlike(r"\bCOSTA\b")
        & txt.rlike(r"\bGENERICO\b")
    )


df_base = (
    spark.table(BRONZE_RAW_CONTRATOS)
    .select(
        F.coalesce(nullable_text_expr("fonte_referencia"), F.lit("RAW_CONTRATOS")).alias("fonte"),
        nullable_text_expr("tp_vinculo").alias("tp_vinculo"),
        cleaned_empresa_expr("empresa_prestador").alias("empresa_prestador_origem"),
        normalize_cnpj_expr(only_digits_expr("cnpj_prestador")).alias("cnpj_prestador_origem"),
        cleaned_empresa_expr("consultoria").alias("consultoria"),
        normalize_cnpj_expr(only_digits_expr("cnpj_consultoria")).alias("cnpj_consultoria"),
        cleaned_empresa_expr("empresa_cliente").alias("empresa_cliente"),
        nullable_text_expr("cargo").alias("cargo"),
        parse_date_expr("dt_inicio").alias("dt_inicio"),
        parse_date_expr("dt_fim").alias("dt_fim"),
        sane_valor_expr("valor_mensal").alias("vl_valor_mensal"),
        nullable_text_expr("status").alias("status"),
        F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_origem_referencia"), F.col("arquivo_pdf_validacao")).cast("string").alias("arquivo_origem"),
        F.col("arquivo_pdf_validacao").cast("string").alias("arquivo_pdf_validacao"),
        F.col("fl_arquivo_pdf_existe").cast("boolean").alias("fl_arquivo_pdf_existe"),
        nullable_text_expr("tp_clausula_exclusividade").alias("tp_clausula_exclusividade"),
        F.col("fl_clausula_exclusividade").cast("int").alias("fl_clausula_exclusividade"),
        F.col("fl_clausula_nao_concorrencia").cast("int").alias("fl_clausula_nao_concorrencia"),
        repair_text("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade"),
        nullable_text_expr("criterio_extracao_clausula").alias("criterio_extracao_clausula"),
        F.col("score_extracao_clausula").cast("int").alias("score_extracao_clausula"),
        nullable_text_expr("criterio_match_documento").alias("criterio_match_documento"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .withColumn(
        "nome_prestador",
        F.when(
            (F.upper(F.coalesce(F.col("tp_vinculo"), F.lit(""))) == F.lit("CLT"))
            | is_titular_marker_expr("empresa_prestador_origem"),
            F.lit(TITULAR_NOME_PRESTADOR),
        ).otherwise(F.col("empresa_prestador_origem"))
    )
    .withColumn(
        "cpf_cnpj_prestador",
        F.when(
            (F.upper(F.coalesce(F.col("tp_vinculo"), F.lit(""))) == F.lit("CLT"))
            | is_titular_marker_expr("empresa_prestador_origem"),
            F.lit(TITULAR_CPF_PRESTADOR),
        ).otherwise(F.col("cnpj_prestador_origem"))
    )
    .withColumn("senioridade", senioridade_expr())
    .withColumn(
        "cargo",
        F.when(F.trim(F.coalesce(F.col("cargo"), F.lit(""))) == "", F.lit("ENGENHEIRO DE DADOS"))
         .otherwise(F.col("cargo"))
    )
    .withColumn(
        "senioridade",
        F.when(F.trim(F.coalesce(F.col("senioridade"), F.lit(""))) == "", F.lit("PLENO"))
         .otherwise(F.col("senioridade"))
    )
    .filter(
        F.col("empresa_cliente").isNotNull()
        | F.col("nome_prestador").isNotNull()
        | F.col("consultoria").isNotNull()
        | F.col("cpf_cnpj_prestador").isNotNull()
        | F.col("cnpj_consultoria").isNotNull()
    )
    .withColumn(
        "dt_fim",
        F.when(
            F.col("dt_inicio").isNotNull() & F.col("dt_fim").isNotNull() & (F.col("dt_fim") < F.col("dt_inicio")),
            F.lit(None).cast("date"),
        ).otherwise(F.col("dt_fim"))
    )
    .withColumn("status_normalizado", normalized_status_expr())
    .withColumn(
        "status",
        F.when(
            F.col("dt_fim").isNotNull() & (F.col("dt_fim") <= F.current_date()),
            F.lit("FECHADO"),
        ).when(
            F.col("status_normalizado").isNotNull(),
            F.col("status_normalizado"),
        ).when(
            F.col("dt_inicio").isNotNull(),
            F.lit("ABERTO"),
        ).otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "consultoria",
        F.coalesce(
            canonical_company_udf(F.col("consultoria"), F.col("cnpj_consultoria")),
            F.col("consultoria"),
        )
    )
    .withColumn("empresa_cliente_key", company_anchor_key_expr("empresa_cliente"))
    .withColumn("consultoria_key", company_anchor_key_expr("consultoria"))
    .withColumn("consultoria_identity_key", F.coalesce(F.col("cnpj_consultoria"), F.col("consultoria_key")))
    .withColumn("prestador_key", company_anchor_key_expr("nome_prestador"))
    .withColumn("cargo_key", company_anchor_key_expr("cargo"))
    .withColumn("dt_inicio_key", F.coalesce(F.col("dt_inicio").cast("string"), F.lit("")))
    .withColumn(
        "shadow_exact_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("consultoria_identity_key"), F.lit("")),
            F.coalesce(F.col("dt_inicio_key"), F.lit("")),
        ),
    )
    .withColumn(
        "dedup_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("tp_vinculo"), F.lit("")),
            F.coalesce(F.col("consultoria_identity_key"), F.lit("")),
            F.coalesce(F.col("cpf_cnpj_prestador"), F.lit("")),
            F.coalesce(F.col("prestador_key"), F.lit("")),
            F.coalesce(F.col("empresa_cliente_key"), F.lit("")),
            F.coalesce(F.col("cargo"), F.lit("")),
            F.coalesce(F.col("dt_inicio").cast("string"), F.lit("")),
        ),
    )
    .withColumn(
        "score_preenchimento",
        F.when(F.col("empresa_cliente").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("consultoria").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("nome_prestador").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("cargo").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("senioridade").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("dt_inicio").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("dt_fim").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("vl_valor_mensal").isNotNull() & (F.col("vl_valor_mensal") > 0), F.lit(2)).otherwise(F.lit(0)) +
        F.when(F.col("status").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("arquivo_pdf_validacao").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
        F.when(F.col("tp_clausula_exclusividade").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("score_extracao_clausula").isNotNull() & (F.col("score_extracao_clausula") > 0), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "fonte_prioridade",
        F.when(F.col("fonte") == "PLANILHA_CLT", F.lit(0))
         .when(F.col("fonte") == "PLANILHA_PJ", F.lit(1))
         .when(F.col("fonte") == "DOC_ONLY", F.lit(2))
         .otherwise(F.lit(3))
    )
    .withColumn(
        "fl_fonte_planilha",
        F.when(F.col("fonte").isin("PLANILHA_CLT", "PLANILHA_PJ"), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn("base_row_id", F.monotonically_increasing_id())
)

w_shadow_exact = Window.partitionBy("shadow_exact_key")
w_shadow_company = Window.partitionBy("consultoria_identity_key")

df_base = (
    df_base
    .withColumn("fl_tem_planilha_mesma_data", F.max("fl_fonte_planilha").over(w_shadow_exact))
    .withColumn("fl_tem_planilha_mesma_empresa", F.max("fl_fonte_planilha").over(w_shadow_company))
    .filter(
        ~(
            (F.col("fonte") == "DOC_ONLY")
            & (
                (F.col("fl_tem_planilha_mesma_data") == 1)
                | (
                    F.col("dt_inicio").isNull()
                    & (F.col("fl_tem_planilha_mesma_empresa") == 1)
                )
            )
        )
    )
)

df_planilha_truth = (
    df_base
    .filter(F.col("fl_fonte_planilha") == 1)
    .select(
        F.col("base_row_id").alias("plan_row_id"),
        F.col("tp_vinculo").alias("plan_tp_vinculo"),
        F.col("consultoria_identity_key").alias("plan_consultoria_identity_key"),
        F.col("dt_inicio").alias("plan_dt_inicio"),
        F.col("score_preenchimento").alias("plan_score_preenchimento"),
    )
    .filter(F.col("plan_consultoria_identity_key").isNotNull())
)

df_doc_shadow_ids = (
    df_base.alias("doc")
    .join(
        df_planilha_truth.alias("plan"),
        (
            (F.col("doc.fonte") == "DOC_ONLY")
            & F.col("doc.consultoria_identity_key").isNotNull()
            & (F.col("doc.tp_vinculo") == F.col("plan.plan_tp_vinculo"))
            & (F.col("doc.consultoria_identity_key") == F.col("plan.plan_consultoria_identity_key"))
            & (F.col("plan.plan_score_preenchimento") >= F.col("doc.score_preenchimento"))
            & (
                F.col("doc.dt_inicio").isNull()
                | F.col("plan.plan_dt_inicio").isNull()
                | (F.abs(F.datediff(F.col("doc.dt_inicio"), F.col("plan.plan_dt_inicio"))) <= 45)
            )
        ),
        "inner",
    )
    .select(F.col("doc.base_row_id").alias("base_row_id"))
    .dropDuplicates()
)

df_base = df_base.join(df_doc_shadow_ids, "base_row_id", "left_anti")

df_planilha_weak_shadow_ids = (
    df_base.alias("weak")
    .join(
        df_base.alias("strong"),
        (
            (F.col("weak.fl_fonte_planilha") == 1)
            & (F.col("strong.fl_fonte_planilha") == 1)
            & (F.col("weak.base_row_id") != F.col("strong.base_row_id"))
            & (F.col("weak.tp_vinculo") == F.col("strong.tp_vinculo"))
            & F.col("weak.consultoria_identity_key").isNotNull()
            & (F.col("weak.consultoria_identity_key") == F.col("strong.consultoria_identity_key"))
            & (F.coalesce(F.col("weak.prestador_key"), F.lit("")) == F.coalesce(F.col("strong.prestador_key"), F.lit("")))
            & (F.coalesce(F.col("weak.cargo_key"), F.lit("")) == F.coalesce(F.col("strong.cargo_key"), F.lit("")))
            & (F.col("strong.score_preenchimento") > F.col("weak.score_preenchimento"))
            & (F.col("strong.vl_valor_mensal") > 0)
            & F.col("strong.dt_inicio").isNotNull()
            & (
                F.col("weak.dt_inicio").isNull()
                | (F.abs(F.datediff(F.col("weak.dt_inicio"), F.col("strong.dt_inicio"))) <= 31)
            )
            & (
                F.col("weak.vl_valor_mensal").isNull()
                | (F.col("weak.vl_valor_mensal") <= 0)
                | F.col("weak.dt_fim").isNull()
                | F.col("weak.empresa_cliente").isNull()
                | (F.col("weak.status") == "ABERTO")
            )
        ),
        "inner",
    )
    .select(F.col("weak.base_row_id").alias("base_row_id"))
    .dropDuplicates()
)

df_base = df_base.join(df_planilha_weak_shadow_ids, "base_row_id", "left_anti")

w = Window.partitionBy("dedup_key").orderBy(
    F.col("fonte_prioridade").asc(),
    F.col("score_preenchimento").desc(),
    F.when(F.col("arquivo_pdf_validacao").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("ingested_at_origem").desc_nulls_last(),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_id_contrato = Window.orderBy(
    F.coalesce(F.col("tp_vinculo"), F.lit("")),
    F.coalesce(F.col("consultoria"), F.lit("")),
    F.coalesce(F.col("nome_prestador"), F.lit("")),
    F.coalesce(F.col("empresa_cliente"), F.lit("")),
    F.coalesce(F.col("cargo"), F.lit("")),
    F.coalesce(F.col("dt_inicio").cast("string"), F.lit("")),
    F.coalesce(F.col("arquivo_origem"), F.lit("")),
)

df_final = (
    df_base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop(
        "rn",
        "empresa_cliente_key",
        "consultoria_key",
        "consultoria_identity_key",
        "prestador_key",
        "cargo_key",
        "dt_inicio_key",
        "shadow_exact_key",
        "dedup_key",
        "score_preenchimento",
        "fonte_prioridade",
        "fl_fonte_planilha",
        "fl_tem_planilha_mesma_data",
        "fl_tem_planilha_mesma_empresa",
        "base_row_id",
        "status_normalizado",
        "empresa_prestador_origem",
        "cnpj_prestador_origem",
    )
    .withColumn("id_contrato", F.row_number().over(w_id_contrato))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_contrato").cast("int").alias("id_contrato"),
        F.col("fonte").cast("string").alias("fonte"),
        F.col("tp_vinculo").cast("string").alias("tp_vinculo"),
        F.col("nome_prestador").cast("string").alias("nome_prestador"),
        F.col("cpf_cnpj_prestador").cast("string").alias("cpf_cnpj_prestador"),
        F.col("consultoria").cast("string").alias("consultoria"),
        F.col("cnpj_consultoria").cast("string").alias("cnpj_consultoria"),
        F.col("empresa_cliente").cast("string").alias("empresa_cliente"),
        F.col("cargo").cast("string").alias("cargo"),
        F.col("senioridade").cast("string").alias("senioridade"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.col("vl_valor_mensal").cast("double").alias("vl_valor_mensal"),
        F.col("status").cast("string").alias("status"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("fl_arquivo_pdf_existe").cast("boolean").alias("fl_arquivo_pdf_existe"),
        F.col("arquivo_pdf_validacao").cast("string").alias("arquivo_pdf_validacao"),
        F.col("tp_clausula_exclusividade").cast("string").alias("tp_clausula_exclusividade"),
        F.col("fl_clausula_exclusividade").cast("int").alias("fl_clausula_exclusividade"),
        F.col("fl_clausula_nao_concorrencia").cast("int").alias("fl_clausula_nao_concorrencia"),
        F.col("trecho_clausula_exclusividade").cast("string").alias("trecho_clausula_exclusividade"),
        F.col("criterio_extracao_clausula").cast("string").alias("criterio_extracao_clausula"),
        F.col("score_extracao_clausula").cast("int").alias("score_extracao_clausula"),
        F.col("criterio_match_documento").cast("string").alias("criterio_match_documento"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_CONTRATOS, catalog_schema=f"{CATALOG}.{SILVER}")
