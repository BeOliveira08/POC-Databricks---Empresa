# Databricks notebook source
# silver: tb_empresa

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

BRONZE_RAW_EMPRESAS = f"{CATALOG}.{BRONZE}.raw_empresas"
BRONZE_RAW_CTPS = f"{CATALOG}.{BRONZE}.raw_ctps"
BRONZE_RAW_CONTRATOS = f"{CATALOG}.{BRONZE}.raw_contratos"
BRONZE_RAW_BENEFICIOS = f"{CATALOG}.{BRONZE}.raw_beneficios"
BRONZE_RAW_FGTS = f"{CATALOG}.{BRONZE}.raw_fgts"
BRONZE_RAW_INSS = f"{CATALOG}.{BRONZE}.raw_inss"
BRONZE_RAW_DAS = f"{CATALOG}.{BRONZE}.raw_das"
BRONZE_RAW_NF = f"{CATALOG}.{BRONZE}.raw_nf"
BRONZE_RAW_PROPOSTAS = f"{CATALOG}.{BRONZE}.raw_propostas"
BRONZE_RAW_DISTRATOS = f"{CATALOG}.{BRONZE}.raw_distratos"
BRONZE_RAW_RESCISOES = f"{CATALOG}.{BRONZE}.raw_rescisoes"
BRONZE_RAW_ENTREVISTAS = f"{CATALOG}.{BRONZE}.raw_entrevistas"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
ENTREVISTA_ONLY_EMPRESA_ID_OFFSET = 1000000000

date_udf = F.udf(parse_flexible_date_to_iso, "string")


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


def ascii_fold_expr(col_expr):
    return F.translate(
        F.coalesce(col_expr.cast("string"), F.lit("")),
        "ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ",
        "AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn",
    )


def nullable_text_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt == "") | txt.isin("NAO INFORMADO", "NÃO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def cleaned_empresa_expr(col_name: str):
    cleaned = ascii_fold_expr(normalized_text_expr(col_name))
    cleaned = F.regexp_replace(cleaned, r"\{[^}]*\}", " ")
    cleaned = F.regexp_replace(cleaned, r"\b[0-9A-F]{8,}\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCARTA OFERTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCARTA PROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA COMERCIAL\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA DE TRABALHO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCONTRATACAO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCARTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bOFERTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bTRABALHO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bTITULAR\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bJUNIOR\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bGENERICO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bALT\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPDF\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bTXT\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\b\d{4,}\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)
    return F.when(
        (cleaned == "")
        | cleaned.isin("CARTA", "PROPOSTA", "OFERTA", "TRABALHO", "NAO INFORMADO", "NAO MAPEADO")
        | cleaned.rlike(r"^[0-9 /._-]+$"),
        F.lit(None).cast("string"),
    ).otherwise(cleaned)


def name_key_expr(col_expr):
    txt = F.upper(ascii_fold_expr(F.coalesce(col_expr.cast("string"), F.lit(""))))
    txt = F.regexp_replace(txt, r"\b\d{6,}\b", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9]", " ")
    txt = F.regexp_replace(txt, r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU)\b", " ")
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def base_name_key_expr(col_expr):
    txt = F.upper(ascii_fold_expr(F.coalesce(col_expr.cast("string"), F.lit(""))))
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(
        txt,
        r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU|CONSULTORIA|CONSULTING|SERVICOS|SERVICOS DE|TECNOLOGIA|INFORMATICA|DESENVOLVIMENTO|SOLUCOES|ENGENHARIA|ASSESSORIA|EMPRESARIAL|DIGITAL|SOFTWARE|SISTEMAS|DO|DA|DE|E)\b",
        " ",
    )
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def parse_date_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")


def nullify_sentinel_date_expr(col_name: str):
    return F.when(F.col(col_name) <= F.to_date(F.lit("1900-01-01")), F.lit(None).cast("date")).otherwise(F.col(col_name))


def obs_base_df():
    return empty_df_for_schema(
        """
        nu_cnpj string,
        nm_empresa string,
        tp_empresa_observado string,
        fonte_origem string,
        arquivo_origem string,
        ingested_at_origem timestamp,
        dt_inicio_evidencia date,
        dt_fim_evidencia date,
        fl_referencia_empresa int,
        fl_entrevista int,
        fl_proposta int,
        fl_contrato int,
        fl_vinculo_clt int,
        fl_beneficio int,
        fl_fgts int,
        fl_inss int,
        fl_das int,
        fl_nf int,
        fl_rescisao int,
        fl_distrato int
        """
    )


def append_obs(df, nm_expr, cnpj_expr, papel, fonte, arquivo_expr, ingest_expr, dt_inicio_expr=None, dt_fim_expr=None, **flags):
    dt_inicio_expr = dt_inicio_expr if dt_inicio_expr is not None else F.lit(None).cast("date")
    dt_fim_expr = dt_fim_expr if dt_fim_expr is not None else F.lit(None).cast("date")

    def flag(name: str):
        return F.lit(int(flags.get(name, 0))).cast("int")

    return df.select(
        cnpj_expr.alias("nu_cnpj"),
        nm_expr.alias("nm_empresa"),
        F.lit(papel).cast("string").alias("tp_empresa_observado"),
        F.lit(fonte).cast("string").alias("fonte_origem"),
        arquivo_expr.cast("string").alias("arquivo_origem"),
        ingest_expr.cast("timestamp").alias("ingested_at_origem"),
        dt_inicio_expr.cast("date").alias("dt_inicio_evidencia"),
        dt_fim_expr.cast("date").alias("dt_fim_evidencia"),
        flag("fl_referencia_empresa").alias("fl_referencia_empresa"),
        flag("fl_entrevista").alias("fl_entrevista"),
        flag("fl_proposta").alias("fl_proposta"),
        flag("fl_contrato").alias("fl_contrato"),
        flag("fl_vinculo_clt").alias("fl_vinculo_clt"),
        flag("fl_beneficio").alias("fl_beneficio"),
        flag("fl_fgts").alias("fl_fgts"),
        flag("fl_inss").alias("fl_inss"),
        flag("fl_das").alias("fl_das"),
        flag("fl_nf").alias("fl_nf"),
        flag("fl_rescisao").alias("fl_rescisao"),
        flag("fl_distrato").alias("fl_distrato"),
    )


dfs = []

if table_exists(BRONZE_RAW_EMPRESAS):
    df_empresas = spark.table(BRONZE_RAW_EMPRESAS)
    dfs.append(
        append_obs(
            df_empresas,
            cleaned_empresa_expr("consultoria_raw"),
            F.lit(None).cast("string"),
            "CONSULTORIA_REFERENCIA",
            "RAW_EMPRESAS",
            F.col("arquivo_origem"),
            F.col("ingested_at"),
            fl_referencia_empresa=1,
        )
    )

if table_exists(BRONZE_RAW_CTPS):
    df_ctps = spark.table(BRONZE_RAW_CTPS)
    dfs.append(
        append_obs(
            df_ctps,
            cleaned_empresa_expr("nome_empresa"),
            normalize_cnpj_expr(only_digits_expr("nu_cnpj_empresa")),
            "EMPREGADORA_CLT",
            "RAW_CTPS",
            F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_origem_referencia"), F.col("arquivo_pdf_validacao")),
            F.col("ingested_at"),
            parse_date_expr("dt_inicio"),
            parse_date_expr("dt_fim"),
            fl_vinculo_clt=1,
        )
    )

if table_exists(BRONZE_RAW_CONTRATOS):
    df_contratos = spark.table(BRONZE_RAW_CONTRATOS)
    dt_inicio_contrato = parse_date_expr("dt_inicio")
    dt_fim_contrato = parse_date_expr("dt_fim")
    dfs.append(
        append_obs(
            df_contratos,
            cleaned_empresa_expr("consultoria"),
            normalize_cnpj_expr(only_digits_expr("cnpj_consultoria")),
            "CONSULTORIA_CONTRATO",
            "RAW_CONTRATOS",
            F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_origem_referencia"), F.col("arquivo_pdf_validacao")),
            F.col("ingested_at"),
            dt_inicio_contrato,
            dt_fim_contrato,
            fl_contrato=1,
        )
    )
    dfs.append(
        append_obs(
            df_contratos,
            cleaned_empresa_expr("empresa_prestador"),
            normalize_cnpj_expr(only_digits_expr("cnpj_prestador")),
            "PRESTADORA_CONTRATO",
            "RAW_CONTRATOS",
            F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_origem_referencia"), F.col("arquivo_pdf_validacao")),
            F.col("ingested_at"),
            dt_inicio_contrato,
            dt_fim_contrato,
            fl_contrato=1,
        )
    )

if table_exists(BRONZE_RAW_BENEFICIOS):
    df_beneficios = spark.table(BRONZE_RAW_BENEFICIOS)
    dfs.append(
        append_obs(
            df_beneficios,
            F.coalesce(
                cleaned_empresa_expr("empresa_corrigida_raw"),
                cleaned_empresa_expr("nome_operacional_raw"),
                cleaned_empresa_expr("empresa_raw"),
            ),
            normalize_cnpj_expr(only_digits_expr("cnpj_raw")),
            "EMPRESA_BENEFICIOS",
            "RAW_BENEFICIOS",
            F.col("arquivo_origem"),
            F.col("ingested_at"),
            fl_beneficio=1,
        )
    )

if table_exists(BRONZE_RAW_FGTS):
    df_fgts = spark.table(BRONZE_RAW_FGTS)
    dt_fgts = parse_date_expr("dt_lancamento")
    dfs.append(
        append_obs(
            df_fgts,
            cleaned_empresa_expr("empresa"),
            normalize_cnpj_expr(only_digits_expr("cnpj")),
            "EMPREGADORA_FGTS",
            "RAW_FGTS",
            F.col("arquivo_origem"),
            F.col("ingested_at"),
            dt_fgts,
            dt_fgts,
            fl_fgts=1,
        )
    )

if table_exists(BRONZE_RAW_INSS):
    df_inss = spark.table(BRONZE_RAW_INSS)
    dt_inicio_inss = parse_date_expr("dt_inicio")
    dt_fim_inss = parse_date_expr("dt_fim")
    dt_comp_inss = parse_date_expr("competencia")
    dfs.append(
        append_obs(
            df_inss,
            cleaned_empresa_expr("empresa"),
            normalize_cnpj_expr(only_digits_expr("cnpj")),
            "EMPREGADORA_INSS",
            "RAW_INSS",
            F.col("arquivo_origem"),
            F.col("ingested_at"),
            F.coalesce(dt_inicio_inss, dt_comp_inss),
            F.coalesce(dt_fim_inss, dt_comp_inss),
            fl_inss=1,
        )
    )

if table_exists(BRONZE_RAW_DAS):
    df_das = spark.table(BRONZE_RAW_DAS)
    dt_das = parse_date_expr("competencia")
    dfs.append(
        append_obs(
            df_das,
            cleaned_empresa_expr("razao_social"),
            normalize_cnpj_expr(only_digits_expr("cnpj_prestador")),
            "PRESTADORA_DAS",
            "RAW_DAS",
            F.col("arquivo_origem"),
            F.col("ingested_at"),
            dt_das,
            dt_das,
            fl_das=1,
        )
    )

if table_exists(BRONZE_RAW_NF):
    df_nf = spark.table(BRONZE_RAW_NF)
    dt_nf = parse_date_expr("data_emissao")
    dfs.append(
        append_obs(
            df_nf,
            F.lit(None).cast("string"),
            normalize_cnpj_expr(only_digits_expr("prestador_cnpj")),
            "PRESTADORA_NF",
            "RAW_NF",
            F.col("arquivo_origem"),
            F.col("ingested_at"),
            dt_nf,
            dt_nf,
            fl_nf=1,
        )
    )

if table_exists(BRONZE_RAW_PROPOSTAS):
    df_propostas = spark.table(BRONZE_RAW_PROPOSTAS)
    dt_proposta = parse_date_expr("dt_inicio_prevista")
    dfs.append(
        append_obs(
            df_propostas,
            cleaned_empresa_expr("empresa"),
            normalize_cnpj_expr(only_digits_expr("cnpj")),
            "EMPRESA_PROPOSTA",
            "RAW_PROPOSTAS",
            F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_pdf_validacao")),
            F.col("ingested_at"),
            dt_proposta,
            dt_proposta,
            fl_proposta=1,
        )
    )

if table_exists(BRONZE_RAW_DISTRATOS):
    df_distratos = spark.table(BRONZE_RAW_DISTRATOS)
    dfs.append(
        append_obs(
            df_distratos,
            cleaned_empresa_expr("empresa"),
            normalize_cnpj_expr(only_digits_expr("cnpj")),
            "EMPRESA_DISTRATO",
            "RAW_DISTRATOS",
            F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_pdf_validacao")),
            F.col("ingested_at"),
            parse_date_expr("dt_inicio"),
            parse_date_expr("dt_fim"),
            fl_distrato=1,
        )
    )

if table_exists(BRONZE_RAW_RESCISOES):
    df_rescisoes = spark.table(BRONZE_RAW_RESCISOES)
    dt_rescisao = parse_date_expr("dt_rescisao_raw")
    dfs.append(
        append_obs(
            df_rescisoes,
            cleaned_empresa_expr("empresa_raw"),
            normalize_cnpj_expr(only_digits_expr("cnpj_raw")),
            "EMPRESA_RESCISAO",
            "RAW_RESCISOES",
            F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_pdf_validacao")),
            F.col("ingested_at"),
            dt_rescisao,
            dt_rescisao,
            fl_rescisao=1,
        )
    )

if table_exists(BRONZE_RAW_ENTREVISTAS):
    df_entrevistas = spark.table(BRONZE_RAW_ENTREVISTAS)
    dt_entrevista = parse_date_expr("data_entrevista_raw")
    dfs.append(
        append_obs(
            df_entrevistas,
            F.coalesce(
                cleaned_empresa_expr("empresa_cliente_match_raw"),
                cleaned_empresa_expr("empresa_portfolio_mapeada_raw"),
                cleaned_empresa_expr("empresa_chave_raw"),
                cleaned_empresa_expr("empresa_relacionada_raw"),
                cleaned_empresa_expr("empresa_origem_raw"),
            ),
            F.lit(None).cast("string"),
            "EMPRESA_ENTREVISTA",
            "RAW_ENTREVISTAS",
            F.col("arquivo_origem"),
            F.col("ingested_at"),
            dt_entrevista,
            dt_entrevista,
            fl_entrevista=1,
        )
    )

if not dfs:
    df_union = obs_base_df()
else:
    df_union = dfs[0]
    for df_next in dfs[1:]:
        df_union = df_union.unionByName(df_next)

df_union = (
    df_union
    .withColumn("nm_empresa", F.when(F.trim(F.coalesce(F.col("nm_empresa"), F.lit(""))) != "", F.col("nm_empresa")))
    .withColumn("nm_empresa", cleaned_empresa_expr("nm_empresa"))
    .withColumn("nu_cnpj", F.when(F.length(F.coalesce(F.col("nu_cnpj"), F.lit(""))) == 14, F.col("nu_cnpj")))
    .filter(F.col("nu_cnpj").isNotNull() | F.col("nm_empresa").isNotNull())
    .withColumn("empresa_key", name_key_expr(F.col("nm_empresa")))
    .withColumn("empresa_base_key", base_name_key_expr(F.col("nm_empresa")))
)

# Regra de elegibilidade:
# - empresa so pode subir se existir em BASE_CLT/BASE_PJ
#   (aqui representadas por RAW_CTPS e RAW_CONTRATOS)
# - Base_Empresas/RAW_EMPRESAS entra apenas como redundancia nominal,
#   nunca como criterio suficiente para publicar a empresa
# - CNPJ verdadeiro so pode vir de RAW_CTPS/RAW_CONTRATOS
df_fontes_verdade = (
    df_union
    .filter(F.col("fonte_origem").isin("RAW_CTPS", "RAW_CONTRATOS"))
    .select("empresa_key", "empresa_base_key")
    .distinct()
)

df_cnpj_fontes_verdade = (
    df_union
    .filter(F.col("fonte_origem").isin("RAW_CTPS", "RAW_CONTRATOS"))
    .filter(F.col("nu_cnpj").isNotNull())
)

df_cnpj_ref_exact = (
    df_cnpj_fontes_verdade
    .filter(F.col("empresa_key").isNotNull())
    .groupBy(F.col("empresa_key").alias("empresa_key_ref"))
    .agg(
        F.first("nu_cnpj", ignorenulls=True).alias("nu_cnpj_ref"),
    )
)

df_cnpj_ref_base = (
    df_cnpj_fontes_verdade
    .filter(F.col("empresa_base_key").isNotNull())
    .groupBy(F.col("empresa_base_key").alias("empresa_base_key_ref"))
    .agg(
        F.first("nu_cnpj", ignorenulls=True).alias("nu_cnpj_ref_base"),
    )
)

source_priority = (
    F.when(F.col("fonte_origem") == "RAW_CTPS", F.lit(1))
     .when(F.col("fonte_origem") == "RAW_CONTRATOS", F.lit(2))
     .when(F.col("fonte_origem") == "RAW_RESCISOES", F.lit(3))
     .when(F.col("fonte_origem") == "RAW_DISTRATOS", F.lit(4))
     .when(F.col("fonte_origem") == "RAW_PROPOSTAS", F.lit(5))
     .when(F.col("fonte_origem") == "RAW_INSS", F.lit(6))
     .when(F.col("fonte_origem") == "RAW_FGTS", F.lit(7))
     .when(F.col("fonte_origem") == "RAW_NF", F.lit(8))
     .when(F.col("fonte_origem") == "RAW_DAS", F.lit(9))
     .when(F.col("fonte_origem") == "RAW_BENEFICIOS", F.lit(10))
     .when(F.col("fonte_origem") == "RAW_EMPRESAS", F.lit(11))
     .otherwise(F.lit(12))
)

df_enriched = (
    df_union.alias("u")
    .join(F.broadcast(df_cnpj_ref_exact).alias("e1"), F.col("u.empresa_key") == F.col("e1.empresa_key_ref"), "left")
    .join(F.broadcast(df_cnpj_ref_base).alias("e2"), F.col("u.empresa_base_key") == F.col("e2.empresa_base_key_ref"), "left")
    .join(
        F.broadcast(
            df_fontes_verdade
            .select(
                F.col("empresa_key").alias("empresa_key_true"),
                F.col("empresa_base_key").alias("empresa_base_key_true"),
            )
        ).alias("tv"),
        (
            (F.col("u.empresa_key").isNotNull() & (F.col("u.empresa_key") == F.col("tv.empresa_key_true")))
            | (F.col("u.empresa_base_key").isNotNull() & (F.col("u.empresa_base_key") == F.col("tv.empresa_base_key_true")))
        ),
        "left",
    )
    .withColumn(
        "nu_cnpj_resolvido",
        F.coalesce(F.col("u.nu_cnpj"), F.col("e1.nu_cnpj_ref"), F.col("e2.nu_cnpj_ref_base"))
    )
    .withColumn(
        "fl_match_fonte_verdade",
        F.when(F.col("tv.empresa_key_true").isNotNull() | F.col("tv.empresa_base_key_true").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )
    .withColumn("fl_tem_evidencia_documental_row", F.lit(1))
    .withColumn("source_priority", source_priority)
    .withColumn(
        "empresa_dedup_key",
        F.when(F.col("nu_cnpj_resolvido").isNotNull(), F.concat(F.lit("CNPJ|"), F.col("nu_cnpj_resolvido")))
         .otherwise(F.concat(F.lit("BASE|"), F.coalesce(F.col("empresa_base_key"), F.col("empresa_key"), F.lit("SEM_CHAVE"))))
    )
)

w_best = Window.partitionBy("empresa_dedup_key").orderBy(
    F.col("fl_tem_evidencia_documental_row").desc(),
    F.when(F.col("nu_cnpj_resolvido").isNotNull(), F.lit(1)).otherwise(F.lit(0)).desc(),
    F.col("source_priority").asc(),
    F.length(F.coalesce(F.col("nm_empresa"), F.lit(""))).desc(),
    F.col("ingested_at_origem").desc_nulls_last(),
)

df_best = (
    df_enriched
    .withColumn("rn_best", F.row_number().over(w_best))
    .filter(F.col("rn_best") == 1)
    .select(
        "empresa_dedup_key",
        F.col("nu_cnpj_resolvido").alias("nu_cnpj_best"),
        F.col("nm_empresa").alias("nm_empresa_best"),
        F.col("tp_empresa_observado").alias("tp_empresa_best"),
        F.col("fonte_origem").alias("fonte_origem_best"),
        F.col("arquivo_origem").alias("arquivo_origem_best"),
        F.col("ingested_at_origem").alias("ingested_at_origem_best"),
    )
)

df_agg = (
    df_enriched
    .groupBy("empresa_dedup_key")
    .agg(
        F.first("nu_cnpj_resolvido", ignorenulls=True).alias("nu_cnpj"),
        F.concat_ws(" | ", F.sort_array(F.collect_set(F.col("tp_empresa_observado")))).alias("tp_empresa_papeis"),
        F.concat_ws(" | ", F.sort_array(F.collect_set(F.col("fonte_origem")))).alias("fontes_origem"),
        F.max("fl_entrevista").cast("int").alias("fl_tem_entrevista"),
        F.min("dt_inicio_evidencia").alias("dt_primeira_evidencia"),
        F.max("dt_fim_evidencia").alias("dt_ultima_evidencia"),
        F.max("fl_referencia_empresa").cast("int").alias("fl_tem_referencia_empresa"),
        F.max("fl_proposta").cast("int").alias("fl_tem_proposta"),
        F.max("fl_contrato").cast("int").alias("fl_tem_contrato"),
        F.max("fl_vinculo_clt").cast("int").alias("fl_tem_vinculo_clt"),
        F.max("fl_beneficio").cast("int").alias("fl_tem_beneficio"),
        F.max("fl_fgts").cast("int").alias("fl_tem_fgts"),
        F.max("fl_inss").cast("int").alias("fl_tem_inss"),
        F.max("fl_das").cast("int").alias("fl_tem_das"),
        F.max("fl_nf").cast("int").alias("fl_tem_nf"),
        F.max("fl_rescisao").cast("int").alias("fl_tem_rescisao"),
        F.max("fl_distrato").cast("int").alias("fl_tem_distrato"),
        F.max("fl_tem_evidencia_documental_row").cast("int").alias("fl_tem_evidencia_documental"),
        F.max("fl_match_fonte_verdade").cast("int").alias("fl_match_fonte_verdade"),
    )
)

w_empresa_id = Window.orderBy(
    F.coalesce(F.col("nu_cnpj"), F.lit("")),
    F.coalesce(F.col("nm_empresa"), F.lit("")),
)
w_empresa_id_entrevista = Window.orderBy(
    F.coalesce(F.col("nm_empresa"), F.lit("")),
    F.coalesce(F.col("empresa_dedup_key"), F.lit("")),
)

periodo_inicio = F.to_date(F.lit("2021-01-01"))
periodo_fim = F.to_date(F.lit("2026-05-31"))
trilha_forte_expr = (
    (F.col("fl_tem_vinculo_clt") == 1)
    | (F.col("fl_tem_contrato") == 1)
    | (F.col("fl_tem_proposta") == 1)
    | (F.col("fl_tem_beneficio") == 1)
    | (F.col("fl_tem_fgts") == 1)
    | (F.col("fl_tem_inss") == 1)
    | (F.col("fl_tem_das") == 1)
    | (F.col("fl_tem_nf") == 1)
    | (F.col("fl_tem_rescisao") == 1)
    | (F.col("fl_tem_distrato") == 1)
)
trilha_mediana_expr = (~trilha_forte_expr) & (F.col("fl_tem_referencia_empresa") == 1)
# Sinal diagnostico: prioriza enriquecimento de CNPJ para trilhas
# fiscais/previdenciarias, sem impedir a publicacao da dimensao.
cnpj_enriquecimento_prioritario_expr = (
    (F.col("fl_tem_fgts") == 1)
    | (F.col("fl_tem_inss") == 1)
    | (F.col("fl_tem_das") == 1)
    | (F.col("fl_tem_nf") == 1)
)

somente_entrevista_expr = (
    (F.col("a.fl_tem_entrevista") == 1)
    & (F.col("a.fl_tem_referencia_empresa") == 0)
    & (F.col("a.fl_tem_proposta") == 0)
    & (F.col("a.fl_tem_contrato") == 0)
    & (F.col("a.fl_tem_vinculo_clt") == 0)
    & (F.col("a.fl_tem_beneficio") == 0)
    & (F.col("a.fl_tem_fgts") == 0)
    & (F.col("a.fl_tem_inss") == 0)
    & (F.col("a.fl_tem_das") == 0)
    & (F.col("a.fl_tem_nf") == 0)
    & (F.col("a.fl_tem_rescisao") == 0)
    & (F.col("a.fl_tem_distrato") == 0)
)

df_final_base = (
    df_agg.alias("a")
    .join(df_best.alias("b"), "empresa_dedup_key", "left")
    .withColumn("nm_empresa", F.coalesce(F.col("b.nm_empresa_best"), F.lit("NAO_INFORMADO")))
    .withColumn(
        "tp_empresa",
        F.when(F.col("a.fl_tem_vinculo_clt") == 1, F.lit("EMPREGADORA"))
         .when(F.col("a.fl_tem_contrato") == 1, F.lit("EMPRESA_CONTRATO"))
         .when(F.col("a.fl_tem_proposta") == 1, F.lit("EMPRESA_PROPOSTA"))
         .when(somente_entrevista_expr, F.lit("EMPRESA_ENTREVISTA"))
         .when(F.col("a.fl_tem_referencia_empresa") == 1, F.lit("EMPRESA_REFERENCIA"))
         .otherwise(F.coalesce(F.col("b.tp_empresa_best"), F.lit("NAO_INFORMADO")))
    )
    .withColumn("fonte_origem", F.coalesce(F.col("b.fonte_origem_best"), F.lit("NAO_INFORMADO")))
    .withColumn("arquivo_origem", F.coalesce(F.col("b.arquivo_origem_best"), F.lit("NAO_INFORMADO")))
    .withColumn("ingested_at_origem", F.col("b.ingested_at_origem_best"))
    .withColumn("data_inicio", F.col("dt_primeira_evidencia"))
    .withColumn("data_fim", F.col("dt_ultima_evidencia"))
    .withColumn(
        "fl_ativa_periodo_2021_2026",
        F.when(
            F.col("dt_primeira_evidencia").isNull() & F.col("dt_ultima_evidencia").isNull(),
            F.lit(0),
        ).when(
            F.coalesce(F.col("dt_ultima_evidencia"), F.col("dt_primeira_evidencia")) >= periodo_inicio,
            F.when(F.coalesce(F.col("dt_primeira_evidencia"), F.col("dt_ultima_evidencia")) <= periodo_fim, F.lit(1)).otherwise(F.lit(0)),
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "tp_trilha_cnpj",
        F.when(trilha_forte_expr, F.lit("FORTE"))
         .when(trilha_mediana_expr, F.lit("MEDIANA"))
         .otherwise(F.lit("FRACA"))
    )
    .withColumn("fl_exige_cnpj", F.when(cnpj_enriquecimento_prioritario_expr, F.lit(1)).otherwise(F.lit(0)))
    .withColumn("fl_empresa_apenas_entrevista", F.when(somente_entrevista_expr, F.lit(1)).otherwise(F.lit(0)))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    # Publica empresas validadas nas fontes-mestre e tambem
    # empresas vistas somente em entrevistas, com faixa propria de ID.
    .filter(
        (F.coalesce(F.col("fl_match_fonte_verdade"), F.lit(0)) == 1)
        | (F.col("fl_empresa_apenas_entrevista") == 1)
    )
)

df_final_regular = (
    df_final_base
    .filter(F.col("fl_empresa_apenas_entrevista") == 0)
    .withColumn("empresa_id", F.row_number().over(w_empresa_id))
)

df_final_entrevista = (
    df_final_base
    .filter(F.col("fl_empresa_apenas_entrevista") == 1)
    .withColumn(
        "empresa_id",
        (F.row_number().over(w_empresa_id_entrevista) + F.lit(ENTREVISTA_ONLY_EMPRESA_ID_OFFSET)).cast("int"),
    )
)

df_final = (
    df_final_regular
    .unionByName(df_final_entrevista)
    .select(
        F.col("empresa_id").cast("int").alias("empresa_id"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj"),
        F.col("nm_empresa").cast("string").alias("nm_empresa"),
        F.col("tp_empresa").cast("string").alias("tp_empresa"),
        F.col("tp_empresa_papeis").cast("string").alias("tp_empresa_papeis"),
        F.col("fonte_origem").cast("string").alias("fonte_origem"),
        F.col("fontes_origem").cast("string").alias("fontes_origem"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("dt_primeira_evidencia").cast("date").alias("dt_primeira_evidencia"),
        F.col("dt_ultima_evidencia").cast("date").alias("dt_ultima_evidencia"),
        F.col("data_inicio").cast("date").alias("data_inicio"),
        F.col("data_fim").cast("date").alias("data_fim"),
        F.col("fl_ativa_periodo_2021_2026").cast("int").alias("fl_ativa_periodo_2021_2026"),
        F.col("fl_tem_referencia_empresa").cast("int").alias("fl_tem_referencia_empresa"),
        F.col("fl_tem_entrevista").cast("int").alias("fl_tem_entrevista"),
        F.col("fl_empresa_apenas_entrevista").cast("int").alias("fl_empresa_apenas_entrevista"),
        F.col("fl_tem_proposta").cast("int").alias("fl_tem_proposta"),
        F.col("fl_tem_contrato").cast("int").alias("fl_tem_contrato"),
        F.col("fl_tem_vinculo_clt").cast("int").alias("fl_tem_vinculo_clt"),
        F.col("fl_tem_beneficio").cast("int").alias("fl_tem_beneficio"),
        F.col("fl_tem_fgts").cast("int").alias("fl_tem_fgts"),
        F.col("fl_tem_inss").cast("int").alias("fl_tem_inss"),
        F.col("fl_tem_das").cast("int").alias("fl_tem_das"),
        F.col("fl_tem_nf").cast("int").alias("fl_tem_nf"),
        F.col("fl_tem_rescisao").cast("int").alias("fl_tem_rescisao"),
        F.col("fl_tem_distrato").cast("int").alias("fl_tem_distrato"),
        F.col("fl_tem_evidencia_documental").cast("int").alias("fl_tem_evidencia_documental"),
        F.col("tp_trilha_cnpj").cast("string").alias("tp_trilha_cnpj"),
        F.col("fl_exige_cnpj").cast("int").alias("fl_exige_cnpj"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = df_final.withColumn("nu_cnpj", normalize_cnpj_expr(only_digits_expr("nu_cnpj")))

df_final = fill_silver_nulls(df_final)
df_final = df_final.withColumn("nu_cnpj", normalize_cnpj_expr(only_digits_expr("nu_cnpj")))

for date_col in ["dt_primeira_evidencia", "dt_ultima_evidencia", "data_inicio", "data_fim"]:
    df_final = df_final.withColumn(date_col, nullify_sentinel_date_expr(date_col))

write_overwrite(df_final, SILVER_TB_EMPRESA, catalog_schema=f"{CATALOG}.{SILVER}")
